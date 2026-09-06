//go:build linux || darwin

// Package processkill is the process-kill chaos harness (CHAOS-02/03/06 -> SC2/SC4).
//
// It boots N real toyraftd subprocesses, each in its OWN process group
// (SysProcAttr{Setpgid:true}, so the child's pgid == its pid), detects the leader
// over the CLIENT api's GET /status (the LOCKED lowercase `role` string, mirroring
// scripts/smoke.sh find_leader), kills the leader's WHOLE group via the NEGATIVE
// pgid (syscall.Kill(-pgid, SIGKILL) — NOT the child-only cmd.Process kill, which
// signals only the immediate child), asserts a new leader within a deadline plus write survival,
// and tears everything down in t.Cleanup with a portable ps/lsof leak gate.
//
// POSIX-only: Setpgid and negative-pgid signalling are POSIX concepts. The whole
// package is gated `//go:build linux || darwin`; the CI matrix is ubuntu+macos so
// both run it (RESEARCH Pitfall 2).
//
// Port scheme (RESEARCH Pitfall 5): a per-run BASEP well clear of the demo's 7001 —
// peer port BASEP+i, client port BASEP+i+clientPortOffset (the daemon's fixed +2000
// convention). Do NOT run two process-kill cases against the same BASEP in parallel.
package processkill

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
	"github.com/prajwalmahajan101/toyraft/internal/raftest"
	"github.com/prajwalmahajan101/toyraft/pkg/kvsm"
)

// clientPortOffset mirrors cmd/toyraftd's fixed peer->client port delta: a node's
// client port is its peer port + this offset (peer 17001 -> client 19001).
const clientPortOffset = 2000

// pollInterval is the sleep between /status polls while waiting for a leader.
const pollInterval = 25 * time.Millisecond

// node is one spawned toyraftd subprocess and the addresses it was launched with.
type node struct {
	id         string
	peerAddr   string // host:port on the consensus plane (-peers entry, -listen is derived)
	clientAddr string // host:port on the CLIENT api (peer port + clientPortOffset)
	dataDir    string
	cmd        *exec.Cmd
	pgid       int  // == cmd.Process.Pid because Setpgid with Pgid==0 makes the child its own group leader
	killed     bool // set once the group has been SIGKILL'd + reaped, so teardown/leak-gate skips it
}

// harness owns the spawned cluster, a shared 307-following HTTP client, and the
// operation recorder whose Snapshot is dumped as the Phase-12-consumable history.
type harness struct {
	t        *testing.T
	bin      string // path to the freshly built toyraftd binary
	nodes    []*node
	client   *http.Client
	recorder *raftest.Recorder
	seed     int64
	baseP    int
}

// newHarness builds the daemon binary, spawns n toyraftd subprocesses each in its
// own process group, and registers the single LIFO teardown. n MUST be odd and >=3
// (the daemon rejects even N; we assert here for a clear message).
func newHarness(t *testing.T, n int, seed int64) *harness {
	t.Helper()
	if n < 3 || n%2 == 0 {
		t.Fatalf("newHarness: n must be odd and >=3 for a clean majority, got %d", n)
	}

	tmp := t.TempDir()

	// Build the daemon once into the temp dir (self-contained; no reliance on
	// `make build`). Record the path for spawnNode.
	bin := filepath.Join(tmp, "toyraftd")
	build := exec.Command("go", "build", "-o", bin, "./cmd/toyraftd")
	build.Dir = repoRoot(t)
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		t.Fatalf("build toyraftd: %v", err)
	}

	// A per-run BASEP well clear of the demo/smoke.sh 7001 (RESEARCH Pitfall 5).
	baseP := 17001

	h := &harness{
		t:        t,
		bin:      bin,
		seed:     seed,
		baseP:    baseP,
		recorder: raftest.NewRecorder(clock.NewReal()),
		// ONE shared 307-following client with SHORT timeouts so a poll against a
		// dead/frozen node returns fast (smoke.sh uses connect=1s/total=2s,
		// RESEARCH Pitfall 3). DEFAULT redirect policy follows 307 like toyraftctl.
		client: &http.Client{
			Timeout: 2 * time.Second,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{Timeout: 1 * time.Second}).DialContext,
			},
		},
	}

	// Derive the shared -peers spec from all peer addrs (INCLUDES every node).
	peerAddrs := make([]string, n)
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("n%02d", i)
		peerAddrs[i] = fmt.Sprintf("127.0.0.1:%d", baseP+i)
	}
	specParts := make([]string, n)
	for i := 0; i < n; i++ {
		specParts[i] = ids[i] + "=" + peerAddrs[i]
	}
	peersSpec := strings.Join(specParts, ",")

	// Register teardown ONCE, BEFORE spawning, so a mid-spawn t.Fatalf still reaps
	// whatever already started (RESEARCH Pitfall 4).
	t.Cleanup(h.teardown)

	for i := 0; i < n; i++ {
		clientAddr := fmt.Sprintf("127.0.0.1:%d", baseP+i+clientPortOffset)
		dataDir := filepath.Join(tmp, "data-"+ids[i])
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			t.Fatalf("mkdir data dir for %s: %v", ids[i], err)
		}
		h.nodes = append(h.nodes, h.spawnNode(ids[i], peerAddrs[i], clientAddr, dataDir, peersSpec))
	}

	return h
}

// spawnNode launches one toyraftd in its OWN process group and returns the node
// handle. pgid == pid because Setpgid with Pgid==0 makes the child its own group
// leader.
func (h *harness) spawnNode(id, peerAddr, clientAddr, dataDir, peersSpec string) *node {
	h.t.Helper()
	cmd := exec.Command(h.bin,
		"-id", id,
		"-peers", peersSpec,
		"-listen", clientAddr,
		"-data-dir", dataDir,
		"-seed", strconv.FormatInt(h.seed, 10),
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // own group; pgid == pid
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr         // surface daemon logs on failure
	if err := cmd.Start(); err != nil {
		h.t.Fatalf("spawn %s: %v", id, err)
	}
	return &node{
		id:         id,
		peerAddr:   peerAddr,
		clientAddr: clientAddr,
		dataDir:    dataDir,
		cmd:        cmd,
		pgid:       cmd.Process.Pid, // pgid == pid because Pgid==0 with Setpgid
	}
}

// statusWire is the small slice of GET /status this harness reads: the LOCKED
// lowercase role string and the durable commit_index (cmd/toyraftd/kvapi.go).
type statusWire struct {
	Role        string `json:"role"`
	CommitIndex uint64 `json:"commit_index"`
}

// getStatus issues GET http://<clientAddr>/status and decodes the small wire slice.
// On any error (dead/frozen node, non-200) it returns ok=false so callers treat the
// node as "not leader / unknown commit".
func (h *harness) getStatus(clientAddr string) (statusWire, bool) {
	var s statusWire
	resp, err := h.client.Get("http://" + clientAddr + "/status")
	if err != nil {
		return s, false // dead/frozen node => not leader
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return s, false
	}
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return s, false
	}
	return s, true
}

// roleOf returns the LOCKED lowercase role string for the node at clientAddr, or ""
// on any error (mirrors cmd/toyraftctl statusResp + smoke.sh role_of).
func (h *harness) roleOf(clientAddr string) string {
	s, ok := h.getStatus(clientAddr)
	if !ok {
		return ""
	}
	return s.Role
}

// commitIndexOf returns the node's durable commit_index and ok=true, or (0,false)
// on any error. Used to poll a majority's commit advance for the >=M oracle — this
// reads the COMMIT SEAM, not a read-back of an applied key.
func (h *harness) commitIndexOf(clientAddr string) (uint64, bool) {
	s, ok := h.getStatus(clientAddr)
	if !ok {
		return 0, false
	}
	return s.CommitIndex, true
}

// findLeader polls the live (un-killed) survivors for role=="leader" until deadline,
// returning the leader node and true, or (nil,false) if none appears in time.
func (h *harness) findLeader(deadline time.Time) (*node, bool) {
	for time.Now().Before(deadline) {
		for _, n := range h.nodes {
			if n.killed {
				continue
			}
			if h.roleOf(n.clientAddr) == "leader" {
				return n, true
			}
		}
		time.Sleep(pollInterval)
	}
	return nil, false
}

// withLeaderRetry re-resolves the leader and re-issues fn until it succeeds or the
// deadline passes. It absorbs the leader-churn window a bare 307-follow cannot:
// a short-lived leader can accept a write and then step down before the entry
// commits, so the request hangs until the client timeout (v1 Propose does not
// actively cancel in-flight proposals on step-down — pkg/raft/node_public.go), or a
// follower 503s no_leader_known before it learns the new leader. Either surfaces as
// an error from fn; we re-resolve the (now stale) leader addr and retry. The old
// leader addr is never reused — findLeader skips killed nodes and returns whoever
// currently reports role=="leader".
func (h *harness) withLeaderRetry(deadline time.Time, fn func(clientAddr string) error) error {
	var last error
	for time.Now().Before(deadline) {
		lead, ok := h.findLeader(deadline)
		if !ok {
			last = fmt.Errorf("no leader before deadline")
			break
		}
		if err := fn(lead.clientAddr); err == nil {
			return nil
		} else {
			last = err
		}
		time.Sleep(pollInterval)
	}
	return fmt.Errorf("withLeaderRetry: %w", last)
}

// killLeaderGroup SIGKILLs the leader's WHOLE process group via the NEGATIVE pgid
// (never the child-only cmd.Process kill, which signals only the immediate child —
// RESEARCH Anti-pattern), then reaps the zombie and marks the node killed so teardown and
// the leak gate skip it.
func (h *harness) killLeaderGroup(n *node) {
	h.t.Helper()
	if err := syscall.Kill(-n.pgid, syscall.SIGKILL); err != nil {
		h.t.Fatalf("kill -%d (%s): %v", n.pgid, n.id, err)
	}
	_ = n.cmd.Wait() // reap the zombie
	n.killed = true
}

// setKV issues PUT /kv/<k> with v as the body through the 307-following client,
// recording the call via the harness recorder. The body is a bytes.Reader so
// http.NewRequest sets Request.GetBody, letting the default client REPLAY the body
// across a follower's 307 to the leader (the 10-03 correctness point).
func (h *harness) setKV(leaderClientAddr, k, v string) error {
	op := kvsm.Op{Kind: "set", Key: k, Value: []byte(v)}
	callID := h.recorder.BeginCall(0, op)
	req, err := http.NewRequest(http.MethodPut, "http://"+leaderClientAddr+"/kv/"+k, bytes.NewReader([]byte(v)))
	if err != nil {
		h.recorder.EndCall(0, callID, map[string]any{"error": err.Error()})
		return fmt.Errorf("build PUT %s: %w", k, err)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		h.recorder.EndCall(0, callID, map[string]any{"error": err.Error()})
		return fmt.Errorf("PUT %s: %w", k, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		h.recorder.EndCall(0, callID, map[string]any{"status": resp.StatusCode})
		return fmt.Errorf("PUT %s: server returned %s: %s", k, resp.Status, strings.TrimSpace(string(body)))
	}
	h.recorder.EndCall(0, callID, map[string]any{"ok": true})
	return nil
}

// getKV issues GET /kv/<k> through the 307-following client, recording the call.
// A 200 returns the value; a 404 returns an empty string + nil error (absent key);
// any other status is an error.
func (h *harness) getKV(leaderClientAddr, k string) (string, error) {
	op := kvsm.Op{Kind: "get", Key: k}
	callID := h.recorder.BeginCall(0, op)
	resp, err := h.client.Get("http://" + leaderClientAddr + "/kv/" + k)
	if err != nil {
		h.recorder.EndCall(0, callID, map[string]any{"error": err.Error()})
		return "", fmt.Errorf("GET %s: %w", k, err)
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			h.recorder.EndCall(0, callID, map[string]any{"error": err.Error()})
			return "", fmt.Errorf("GET %s read body: %w", k, err)
		}
		h.recorder.EndCall(0, callID, map[string]any{"value": string(body)})
		return string(body), nil
	case http.StatusNotFound:
		h.recorder.EndCall(0, callID, map[string]any{"not_found": true})
		return "", nil
	default:
		body, _ := io.ReadAll(resp.Body)
		h.recorder.EndCall(0, callID, map[string]any{"status": resp.StatusCode})
		return "", fmt.Errorf("GET %s: server returned %s: %s", k, resp.Status, strings.TrimSpace(string(body)))
	}
}

// teardown is the SINGLE LIFO cleanup (registered ONCE via t.Cleanup). Order: kill
// every still-live child PGID FIRST, cmd.Wait to reap zombies, THEN the leak
// assertion. Data dirs are t.TempDir() (auto-removed) so no manual rm is needed.
// Every failure is reported via t.Errorf — NEVER t.Fatalf, which is disallowed in
// a cleanup func.
func (h *harness) teardown() {
	for _, n := range h.nodes {
		if n.killed {
			continue
		}
		if err := syscall.Kill(-n.pgid, syscall.SIGKILL); err != nil {
			// ESRCH (group already gone) is benign; anything else is a leak signal.
			if err != syscall.ESRCH {
				h.t.Errorf("teardown: kill -%d (%s): %v", n.pgid, n.id, err)
			}
		}
		_ = n.cmd.Wait() // reap
		n.killed = true
	}
	// Data dirs live under t.TempDir(), auto-removed by the testing framework —
	// no manual os.RemoveAll needed here.
	h.assertNoLeak()
}

// assertNoLeak is the post-kill leak gate. PRIMARY, portable (Linux+macOS) check:
// `ps -o pgid= -p <pid>` — process-absence means the group is gone (RESEARCH Open-Q
// 4). SECONDARY check: `lsof -i :<clientPort>` — the port must not still be bound by
// a toyraftd (lsof-missing is a skip, not a failure). Leaks are reported via
// t.Errorf naming the pid/port.
func (h *harness) assertNoLeak() {
	for _, n := range h.nodes {
		// PRIMARY: process must be gone. `ps -o pgid= -p <pid>` exits non-zero (or
		// prints nothing) when the pid no longer exists.
		out, _ := exec.Command("ps", "-o", "pgid=", "-p", strconv.Itoa(n.pgid)).Output()
		if strings.TrimSpace(string(out)) != "" {
			h.t.Errorf("leak: process pid=%d (%s, pgid=%d) still alive after teardown", n.pgid, n.id, n.pgid)
		}

		// SECONDARY: the client port must not still be bound. lsof may be absent;
		// treat that as a skip, not a failure.
		_, portStr, err := net.SplitHostPort(n.clientAddr)
		if err != nil {
			continue
		}
		lsof, lsofErr := exec.LookPath("lsof")
		if lsofErr != nil {
			continue // lsof not installed — skip the secondary check
		}
		out, _ = exec.Command(lsof, "-i", ":"+portStr).Output()
		if strings.Contains(string(out), "toyraftd") {
			h.t.Errorf("leak: client port %s still bound by a toyraftd after teardown (%s)", portStr, n.id)
		}
	}
}

// repoRoot walks up from the test's working directory to the module root (the dir
// holding go.mod), so `go build ./cmd/toyraftd` resolves regardless of where the
// test binary runs from.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("repoRoot: no go.mod found walking up from working dir")
		}
		dir = parent
	}
}
