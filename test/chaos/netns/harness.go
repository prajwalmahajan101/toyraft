//go:build linux && netns

// Package netns is the network-namespace chaos harness (CHAOS-04 -> SC1/SC2).
//
// It retires risk D-1 (loopback masking): the inproc + process-kill harnesses run
// over 127.0.0.1, where the kernel loopback path cannot exhibit a REAL network
// partition. This harness gives each toyraftd its OWN network namespace + veth pair +
// non-loopback IP on a shared Linux bridge (a genuine L3 hop between nodes), then
// severs exactly one link with per-namespace `iptables -j DROP` so the transport's
// real dial/timeout/reconnect behaviour is exercised across distinct L3 addresses.
//
// LOAD-BEARING SAFETY DECISION: this harness drives netns from `ip`/`iptables`
// SUBPROCESSES, NEVER via the in-process namespace-join syscall. In Go a network
// namespace is a per-OS-thread property, but goroutines migrate across threads freely,
// so joining a namespace in-process is the classic Go-scheduler<->thread<->namespace
// footgun (RESEARCH REJECTED table). Every topology mutation is an `ip`/`iptables` argv
// run via run().
//
// Daemon needs NO code change: cmd/toyraftd binds arbitrary -peers/-listen host:port
// (main.go derives every url from -peers), so passing namespace IPs like
// 10.x.0.10:7001 binds and dials them unchanged (verified in cmd/toyraftd/main.go —
// do not re-litigate this, RESEARCH Pitfall 6).
//
// Per-pid name randomization (bridge/ns/subnet-octet derived from os.Getpid) gives
// idempotency against a crashed prior run (RESEARCH Pitfall 4). This assumes ONE netns
// test per `go test` process; a second test in this package would collide on the
// `tr-<pid>-nX` names and MUST add a per-newHarness suffix.
//
// Build-tag gate (SC2 mechanism): the whole package is gated `//go:build linux && netns`,
// so `go test ./...` (no tag) never compiles it, and macOS never compiles it even with
// `-tags=netns`. Running it needs root / passwordless sudo (ip/iptables); the compile +
// vet gates are what plan 13-01 verifies.
package netns

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
)

// clientPortOffset mirrors cmd/toyraftd's fixed peer->client port delta: a node's
// client port is its peer port + this offset (peer 7001 -> client 9001). Each node
// lives in its own netns so every node can share the SAME peer/client ports.
const clientPortOffset = 2000

// pollInterval is the sleep between /status polls while waiting for a leader.
const pollInterval = 25 * time.Millisecond

// node is one spawned toyraftd subprocess and the addresses it was launched with.
type node struct {
	id         string
	peerAddr   string // host:port on the consensus plane (namespace IP, -peers entry)
	clientAddr string // host:port on the CLIENT api (peer port + clientPortOffset)
	dataDir    string
	cmd        *exec.Cmd
	pgid       int  // == cmd.Process.Pid because Setpgid with Pgid==0 makes the child its own group leader
	killed     bool // set once the group has been SIGKILL'd + reaped, so teardown/leak-gate skips it
}

// harness owns the spawned cluster, its netns/veth/bridge topology, and a shared
// 307-following HTTP client. The Phase-12 raftest history-capture seam is DELIBERATELY
// dropped (SC1 needs no history artifact), so setKV/getKV are plain PUT/GET with no
// raftest/clock coupling and no such field on the harness.
type harness struct {
	t      *testing.T
	bin    string // path to the freshly built toyraftd binary
	nodes  []*node
	client *http.Client
	seed   int64

	// Per-run topology names, randomized from the pid for idempotency (Pitfall 4).
	bridge string   // root-ns bridge, e.g. "br-tr-12345"
	gwIP   string   // the bridge's host IP in the node subnet, "10.<octet>.0.254" (root-ns route)
	nss    []string // the 3 namespace names, "tr-<pid>-n{0,1,2}"
	ips    []string // "10.<octet>.0.1{0,1,2}", octet = pid%250 + 1
}

// run executes an ip/iptables/sudo argv and t.Fatalf's with the argv + combined
// output on non-zero exit. ALL topology mutations go through this checked helper.
func (h *harness) run(t *testing.T, argv ...string) {
	t.Helper()
	cmd := exec.Command(argv[0], argv[1:]...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("cmd %v failed: %v\n%s", argv, err, out)
	}
}

// runQuiet is the non-fatal variant for best-effort pre-clean and teardown: it never
// t.Fatalf's (both are forbidden in a cleanup func), returning the error instead.
func (h *harness) runQuiet(argv ...string) error {
	cmd := exec.Command(argv[0], argv[1:]...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("cmd %v: %w\n%s", argv, err, out)
	}
	return nil
}

// newHarness builds the daemon once, stands up a randomized-per-run bridge + N=3
// namespaces + N veth pairs with real non-loopback IPs, and spawns each toyraftd INTO
// its namespace via `sudo ip netns exec`. N is FIXED at 3 — the minimum clean odd
// majority for "partition two, third holds quorum". Registers the single LIFO teardown
// BEFORE creating any kernel resource so a mid-setup t.Fatalf still reaps partial state.
func newHarness(t *testing.T, seed int64) *harness {
	t.Helper()

	const n = 3 // FIXED: minimum clean odd majority (partition 2, third keeps quorum)

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

	// Per-run topology names, randomized from the pid (Pitfall 4 idempotency).
	pid := os.Getpid()
	octet := pid%250 + 1 // per-run /24 subnet uniqueness: 10.<octet>.0.0/24
	h := &harness{
		t:      t,
		bin:    bin,
		seed:   seed,
		bridge: fmt.Sprintf("br-tr-%d", pid),
		// The bridge's OWN host IP in the node subnet. REQUIRED (not optional): the
		// test process polls each node's /status from the ROOT namespace, so the root
		// ns needs a connected route to 10.<octet>.0.0/24 — which only exists once the
		// bridge carries an address in that subnet. Without it the cluster elects a
		// leader fine internally (ns<->ns over the bridge) but the test can never
		// OBSERVE it (every root-ns poll to 10.x.0.1i:9001 fails) → "no initial
		// leader" (DEBUG.md, 2026-07-04). .254 is the conventional gateway host.
		gwIP: fmt.Sprintf("10.%d.0.254", octet),
		// ONE shared 307-following client with SHORT timeouts so a poll against a
		// dead/frozen/partitioned node returns fast. DEFAULT redirect policy follows
		// 307 like toyraftctl.
		client: &http.Client{
			Timeout: 2 * time.Second,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{Timeout: 1 * time.Second}).DialContext,
			},
		},
	}
	for i := 0; i < n; i++ {
		h.nss = append(h.nss, fmt.Sprintf("tr-%d-n%d", pid, i))
		h.ips = append(h.ips, fmt.Sprintf("10.%d.0.1%d", octet, i))
	}

	// Register teardown ONCE, BEFORE creating any kernel resource, so a mid-setup
	// t.Fatalf still cleans partial state (Pitfall 4).
	t.Cleanup(h.teardown)

	// Best-effort pre-clean against a crashed prior run (idempotency): ignore errors.
	for _, ns := range h.nss {
		_ = h.runQuiet("sudo", "ip", "netns", "del", ns)
	}
	_ = h.runQuiet("sudo", "ip", "link", "del", h.bridge)

	// Create the root-ns bridge and give it a host IP in the node subnet. The IP is
	// what lets the ROOT-namespace test process reach each node's /status API over the
	// bridge (connected route to 10.<octet>.0.0/24); without it the cluster is healthy
	// but unobservable from the test (DEBUG.md, 2026-07-04).
	h.run(t, "sudo", "ip", "link", "add", h.bridge, "type", "bridge")
	h.run(t, "sudo", "ip", "addr", "add", h.gwIP+"/24", "dev", h.bridge)
	h.run(t, "sudo", "ip", "link", "set", h.bridge, "up")

	// For each node: a namespace, a veth pair (ns end + bridge end), address the ns
	// end, bring lo + the veth up inside the ns (RESEARCH Pattern 1 recipe).
	for i := 0; i < n; i++ {
		nsEnd := fmt.Sprintf("veth-%d-n%d", pid, i)   // moved into the namespace
		hostEnd := fmt.Sprintf("veth-%d-h%d", pid, i) // stays in root, attaches to bridge
		h.run(t, "sudo", "ip", "netns", "add", h.nss[i])
		// `ip link add <nsEnd> type veth peer name <hostEnd>`: a veth pair — one end
		// per namespace, the other attached to the bridge.
		h.run(t, "sudo", "ip", "link", "add", nsEnd, "type", "veth", "peer", "name", hostEnd)
		h.run(t, "sudo", "ip", "link", "set", hostEnd, "master", h.bridge, "up")
		h.run(t, "sudo", "ip", "link", "set", nsEnd, "netns", h.nss[i])
		h.run(t, "sudo", "ip", "netns", "exec", h.nss[i], "ip", "addr", "add", h.ips[i]+"/24", "dev", nsEnd)
		h.run(t, "sudo", "ip", "netns", "exec", h.nss[i], "ip", "link", "set", nsEnd, "up")
		h.run(t, "sudo", "ip", "netns", "exec", h.nss[i], "ip", "link", "set", "lo", "up")
	}

	// Sanity check (Pitfall 5): a wiring bug fails HERE with a clear message instead
	// of a mysterious no-leader timeout downstream.
	//
	// (a) ns<->ns: proves the consensus plane (bridge-switched, same subnet) works.
	h.run(t, "sudo", "ip", "netns", "exec", h.nss[0], "ping", "-c1", "-W1", h.ips[1])
	// (b) root->ns: proves the OBSERVATION path works — the test polls /status from the
	// root ns, so the root ns must reach the node subnet over the bridge. This is the
	// exact path the missing bridge IP broke; the guard turns that regression into a
	// clear setup failure instead of a downstream "no initial leader" (DEBUG.md).
	h.run(t, "sudo", "ping", "-c1", "-W1", h.ips[0])

	// Shared -peers spec from the namespace IPs (all share peer port 7001 — each is in
	// its own netns, so no collision).
	specParts := make([]string, n)
	for i := 0; i < n; i++ {
		specParts[i] = fmt.Sprintf("n%d=%s:7001", i, h.ips[i])
	}
	peersSpec := strings.Join(specParts, ",")

	// Spawn each toyraftd INTO its namespace.
	for i := 0; i < n; i++ {
		peerAddr := h.ips[i] + ":7001"
		clientAddr := fmt.Sprintf("%s:%d", h.ips[i], 7001+clientPortOffset)
		dataDir := filepath.Join(tmp, "data-n"+strconv.Itoa(i))
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			t.Fatalf("mkdir data dir for n%d: %v", i, err)
		}
		h.nodes = append(h.nodes, h.spawnNode(h.nss[i], "n"+strconv.Itoa(i), peerAddr, clientAddr, dataDir, peersSpec))
	}

	return h
}

// spawnNode launches one toyraftd INSIDE the namespace ns (via `sudo ip netns exec`)
// in its OWN process group and returns the node handle. pgid == pid because Setpgid
// with Pgid==0 makes the child its own group leader (ADR-0018 PGID discipline).
func (h *harness) spawnNode(ns, id, peerAddr, clientAddr, dataDir, peersSpec string) *node {
	h.t.Helper()
	cmd := exec.Command("sudo", "ip", "netns", "exec", ns,
		h.bin,
		"-id", id,
		"-peers", peersSpec,
		"-listen", clientAddr,
		"-data-dir", dataDir,
		"-seed", strconv.FormatInt(h.seed, 10),
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // own group; pgid == pid (ADR-0018)
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
// On any error (dead/frozen/partitioned node, non-200) it returns ok=false so callers
// treat the node as "not leader / unknown commit".
func (h *harness) getStatus(clientAddr string) (statusWire, bool) {
	var s statusWire
	resp, err := h.client.Get("http://" + clientAddr + "/status")
	if err != nil {
		return s, false // dead/frozen/partitioned node => not leader
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

// maxCommitIndex returns the largest commit_index reported by any live survivor (every
// node except the excluded one and any already-killed node). Ported from
// processkill/harness_test.go so netns_test.go (13-02) resolves it here.
func (h *harness) maxCommitIndex(excluding *node) uint64 {
	var max uint64
	for _, n := range h.nodes {
		if n == excluding || n.killed {
			continue
		}
		if ci, ok := h.commitIndexOf(n.clientAddr); ok && ci > max {
			max = ci
		}
	}
	return max
}

// majorityCommitAtLeast polls until at least `majority` live survivors report a
// commit_index >= want, or the deadline passes. It reads the COMMIT SEAM (/status
// commit_index), proving DURABLE replication rather than a leader-served read. Ported
// from processkill/harness_test.go so netns_test.go (13-02) resolves it here.
func (h *harness) majorityCommitAtLeast(want uint64, majority int, deadline time.Time) bool {
	for time.Now().Before(deadline) {
		count := 0
		for _, n := range h.nodes {
			if n.killed {
				continue
			}
			if ci, ok := h.commitIndexOf(n.clientAddr); ok && ci >= want {
				count++
			}
		}
		if count >= majority {
			return true
		}
		time.Sleep(pollInterval)
	}
	return false
}

// setKV issues PUT /kv/<k> with v as the body through the 307-following client. The
// body is a bytes.Reader so http.NewRequest sets Request.GetBody, letting the default
// client REPLAY the body across a follower's 307 to the leader (the 10-03 correctness
// point). The Phase-12 history-capture seam is dropped — this is a plain PUT.
func (h *harness) setKV(leaderClientAddr, k, v string) error {
	req, err := http.NewRequest(http.MethodPut, "http://"+leaderClientAddr+"/kv/"+k, bytes.NewReader([]byte(v)))
	if err != nil {
		return fmt.Errorf("build PUT %s: %w", k, err)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("PUT %s: %w", k, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PUT %s: server returned %s: %s", k, resp.Status, strings.TrimSpace(string(body)))
	}
	return nil
}

// getKV issues GET /kv/<k> through the 307-following client. A 200 returns the value;
// a 404 returns an empty string + nil error (absent key); any other status is an error.
// The Phase-12 history-capture seam is dropped — this is a plain GET.
func (h *harness) getKV(leaderClientAddr, k string) (string, error) {
	resp, err := h.client.Get("http://" + leaderClientAddr + "/kv/" + k)
	if err != nil {
		return "", fmt.Errorf("GET %s: %w", k, err)
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", fmt.Errorf("GET %s read body: %w", k, err)
		}
		return string(body), nil
	case http.StatusNotFound:
		return "", nil
	default:
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("GET %s: server returned %s: %s", k, resp.Status, strings.TrimSpace(string(body)))
	}
}

// partition severs EXACTLY the link between the nodes at indices a and b by installing
// per-namespace DROP rules INSIDE each victim's own namespace (RESEARCH Pattern 3): a
// drops b's IP on INPUT+OUTPUT and b drops a's, so the a<->b link is cut while the third
// node keeps quorum. Rules are per-namespace so they never touch the bridge or the third
// node (Anti-pattern: NEVER partition in the root/bridge ns). The same `iptables` binary
// on PATH is used for -A here and -D in heal (Pitfall 2 — never mix nft/legacy).
func (h *harness) partition(a, b int) {
	h.run(h.t, "sudo", "ip", "netns", "exec", h.nss[a], "iptables", "-A", "INPUT", "-s", h.ips[b], "-j", "DROP")
	h.run(h.t, "sudo", "ip", "netns", "exec", h.nss[a], "iptables", "-A", "OUTPUT", "-d", h.ips[b], "-j", "DROP")
	h.run(h.t, "sudo", "ip", "netns", "exec", h.nss[b], "iptables", "-A", "INPUT", "-s", h.ips[a], "-j", "DROP")
	h.run(h.t, "sudo", "ip", "netns", "exec", h.nss[b], "iptables", "-A", "OUTPUT", "-d", h.ips[a], "-j", "DROP")
}

// heal restores the a<->b link by deleting the SAME four DROP rules partition added
// (-D instead of -A), via the same iptables binary on PATH (Pitfall 2).
func (h *harness) heal(a, b int) {
	h.run(h.t, "sudo", "ip", "netns", "exec", h.nss[a], "iptables", "-D", "INPUT", "-s", h.ips[b], "-j", "DROP")
	h.run(h.t, "sudo", "ip", "netns", "exec", h.nss[a], "iptables", "-D", "OUTPUT", "-d", h.ips[b], "-j", "DROP")
	h.run(h.t, "sudo", "ip", "netns", "exec", h.nss[b], "iptables", "-D", "INPUT", "-s", h.ips[a], "-j", "DROP")
	h.run(h.t, "sudo", "ip", "netns", "exec", h.nss[b], "iptables", "-D", "OUTPUT", "-d", h.ips[a], "-j", "DROP")
}

// isolate severs the node at index v from EVERY other node — a full MINORITY
// partition — by reusing the per-link partition primitive against each peer. This is
// the correct 3-node partition shape: a single-link cut (partition(a,b) alone) leaves
// both "cut" nodes still reachable via the third, and with no PreVote / no check-quorum
// step-down (pkg/raft) the two split nodes alternately win the third's vote → leadership
// FLAPS and a mid-sequence client write blocks in Propose until it times out (DEBUG.md
// bug #2, 2026-07-04). Isolating ONE node instead leaves the remaining two a STABLE
// connected majority; the isolated node's election packets are all dropped so it never
// disrupts the majority's leader while the partition holds.
func (h *harness) isolate(v int) {
	for i := range h.nodes {
		if i != v {
			h.partition(v, i)
		}
	}
}

// rejoin restores every link isolate(v) cut (its inverse), letting node v rejoin. With
// no PreVote the rejoining node's inflated term can force ONE re-election on rejoin;
// callers tolerate that via commitKV's re-find-and-retry.
func (h *harness) rejoin(v int) {
	for i := range h.nodes {
		if i != v {
			h.heal(v, i)
		}
	}
}

// indexOf returns the slice index of node n (nodes carry no self-index), or -1.
func (h *harness) indexOf(n *node) int {
	for i := range h.nodes {
		if h.nodes[i] == n {
			return i
		}
	}
	return -1
}

// commitKV writes k=v against the CURRENT leader, tolerating transient leadership: it
// (re-)finds the leader and PUTs; on any error — a stale leader that accepts then blocks
// until the client timeout, or a redirect that races a step-down — it re-finds and
// retries until the deadline. Returns nil once a leader accepts and commits the write.
// This absorbs the single disruptive re-election a no-PreVote node can trigger on rejoin
// (DEBUG.md bug #2); during the stable-majority partition phase the first attempt
// succeeds immediately.
func (h *harness) commitKV(k, v string, deadline time.Time) error {
	var lastErr error
	for time.Now().Before(deadline) {
		lead, ok := h.findLeader(time.Now().Add(2 * time.Second))
		if !ok {
			lastErr = fmt.Errorf("no leader")
			continue
		}
		if err := h.setKV(lead.clientAddr, k, v); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("deadline exceeded before any leader accepted the write")
	}
	return fmt.Errorf("commitKV %s: %w", k, lastErr)
}

// teardown is the SINGLE LIFO cleanup registered in newHarness. Order (Pitfall 3,
// ADR-0018 kill-then-assert): kill every daemon PGID + reap, `ip netns del` each
// namespace (auto-reaps its veth end + iptables rules), then delete the root-ns bridge
// explicitly, then the SC1 namespace-absence leak gate. Every failure is reported via
// t.Errorf — NEVER t.Fatalf, which is disallowed in a cleanup func.
func (h *harness) teardown() {
	for _, n := range h.nodes {
		if n.killed {
			continue
		}
		// The daemon runs as root under `sudo ip netns exec`, so the unprivileged
		// test's negative-pgid signal may hit EPERM; fall back to `sudo kill`.
		if err := syscall.Kill(-n.pgid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
			_ = h.runQuiet("sudo", "kill", "--", "-"+strconv.Itoa(n.pgid))
		}
		_ = n.cmd.Wait() // reap
		n.killed = true
	}
	for _, ns := range h.nss {
		if err := h.runQuiet("sudo", "ip", "netns", "del", ns); err != nil &&
			!strings.Contains(err.Error(), "No such file or directory") {
			h.t.Errorf("teardown: ip netns del %s: %v", ns, err)
		}
	}
	if err := h.runQuiet("sudo", "ip", "link", "del", h.bridge); err != nil &&
		!strings.Contains(err.Error(), "Cannot find device") {
		h.t.Errorf("teardown: ip link del %s: %v", h.bridge, err)
	}
	h.assertNoLeak()
}

// assertNoLeak is the SC1 positive-absence gate (the netns analog of processkill's
// `ps -o pgid=` process-absence check): after teardown, NONE of this run's
// `tr-<pid>-nX` namespaces may still appear in `ip netns list`. Data dirs live under
// t.TempDir() (auto-removed) so no manual rm is needed. Every leak is reported via
// t.Errorf — never t.Fatalf, which is disallowed in a cleanup func.
func (h *harness) assertNoLeak() {
	out, _ := exec.Command("ip", "netns", "list").Output()
	for _, ns := range h.nss {
		if strings.Contains(string(out), ns) {
			h.t.Errorf("leak: netns %s still present after teardown", ns)
		}
	}
}

// repoRoot walks up from the test's working directory to the module root (the dir
// holding go.mod), so `go build ./cmd/toyraftd` resolves regardless of where the test
// binary runs from.
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
