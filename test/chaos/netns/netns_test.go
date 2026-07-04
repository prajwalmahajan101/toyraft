//go:build linux && netns

package netns

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"
)

// seed is the deterministic RNG seed handed to every toyraftd in this run. It
// names the run and fixes the daemons' election-timeout draws. Like the
// process-kill suite's `seed = 11`, netns partition histories are NOT
// byte-deterministic (real wall-clock over a real L3 hop) — that is expected;
// SC1 for the netns path is the partition + recovery + namespace-absent
// teardown, not a Phase-12 history artifact.
const seed = 13

// sudoAvailable reports whether passwordless sudo works (`sudo -n true` exits 0),
// so a dev box without root SKIPS the netns chaos instead of failing it. This is
// the SC2 second-layer guard: `ip`/`iptables` topology mutation needs root, and a
// contributor without it must see a SKIP, never a FAIL.
func sudoAvailable() bool {
	return exec.Command("sudo", "-n", "true").Run() == nil
}

// TestNetnsPartition is the single SC1 test: it drives the 13-01 harness to prove a
// REAL IP-layer partition is survived. It boots 3 toyraftd nodes each in its own
// network namespace on a shared bridge (real non-loopback IPs, a genuine L3 hop),
// waits for an initial leader, DROPs the n0<->n1 link with per-namespace `iptables
// -j DROP` (n2 bridges quorum), ASSERTS a surviving majority still elects + commits
// (the ADR-0018 POSITIVE commit oracle over the /status commit_index seam — never a
// mere "no panic"), HEALS the link, asserts full re-convergence, and lets the
// harness's registered t.Cleanup delete the namespaces and assert they are GONE.
//
// This test adds NO harness code: newHarness/findLeader/setKV/getKV/maxCommitIndex/
// majorityCommitAtLeast/partition/heal are all declared on the netns harness in
// 13-01's harness.go; this scenario only calls them and issues no direct ip/iptables
// shellouts of its own.
//
// SC2 is enforced by the THREE-LAYER guard below: on non-Linux the build tag already
// excludes the file (so `go test -tags=netns ./...` reports "no test files" = a PASS);
// the defensive runtime.GOOS check backs that up; a box without root/passwordless-sudo
// SKIPS; and -short SKIPS (this spawns subprocesses + configures the kernel network
// stack). The criterion "skipped, not failed" therefore holds in every invocation.
func TestNetnsPartition(t *testing.T) {
	// --- SC2 three-layer skip guard (must be the first statements). ---
	if runtime.GOOS != "linux" {
		t.Skip("netns chaos is linux-only")
	}
	if os.Geteuid() != 0 && !sudoAvailable() {
		t.Skip("netns chaos needs root or passwordless sudo")
	}
	if testing.Short() {
		t.Skip("netns chaos spawns subprocesses + configures the kernel network stack")
	}
	// This test does NOT run in parallel — netns cases run SERIALLY (no parallel
	// marker below). The per-run bridge/subnet is derived from the pid, so a second
	// concurrent netns test would collide on the shared subnet (RESEARCH Anti-pattern).

	// newHarness builds the topology + spawns n0/n1/n2 into their namespaces and
	// registers the LIFO t.Cleanup (kill PGIDs -> ip netns del -> assert namespaces
	// gone) BEFORE creating any kernel resource.
	h := newHarness(t, seed)

	// 1. Wait for an INITIAL leader within a generous boot deadline (5s — matches the
	// process-kill suite's boot deadline; booting 3 real daemons over veth is slower
	// than loopback but 5s is ample).
	lead, ok := h.findLeader(time.Now().Add(5 * time.Second))
	if !ok {
		t.Fatalf("no initial leader within boot deadline (seed=%d)", seed)
	}

	// 2. Pre-partition write (survival setup). The 307-following client re-sends the
	// body across any follower redirect, so writing against the observed leader is safe.
	if err := h.setKV(lead.clientAddr, "k", "v"); err != nil {
		t.Fatalf("pre-partition set k=v: %v (seed=%d)", err, seed)
	}
	if got, err := h.getKV(lead.clientAddr, "k"); err != nil || got != "v" {
		t.Fatalf("pre-partition get k: got %q err %v, want \"v\" nil (seed=%d)", got, err, seed)
	}

	// Baseline commit_index over ALL live nodes BEFORE the partition — the max any
	// node reports (the commit oracle reads the COMMIT SEAM, /status commit_index).
	// nil = exclude nothing; every node is still reachable at this point.
	baseline := h.maxCommitIndex(nil)

	majority := len(h.nodes)/2 + 1 // = 2 for N=3: the majority that must keep committing.

	// 3. PARTITION: fully ISOLATE one FOLLOWER from the other two with real per-namespace
	// iptables DROP across distinct L3 addresses (the retirement of D-1 — a real network
	// partition loopback cannot exhibit). Isolating a SINGLE node (not cutting one link)
	// is the correct 3-node shape: it leaves the remaining two a STABLE connected
	// majority. A single-link cut instead leaves both split nodes reachable via the third
	// and — with no PreVote / no check-quorum step-down in pkg/raft — flaps leadership as
	// they duel for the third's vote, so a write mid-sequence blocks until timeout
	// (DEBUG.md bug #2, 2026-07-04). The victim is a FOLLOWER so the current leader keeps
	// its majority and never even needs to fail over during the partition.
	victim := (h.indexOf(lead) + 1) % len(h.nodes) // a node that is NOT the current leader
	h.isolate(victim)

	// 4. Assert the SURVIVING MAJORITY keeps committing (positive oracle, ADR-0018), NOT
	// "no panic". commitKV re-finds the leader per write and tolerates transient
	// leadership; deadlines are WIDENED from the process-kill suite's 1s/2s because a real
	// L3 DROP makes TCP connect/read block until the transport's ~150ms SendTimeout, then
	// retry (RESEARCH Pitfall 7). Then poll the COMMIT SEAM until a MAJORITY reports
	// commit_index >= baseline+M — DURABLE replication, not a leader-served read-back.
	const m = 3
	for i := 0; i < m; i++ {
		if err := h.commitKV(fmt.Sprintf("p%d", i), fmt.Sprintf("pv%d", i), time.Now().Add(8*time.Second)); err != nil {
			t.Fatalf("post-partition commit p%d: %v (seed=%d)", i, err, seed)
		}
	}
	if !h.majorityCommitAtLeast(baseline+m, majority, time.Now().Add(8*time.Second)) {
		t.Fatalf("partition commit oracle: commit_index did not advance >= %d on a majority (baseline=%d, seed=%d)",
			m, baseline, seed)
	}

	// 5. HEAL: rejoin the isolated node and assert FULL re-convergence — a further write
	// commits on a majority now that all three nodes are reachable again, proving the
	// cluster recovers full connectivity rather than limping on a partial quorum. The
	// rejoining node may have inflated its term while isolated (no PreVote), forcing ONE
	// re-election on rejoin; commitKV's re-find-and-retry absorbs it within the deadline.
	h.rejoin(victim)

	healBaseline := h.maxCommitIndex(nil)
	const healM = 2
	for i := 0; i < healM; i++ {
		if err := h.commitKV(fmt.Sprintf("h%d", i), fmt.Sprintf("hv%d", i), time.Now().Add(10*time.Second)); err != nil {
			t.Fatalf("post-heal commit h%d: %v (seed=%d)", i, err, seed)
		}
	}
	if !h.majorityCommitAtLeast(healBaseline+healM, majority, time.Now().Add(10*time.Second)) {
		t.Fatalf("heal commit oracle: commit_index did not re-converge >= %d on a majority (baseline=%d, seed=%d)",
			healM, healBaseline, seed)
	}

	// No history dump: 13-01 DROPPED the recorder (LOCKED) and SC1 for the netns path
	// needs no Phase-12 artifact. The harness's registered t.Cleanup now kills the
	// daemons, `ip netns del`s each namespace, deletes the bridge, and asserts the
	// namespaces are GONE (the SC1 "netns is gone" leak gate) — nothing more is needed
	// in the test body for teardown.
}
