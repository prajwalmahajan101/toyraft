//go:build linux || darwin

package processkill

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/raftest"
)

// seed is the deterministic RNG seed handed to every toyraftd. Process-kill
// histories are NOT byte-deterministic (real wall-clock timestamps in the
// Recorder) — that is expected; SC1 byte-determinism is inproc-only. The seed
// still fixes the daemons' election-timeout draws and names the artifact dir.
const seed = 11

// TestProcessKill proves SC2/CHAOS-02/03: boot N real toyraftd subprocesses each in
// its own PGID, set a value, SIGKILL the leader's WHOLE group via the negative pgid,
// assert a NEW leader within 1 s + the pre-kill write survives, prove M new writes
// COMMIT from the /status commit_index seam (the positive oracle, not read-back),
// and dump the recorded history in []raftest.HistoryEvent shape (SC4). t.Cleanup's
// ps/lsof leak gate asserts zero orphaned processes/ports.
func TestProcessKill(t *testing.T) {
	if testing.Short() {
		t.Skip("process-kill spawns subprocesses")
	}

	h := newHarness(t, 5, seed) // N=5 per RESEARCH Open-Q1

	// Wait for an INITIAL leader within a generous boot deadline. The specific
	// node is not retained: the pre-kill write re-resolves the leader per attempt
	// (withLeaderRetry) and the leader to kill is re-resolved just before the kill.
	if _, ok := h.findLeader(time.Now().Add(5 * time.Second)); !ok {
		t.Fatal("no initial leader within boot deadline")
	}

	// Pre-kill write (survival setup). Writes go through the leader; the
	// 307-following client re-sends the body across any redirect churn. A brief
	// initial split election can hand leadership to a short-lived leader that
	// steps down mid-propose (v1 Propose does not cancel in-flight proposals on
	// step-down — pkg/raft/node_public.go), so the write is wrapped in a
	// leader-re-resolving retry rather than pinned to the boot-time leader.
	writeDeadline := time.Now().Add(5 * time.Second)
	if err := h.withLeaderRetry(writeDeadline, func(addr string) error { return h.setKV(addr, "k", "v") }); err != nil {
		t.Fatalf("pre-kill set k=v: %v", err)
	}
	if err := h.withLeaderRetry(writeDeadline, func(addr string) error {
		got, err := h.getKV(addr, "k")
		if err != nil {
			return err
		}
		if got != "v" {
			return fmt.Errorf("got %q, want %q", got, "v")
		}
		return nil
	}); err != nil {
		t.Fatalf("pre-kill get k: %v", err)
	}

	// Re-resolve the CURRENT leader before the kill: early churn may have left any
	// earlier leader stale, and CHAOS-02 must kill the ACTUAL leader (killing a
	// follower would make failover vacuously "succeed").
	lead, ok := h.findLeader(time.Now().Add(5 * time.Second))
	if !ok {
		t.Fatal("no leader to kill after pre-kill write")
	}

	// Baseline commit_index over the survivors BEFORE the new writes — the max
	// commit_index any survivor reports (the commit oracle reads the COMMIT SEAM).
	baseline := h.maxCommitIndex(lead)

	// Kill the leader's WHOLE process group via the negative pgid.
	h.killLeaderGroup(lead)

	// SC2: a NEW leader must appear within 1 second of the kill.
	deadline := time.Now().Add(1 * time.Second)
	newLead, ok := h.findLeader(deadline)
	if !ok {
		t.Fatalf("no new leader within 1s after killing %s (seed=%d)", lead.id, seed)
	}
	if newLead.id == lead.id {
		t.Fatalf("re-elected the killed leader %s (seed=%d)", lead.id, seed)
	}

	// Positive commit oracle (>=M) from the COMMIT SEAM. Write M new keys against
	// the new leader, then poll until a MAJORITY of survivors report
	// commit_index >= baseline + M. Read-back alone (a 307-followed GET served from
	// the leader's applied KVSM) does NOT prove a durable commit — the commit_index
	// advance does.
	const m = 3
	for i := 0; i < m; i++ {
		k, v := fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)
		if err := h.withLeaderRetry(time.Now().Add(5*time.Second), func(addr string) error { return h.setKV(addr, k, v) }); err != nil {
			t.Fatalf("post-election set k%d: %v (seed=%d)", i, err, seed)
		}
	}

	majority := len(h.nodes)/2 + 1 // majority of the FULL cluster (survivors must still form one)
	oracleDeadline := time.Now().Add(2 * time.Second)
	if !h.majorityCommitAtLeast(baseline+m, majority, oracleDeadline) {
		t.Fatalf("commit oracle: commit_index did not advance >= %d on a majority (baseline=%d, seed=%d)",
			m, baseline, seed)
	}

	// Write-survival oracle (CHAOS-03): the committed pre-kill value is readable
	// against the NEW leader. This is checked AFTER the post-election writes commit,
	// not immediately after re-election, because v1 reads are leader-only-reads off
	// the applied KVSM with no read-index / no leader no-op (.journal/M10.md — a
	// documented v1 limitation). If the leader is killed in the narrow window after
	// it committed+applied the pre-kill entry but BEFORE that commit index reached
	// the followers, every survivor holds the entry in its LOG (durable) yet has not
	// APPLIED it, and a freshly elected leader cannot serve it until it commits an
	// entry in its OWN term (Raft §5.4.2 / Figure-8). The post-election writes above
	// are exactly that current-term commit: they carry the pre-kill entry into the
	// applied map by Log Matching, making this read deterministic.
	if err := h.withLeaderRetry(time.Now().Add(5*time.Second), func(addr string) error {
		got, err := h.getKV(addr, "k")
		if err != nil {
			return err
		}
		if got != "v" {
			return fmt.Errorf("got %q, want %q", got, "v")
		}
		return nil
	}); err != nil {
		t.Fatalf("write survival: get k after re-election: %v (seed=%d)", err, seed)
	}

	// History dump (SC4): the on-disk JSON MUST deserialize into
	// []raftest.HistoryEvent (Phase 12 TestHistoryShape reads exactly this).
	// Process-kill histories are wall-clock (non-deterministic) by design.
	dumpHistory(t, seed, h.recorder.Snapshot())

	// teardown (registered by newHarness) runs the ps/lsof leak gate at test end.
}

// maxCommitIndex returns the largest commit_index reported by any live survivor
// (every node except the one about to be, or already, killed).
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
// commit_index), proving DURABLE replication rather than a leader-served read.
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

// dumpHistory writes the recorded events to test/artifacts/chaos/<seed>/history.json
// in the same os.MkdirAll + json.MarshalIndent + os.WriteFile shape as the inproc
// suite. The on-disk JSON round-trips through []raftest.HistoryEvent (the Phase-12
// consumer shape). A local helper here deliberately avoids importing the inproc test
// package.
func dumpHistory(t *testing.T, seed int64, events []raftest.HistoryEvent) {
	t.Helper()
	dir := filepath.Join(repoRoot(t), "test", "artifacts", "chaos", fmt.Sprintf("%d", seed))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir artifacts dir %s: %v", dir, err)
	}
	data, err := json.MarshalIndent(events, "", "  ")
	if err != nil {
		t.Fatalf("marshal history: %v", err)
	}
	path := filepath.Join(dir, "history.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}

	// Guard the Phase-12 contract: the file MUST deserialize back into
	// []raftest.HistoryEvent.
	var back []raftest.HistoryEvent
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("history.json does not deserialize into []raftest.HistoryEvent: %v", err)
	}
}
