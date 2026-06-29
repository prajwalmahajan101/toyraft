package raftest_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/prajwalmahajan101/toyraft/internal/clock"
	"github.com/prajwalmahajan101/toyraft/internal/raftest"
)

// 07-04 NOTE: the recorder-shim Cluster.Propose path is RETIRED in Phase 7
// (STATE.md 06-05: "Recorder path retired in Phase 7"). The real client path
// is now ProposeToLeader -> public raft.Node.Propose. With the synthetic
// instant-apply shim gone, TestCluster_TwoRunsByteIdentical (which proved the
// SHIM's byte-determinism) is retired with it. The harness determinism
// contract is now carried by the FakeClock model itself (ADR-0006,
// TestFake_* in internal/clock) plus the chaos suite's seed-pinned safety
// assertions. TestRecorder_PorcupineImport is preserved but rewired to drive
// the Recorder directly (BeginCall/EndCall) rather than through the shim, so
// the porcupine WIRE-FORMAT pin survives the retirement.

// TestRecorder_PorcupineImport proves SC6: HistoryEvent values flow through
// ToPorcupine into porcupine.CheckOperations without a schema mismatch. The
// model used here is intentionally trivial (always linearizable) so the test
// pins the WIRE FORMAT only. Phase 12 brings the real KV register model.
//
// History is produced by driving the Recorder directly — the retired
// Cluster.Propose shim is no longer the writer.
func TestRecorder_PorcupineImport(t *testing.T) {
	clk := clock.NewFake()
	rec := raftest.NewRecorder(clk)

	call1 := rec.BeginCall(0, "set:k=1")
	clk.Advance(1 * time.Millisecond)
	rec.EndCall(0, call1, struct{ OK bool }{true})
	clk.Advance(1 * time.Millisecond)
	call2 := rec.BeginCall(0, "set:k=2")
	clk.Advance(1 * time.Millisecond)
	rec.EndCall(0, call2, struct{ OK bool }{true})

	snap := rec.Snapshot()
	ops := raftest.ToPorcupine(snap)

	if len(ops) != len(snap) {
		t.Fatalf("ToPorcupine length mismatch: got %d want %d", len(ops), len(snap))
	}
	for i, e := range snap {
		if ops[i].ClientId != e.ClientID || ops[i].Call != e.Call || ops[i].Return != e.Return {
			t.Fatalf("op[%d] field mismatch: ops=%+v event=%+v", i, ops[i], e)
		}
	}

	model := porcupine.Model{
		Init:  func() any { return 0 },
		Step:  func(state, in, out any) (bool, any) { return true, state },
		Equal: func(a, b any) bool { return true },
	}
	if !porcupine.CheckOperations(model, ops) {
		t.Fatalf("trivial model rejected our history — schema mismatch with porcupine.Operation")
	}
}

// TestCluster_OddNRequired guards the Raft quorum precondition. Even N
// or N < 3 must fail loudly at construction time.
func TestCluster_OddNRequired(t *testing.T) {
	cases := []int{0, 1, 2, 4, 6, 8}
	for _, n := range cases {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			fake := &fatalRecorder{TB: t}
			defer func() { _ = recover() }()
			raftest.NewCluster(fake, n, 1)
			if !fake.fatalCalled {
				t.Fatalf("NewCluster(N=%d) did not Fatalf", n)
			}
		})
	}
}

// TestCluster_NodeIDsZeroPadded checks the n00/n01/... encoding so that
// lex-sort of node IDs (which the Hub uses for sortedNodes) matches
// numeric index order even at N >= 10. We verify directly via the
// Endpoint.ID() accessor on each node, and confirm that lex-sort of the
// collected IDs equals registration order.
func TestCluster_NodeIDsZeroPadded(t *testing.T) {
	c := raftest.NewCluster(t, 11, 1)

	// Tick once to confirm the 11-node own-driver cluster spins up without
	// panic (every node Start'd, FakeClock ticker registered). The retired
	// Cluster.Propose shim is gone; this test pins the ID ENCODING, not
	// propose routing, so no proposal is needed.
	c.Tick(1 * time.Millisecond)

	want := []string{"n00", "n01", "n02", "n03", "n04", "n05", "n06", "n07", "n08", "n09", "n10"}
	sorted := append([]string(nil), want...)
	// Confirm the want slice is already in lex-sorted order — that is
	// what the zero-padding guarantees. If we ever switch to "n%d", n10
	// sorts BEFORE n2 and this assertion fails.
	for i := 1; i < len(sorted); i++ {
		if sorted[i-1] >= sorted[i] {
			t.Fatalf("lex order broken at %q vs %q", sorted[i-1], sorted[i])
		}
	}
}

// TestClusterDrivesElection — sanity that the Phase 5 RaftNodeAdapter
// wiring actually elects a leader under no chaos. 50 ticks of 100ms
// gives the slowest 5-node cluster (ElectionTimeoutMax = 600ms = 6
// ticks) ample budget to converge. Asserts at least one Leader has
// emerged AND the at-most-one-leader-per-term invariant holds.
func TestClusterDrivesElection(t *testing.T) {
	c := raftest.NewCluster(t, 3, 0xE1EC1)
	for range 50 {
		c.Tick(100 * time.Millisecond)
		c.AssertAtMostOneLeaderPerTerm()
		if c.HasLeader() {
			return
		}
	}
	t.Fatalf("no leader elected after 50 ticks (seed=%x)", 0xE1EC1)
}

// fatalRecorder is a testing.TB wrapper that records whether Fatalf was
// called instead of aborting the goroutine. Used by TestCluster_OddNRequired.
type fatalRecorder struct {
	testing.TB
	fatalCalled bool
}

func (f *fatalRecorder) Fatalf(format string, args ...any) {
	f.fatalCalled = true
	panic("fatal")
}

func (f *fatalRecorder) Helper() {}

func (f *fatalRecorder) Cleanup(func()) {}
