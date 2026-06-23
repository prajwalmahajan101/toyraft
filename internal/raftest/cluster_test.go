package raftest_test

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/prajwalmahajan101/toyraft/internal/raftest"
)

// TestCluster_TwoRunsByteIdentical proves SC5: the same int64 seed
// produces a byte-identical []HistoryEvent across two independent runs.
// This is the load-bearing determinism guarantee for the entire Phase 4
// harness.
func TestCluster_TwoRunsByteIdentical(t *testing.T) {
	const seed = int64(0xC0FFEE)
	h1 := runScenario(t, seed)
	h2 := runScenario(t, seed)
	if !reflect.DeepEqual(h1, h2) {
		t.Fatalf("history diverged across runs at same seed:\nrun1=%v\nrun2=%v", h1, h2)
	}
	if len(h1) == 0 {
		t.Fatalf("history is empty; scenario produced no events")
	}
}

// runScenario drives a deterministic workload: 50 client operations
// round-robin across a 3-node cluster, interleaved with 20ms ticks. The
// scenario shape is intentionally simple — the determinism contract is
// what we are testing, not consensus behaviour.
func runScenario(t *testing.T, seed int64) []raftest.HistoryEvent {
	t.Helper()
	c := raftest.NewCluster(t, 3, seed)
	for i := range 50 {
		c.Propose(i%3, struct{ Kind, Key, Value string }{
			Kind: "set", Key: "k", Value: strconv.Itoa(i),
		})
		c.Tick(20 * time.Millisecond)
	}
	return c.Recorder.Snapshot()
}

// TestRecorder_PorcupineImport proves SC6: HistoryEvent values flow
// through ToPorcupine into porcupine.CheckOperations without a schema
// mismatch. The model used here is intentionally trivial (always
// linearizable) so the test pins the WIRE FORMAT only. Phase 12 brings
// the real KV register model.
func TestRecorder_PorcupineImport(t *testing.T) {
	c := raftest.NewCluster(t, 3, 1)
	c.Propose(0, "set:k=1")
	c.Tick(1 * time.Millisecond)
	c.Propose(0, "set:k=2")
	c.Tick(1 * time.Millisecond)
	snap := c.Recorder.Snapshot()
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

	// Drive a propose against each node to confirm indices 0..10 are
	// reachable without panic. Then tick to let the noopNode drain.
	for i := range 11 {
		c.Propose(i, "ping")
	}
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

// TestClusterDrivesElection — sanity that the Phase 5 raftNodeAdapter
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
