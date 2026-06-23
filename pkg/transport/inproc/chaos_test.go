package inproc_test

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	"github.com/prajwalmahajan101/toyraft/pkg/transport/inproc"
)

// deliveredMsg is the per-receiver record captured by SC3 / chaos
// tests. It is intentionally narrow: receivers cannot observe
// deliverAt directly, so we record only the (from, to, term) tuple in
// receive order. Two runs at the same seed must produce the same
// per-receiver slice byte-for-byte.
type deliveredMsg struct {
	From raft.NodeID
	To   raft.NodeID
	Term raft.Term
}

// recordingRig spins up one drain goroutine per endpoint, appending
// received (from, to, term) tuples under a single shared mutex to a
// total-order receive log. The mutex serialises receives across
// goroutines, but the relative order *within* each receiver is what
// SC3 cares about — same-seed runs deliver in the same per-receiver
// order, and the dispatcher's single goroutine + (deliverAt, seq)
// total order makes the interleaved log itself reproducible.
type recordingRig struct {
	mu        sync.Mutex
	perReader map[raft.NodeID][]deliveredMsg
	wg        sync.WaitGroup
	stop      chan struct{}
}

func newRecordingRig() *recordingRig {
	return &recordingRig{
		perReader: make(map[raft.NodeID][]deliveredMsg),
		stop:      make(chan struct{}),
	}
}

func (r *recordingRig) attach(ep *inproc.Endpoint) {
	r.wg.Go(func() {
		for {
			select {
			case m, ok := <-ep.Recv():
				if !ok {
					return
				}
				r.mu.Lock()
				r.perReader[ep.ID()] = append(r.perReader[ep.ID()], deliveredMsg{
					From: m.From,
					To:   m.To,
					Term: m.Term,
				})
				r.mu.Unlock()
			case <-r.stop:
				return
			}
		}
	})
}

func (r *recordingRig) drain() map[raft.NodeID][]deliveredMsg {
	close(r.stop)
	r.wg.Wait()
	return r.perReader
}

// quiesce gives the dispatcher a brief wall-clock window to drain any
// in-flight deliveries onto receiver goroutines. It is a test-only
// scheduling helper — NOT a determinism boundary; the trace itself is
// determined by seed + FakeClock, the sleep only ensures all
// scheduled deliveries land before we snapshot the trace.
func quiesce() { time.Sleep(20 * time.Millisecond) }

// TestHub_SameSeedIdenticalTrace is the SC3 acceptance test. Two runs
// at the same int64 seed with the same canned message sequence must
// produce a byte-identical delivery trace. Discharges all five chaos
// knobs simultaneously.
func TestHub_SameSeedIdenticalTrace(t *testing.T) {
	const seed = int64(0xC0FFEE)
	h1 := runChaosScenario(t, seed)
	h2 := runChaosScenario(t, seed)
	if !reflect.DeepEqual(h1, h2) {
		t.Fatalf("SC3: same seed produced different traces\nrun1=%#v\nrun2=%#v", h1, h2)
	}
	h3 := runChaosScenario(t, 0xDECAFBAD)
	if reflect.DeepEqual(h1, h3) {
		t.Fatalf("SC3 sanity: different seeds produced the same trace; the chaos layer is not seed-dependent")
	}
}

// runChaosScenario drives a fixed message sequence through a Hub
// configured with non-trivial drop / delay / duplicate (no partition
// for SC3 — partition symmetry has its own test). Returns the
// per-receiver delivery trace.
func runChaosScenario(t *testing.T, seed int64) map[raft.NodeID][]deliveredMsg {
	t.Helper()
	clk := clock.NewFake()
	h, err := inproc.NewHub(inproc.HubConfig{Clock: clk, Seed: seed})
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	defer h.Close()

	h.DropRate("A", 0.1)
	h.DropRate("B", 0.1)
	h.DropRate("C", 0.1)
	h.Delay(1*time.Millisecond, 10*time.Millisecond)
	h.Duplicate(0.05)

	a := h.Connect("A")
	b := h.Connect("B")
	c := h.Connect("C")

	rig := newRecordingRig()
	rig.attach(a)
	rig.attach(b)
	rig.attach(c)

	pairs := []struct{ from, to raft.NodeID }{
		{"A", "B"}, {"B", "C"}, {"C", "A"},
		{"A", "C"}, {"B", "A"}, {"C", "B"},
	}
	endpoints := map[raft.NodeID]*inproc.Endpoint{"A": a, "B": b, "C": c}

	const total = 100
	for i := range total {
		p := pairs[i%len(pairs)]
		ep := endpoints[p.from]
		_ = ep.Send(context.Background(), raft.Message{
			Type: raft.MsgAppendEntries,
			Term: raft.Term(i),
			From: p.from,
			To:   p.to,
		})
		clk.Advance(1 * time.Millisecond)
	}
	// Push the clock past the max delay so the last batch of delayed
	// deliveries become due.
	clk.Advance(20 * time.Millisecond)
	quiesce()

	return rig.drain()
}

// TestHub_PartitionIsSymmetric guards RESEARCH Pitfall 7: Partition(a,
// b) installs BOTH directions; Heal(a, b) removes both.
func TestHub_PartitionIsSymmetric(t *testing.T) {
	clk := clock.NewFake()
	h, err := inproc.NewHub(inproc.HubConfig{Clock: clk, Seed: 1})
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	defer h.Close()

	a := h.Connect("A")
	b := h.Connect("B")

	h.Partition("A", "B")
	_ = a.Send(context.Background(), msg("A", "B", 1))
	_ = b.Send(context.Background(), msg("B", "A", 2))
	clk.Advance(100 * time.Millisecond)
	quiesce()

	select {
	case got := <-a.Recv():
		t.Fatalf("partition leaked B->A: got %+v", got)
	default:
	}
	select {
	case got := <-b.Recv():
		t.Fatalf("partition leaked A->B: got %+v", got)
	default:
	}

	h.Heal("A", "B")
	_ = a.Send(context.Background(), msg("A", "B", 3))
	_ = b.Send(context.Background(), msg("B", "A", 4))
	clk.Advance(100 * time.Millisecond)

	select {
	case got := <-b.Recv():
		if got.Term != 3 {
			t.Fatalf("post-heal A->B: want term=3, got %+v", got)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("post-heal A->B did not deliver within 500ms")
	}
	select {
	case got := <-a.Recv():
		if got.Term != 4 {
			t.Fatalf("post-heal B->A: want term=4, got %+v", got)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("post-heal B->A did not deliver within 500ms")
	}
}

// TestHub_Delay_AppliedPerSend uses a zero-span delay range so the
// deterministic delayMin is applied without an RNG draw — the message
// must NOT be delivered before clk advances past deliverAt.
func TestHub_Delay_AppliedPerSend(t *testing.T) {
	clk := clock.NewFake()
	h, err := inproc.NewHub(inproc.HubConfig{Clock: clk, Seed: 1})
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	defer h.Close()

	a := h.Connect("A")
	b := h.Connect("B")

	h.Delay(50*time.Millisecond, 50*time.Millisecond)

	_ = a.Send(context.Background(), msg("A", "B", 7))

	clk.Advance(49 * time.Millisecond)
	quiesce()
	select {
	case got := <-b.Recv():
		t.Fatalf("delay leaked at t=49ms: got %+v", got)
	default:
	}

	clk.Advance(2 * time.Millisecond)
	select {
	case got := <-b.Recv():
		if got.Term != 7 {
			t.Fatalf("want term=7 after advancing past delay, got %+v", got)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("message did not deliver within 500ms after delay elapsed")
	}
}

// TestHub_DropRate_OneMeansAlways: p=1.0 drops every message; p=0.0
// drops none.
func TestHub_DropRate_OneMeansAlways(t *testing.T) {
	clk := clock.NewFake()
	h, err := inproc.NewHub(inproc.HubConfig{Clock: clk, Seed: 1})
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	defer h.Close()

	a := h.Connect("A")
	b := h.Connect("B")

	h.DropRate("A", 1.0)
	for i := range raft.Term(20) {
		_ = a.Send(context.Background(), msg("A", "B", i))
	}
	clk.Advance(50 * time.Millisecond)
	quiesce()

	dropCount := 0
loop1:
	for {
		select {
		case <-b.Recv():
			dropCount++
		default:
			break loop1
		}
	}
	if dropCount != 0 {
		t.Fatalf("DropRate=1.0 leaked %d messages", dropCount)
	}

	h.DropRate("A", 0.0)
	for i := raft.Term(20); i < 40; i++ {
		_ = a.Send(context.Background(), msg("A", "B", i))
	}
	clk.Advance(50 * time.Millisecond)
	quiesce()

	keep := 0
loop2:
	for {
		select {
		case <-b.Recv():
			keep++
		default:
			break loop2
		}
	}
	if keep != 20 {
		t.Fatalf("DropRate=0.0 lost %d/20 messages", 20-keep)
	}
}

// TestHub_Duplicate_OneMeansAlways: p=1.0 delivers every message
// twice.
func TestHub_Duplicate_OneMeansAlways(t *testing.T) {
	clk := clock.NewFake()
	h, err := inproc.NewHub(inproc.HubConfig{Clock: clk, Seed: 1})
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	defer h.Close()

	a := h.Connect("A")
	b := h.Connect("B")

	h.Duplicate(1.0)
	h.Delay(0, 0)

	_ = a.Send(context.Background(), msg("A", "B", 42))
	clk.Advance(10 * time.Millisecond)
	quiesce()

	got := 0
loop:
	for {
		select {
		case m := <-b.Recv():
			if m.Term != 42 {
				t.Fatalf("dup delivered wrong term: %+v", m)
			}
			got++
		default:
			break loop
		}
	}
	if got != 2 {
		t.Fatalf("Duplicate=1.0: want 2 deliveries, got %d", got)
	}
}

// TestHub_Reorder_QDOneDegradesToFIFO documents RESEARCH Pitfall 6:
// queueDepth=1 is a no-op; deliveries arrive in send order. This is
// the limitation acknowledgment — NOT the recommended config.
func TestHub_Reorder_QDOneDegradesToFIFO(t *testing.T) {
	clk := clock.NewFake()
	h, err := inproc.NewHub(inproc.HubConfig{Clock: clk, Seed: 1})
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	defer h.Close()

	a := h.Connect("A")
	b := h.Connect("B")

	h.Reorder(true, 1)
	h.Delay(0, 0)

	for i := range raft.Term(5) {
		_ = a.Send(context.Background(), msg("A", "B", i))
	}
	clk.Advance(10 * time.Millisecond)
	quiesce()

	for i := range raft.Term(5) {
		select {
		case got := <-b.Recv():
			if got.Term != i {
				t.Fatalf("QD=1 FIFO violated at idx %d: got term=%d, want %d", i, got.Term, i)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("missing delivery at idx %d", i)
		}
	}
}

// TestHub_Reorder_QDFiveShuffles asserts that with a large enough
// queue depth, the dispatcher emits a permutation of the input — every
// element appears exactly once AND at least one seed within a small
// search space produces a NON-identity permutation.
func TestHub_Reorder_QDFiveShuffles(t *testing.T) {
	seeds := []int64{0, 1, 2, 3, 4}
	sawNonIdentity := false

	for _, seed := range seeds {
		clk := clock.NewFake()
		h, err := inproc.NewHub(inproc.HubConfig{Clock: clk, Seed: seed})
		if err != nil {
			t.Fatalf("NewHub seed=%d: %v", seed, err)
		}

		a := h.Connect("A")
		b := h.Connect("B")

		h.Reorder(true, 5)
		h.Delay(0, 0)

		for i := range raft.Term(5) {
			_ = a.Send(context.Background(), msg("A", "B", i))
		}
		clk.Advance(10 * time.Millisecond)
		quiesce()

		got := make([]raft.Term, 0, 5)
		for range 5 {
			select {
			case m := <-b.Recv():
				got = append(got, m.Term)
			case <-time.After(500 * time.Millisecond):
				t.Fatalf("seed=%d: missing delivery; got so far=%v", seed, got)
			}
		}
		_ = h.Close()

		// Permutation check: every value in [0,5) appears exactly once.
		seen := make([]bool, 5)
		for _, term := range got {
			if int(term) < 0 || int(term) >= 5 || seen[int(term)] {
				t.Fatalf("seed=%d: bad permutation %v", seed, got)
			}
			seen[int(term)] = true
		}

		identity := true
		for i, term := range got {
			if int(term) != i {
				identity = false
				break
			}
		}
		if !identity {
			sawNonIdentity = true
		}
	}

	if !sawNonIdentity {
		t.Fatal("Reorder QD=5: no seed in {0..4} produced a non-identity permutation; the shuffle is suspect")
	}
}
