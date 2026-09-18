package raft_test

// dogfood_test.go covers the v1.0.0 dogfood-gate additions that need a
// STARTED node (the async runTicker path the raftest model-ii harness does not
// exercise): NodeID() (FRICTION-06), NotifyC() (FRICTION-07), and the
// event-driven immediate flush on Propose (FRICTION-08).
//
// A single-node cluster (Peers=[self]) is the minimal fixture that runs the
// full new path — Propose -> broadcastAppendEntriesLocked -> signalWake ->
// runTicker flush -> drain -> apply -> waiter resolved — with NO peer message
// delivery to arrange, so a no-op Transport suffices.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	"github.com/prajwalmahajan101/toyraft/pkg/storage/memory"
)

// noopTransport is a Transport for a single-node cluster: there are no peers,
// so Send is never meaningfully invoked. All methods are inert.
type noopTransport struct{}

func (noopTransport) Send(context.Context, raft.Message) error                   { return nil }
func (noopTransport) Register(func(ctx context.Context, msg raft.Message) error) {}
func (noopTransport) Close() error                                               { return nil }

// countingSM records how many entries were applied. Snapshots are stubbed
// (v1 contract).
type countingSM struct {
	mu      sync.Mutex
	applied int
}

func (s *countingSM) Apply(e raft.Entry) (any, error) {
	s.mu.Lock()
	s.applied++
	n := s.applied
	s.mu.Unlock()
	return n, nil // opaque result echoed back through Propose (FRICTION-01)
}
func (s *countingSM) Snapshot() ([]byte, raft.Index, error) {
	return nil, 0, raft.ErrSnapshotUnsupported
}
func (s *countingSM) Restore([]byte) error { return raft.ErrSnapshotUnsupported }

// newSoloLeader builds, starts, and returns a single-node leader plus its SM.
// hb is the HeartbeatInterval (== driver tick period); election timing is set
// well above P1-5's HeartbeatInterval*3 floor.
func newSoloLeader(t *testing.T, hb time.Duration) (raft.Node, *countingSM) {
	t.Helper()
	sm := &countingSM{}
	node, err := raft.New(raft.Config{
		NodeID:             "solo",
		Peers:              []raft.NodeID{"solo"},
		Seed:               42,
		Storage:            memory.New(),
		Transport:          noopTransport{},
		StateMachine:       sm,
		HeartbeatInterval:  hb,
		ElectionTimeoutMin: hb * 4,
		ElectionTimeoutMax: hb * 6,
	})
	if err != nil {
		t.Fatalf("raft.New: %v", err)
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = node.Stop() })

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if node.Status().Role == raft.Leader {
			return node, sm
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("solo node never became leader")
	return nil, nil
}

// TestNodeIDAccessor: FRICTION-06 — Node exposes its own id.
func TestNodeIDAccessor(t *testing.T) {
	node, _ := newSoloLeader(t, 20*time.Millisecond)
	if got := node.NodeID(); got != "solo" {
		t.Fatalf("NodeID() = %q, want %q", got, "solo")
	}
}

// TestProposeFlushesWithoutTick: FRICTION-08 — a Propose commits+applies via
// the immediate out-of-tick flush, so its latency is far below one tick period
// (HeartbeatInterval). A regression to tick-bound replication would floor the
// latency at ~1 tick and fail this. Also asserts NotifyC (FRICTION-07) fired.
func TestProposeFlushesWithoutTick(t *testing.T) {
	const hb = 500 * time.Millisecond // deliberately slow ticks
	node, sm := newSoloLeader(t, hb)

	notify := node.NotifyC()
	// Drain any election-time signal so the receive below reflects this propose.
	select {
	case <-notify:
	default:
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	idx, _, res, err := node.Propose(ctx, []byte("x"))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if idx != 1 {
		t.Fatalf("Propose idx = %d, want 1", idx)
	}
	if res != 1 {
		t.Fatalf("Propose result = %v, want 1 (Apply return)", res)
	}
	// The acceptance signal: tick-bound would be ~hb (500ms); immediate is ms.
	if elapsed >= hb {
		t.Fatalf("Propose took %v (>= one tick %v): replication is tick-bound (FRICTION-08 regressed)", elapsed, hb)
	}

	if sm.applied != 1 {
		t.Fatalf("applied = %d, want 1", sm.applied)
	}
	if got := node.Status().CommitIndex; got != 1 {
		t.Fatalf("CommitIndex = %d, want 1", got)
	}

	// FRICTION-07: the commit advance fired NotifyC.
	select {
	case <-notify:
	case <-time.After(hb):
		t.Fatal("NotifyC did not fire on commit advance")
	}
}
