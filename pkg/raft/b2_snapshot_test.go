package raft

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
)

// countingSM is a StateMachine that records every applied index and implements
// Snapshot/Restore over its applied high-water. It is the B2 probe: after a
// restart from a durable checkpoint, Apply must NOT be called for indices at or
// below the snapshot (ADR-0024).
type countingSM struct {
	mu      sync.Mutex
	applied []Index
	last    Index
}

func (s *countingSM) Apply(e Entry) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = append(s.applied, e.Index)
	s.last = e.Index
	return e.Data, nil
}

func (s *countingSM) Snapshot() ([]byte, Index, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	blob, err := json.Marshal(struct {
		Last Index `json:"last"`
	}{s.last})
	return blob, s.last, err
}

func (s *countingSM) Restore(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(data) == 0 {
		s.last = 0
		return nil
	}
	var v struct {
		Last Index `json:"last"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	s.last = v.Last
	return nil
}

func (s *countingSM) appliedIndices() []Index {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Index, len(s.applied))
	copy(out, s.applied)
	return out
}

// startNode builds+starts a single-node Node over the given (shared) Storage and
// StateMachine on a fresh fake clock, driven to leadership. Cleanup stops it.
func startNode(t *testing.T, st Storage, sm StateMachine) (Node, *clock.Fake) {
	t.Helper()
	clk := clock.NewFake()
	n, err := New(Config{
		NodeID:       "n0",
		Peers:        []NodeID{"n0"},
		Storage:      st,
		Transport:    nopTransport{},
		StateMachine: sm,
		Clock:        clk,
		Seed:         1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = n.Stop() })
	advanceUntil(t, clk, testTick, func() bool { return n.Status().Role == Leader })
	return n, clk
}

// proposeAndWait proposes data and drives ticks until Propose returns.
func proposeAndWait(t *testing.T, n Node, clk *clock.Fake, data string) Index {
	t.Helper()
	type res struct {
		idx Index
		err error
	}
	done := make(chan res, 1)
	go func() {
		idx, _, _, err := n.Propose(context.Background(), []byte(data))
		done <- res{idx, err}
	}()
	for range 400 {
		select {
		case r := <-done:
			if r.err != nil {
				t.Fatalf("Propose(%q): %v", data, r.err)
			}
			return r.idx
		default:
			clk.Advance(testTick)
		}
	}
	t.Fatalf("Propose(%q) did not return", data)
	return 0
}

// TestSnapshotResumeNoDoubleApply is the B2 gating regression test (ADR-0024).
// A single-node leader applies N entries and Stops (final checkpoint). A fresh
// node reconstructed from the SAME Storage must NOT re-apply any entry at or
// below the checkpoint index, and must resume writes at N+1.
func TestSnapshotResumeNoDoubleApply(t *testing.T) {
	t.Parallel()
	const N = 3
	st := &memStorage{}

	// First lifetime: apply N entries, then Stop (checkpoint at N).
	sm1 := &countingSM{}
	n1, clk1 := startNode(t, st, sm1)
	for range N {
		proposeAndWait(t, n1, clk1, "v")
	}
	if got := len(sm1.appliedIndices()); got != N {
		t.Fatalf("first lifetime applied %d entries, want %d", got, N)
	}
	if err := n1.Stop(); err != nil {
		t.Fatalf("Stop n1: %v", err)
	}

	// The Stop checkpoint must have persisted a snapshot at N.
	snap, err := st.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if snap.Index != N {
		t.Fatalf("persisted snapshot index = %d, want %d (Stop checkpoint)", snap.Index, N)
	}

	// Second lifetime: reconstruct from the SAME Storage with a fresh SM.
	sm2 := &countingSM{}
	n2, clk2 := startNode(t, st, sm2)

	// Restore seeded ApplyIndex to the snapshot; no committed entry is above it,
	// so the applier must NOT re-apply anything (B2: no double-apply).
	if got := n2.Status().ApplyIndex; got != N {
		t.Fatalf("restarted Status().ApplyIndex = %d, want %d", got, N)
	}
	if got := sm2.appliedIndices(); len(got) != 0 {
		t.Fatalf("restart re-applied indices %v, want none (double-apply)", got)
	}

	// And the restarted leader accepts a new write at N+1 (B1+B2 together).
	idx := proposeAndWait(t, n2, clk2, "post-restart")
	if idx != N+1 {
		t.Fatalf("post-restart Propose committed at %d, want %d", idx, N+1)
	}
	if got := sm2.appliedIndices(); len(got) != 1 || got[0] != N+1 {
		t.Fatalf("post-restart applied %v, want [%d]", got, N+1)
	}
}

// TestSnapshotPeriodicCheckpoint exercises the SnapshotInterval cadence: with
// interval 1, a checkpoint is taken after each applied entry.
func TestSnapshotPeriodicCheckpoint(t *testing.T) {
	t.Parallel()
	st := &memStorage{}
	sm := &countingSM{}
	clk := clock.NewFake()
	n, err := New(Config{
		NodeID:           "n0",
		Peers:            []NodeID{"n0"},
		Storage:          st,
		Transport:        nopTransport{},
		StateMachine:     sm,
		Clock:            clk,
		Seed:             1,
		SnapshotInterval: 1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = n.Stop() })
	advanceUntil(t, clk, testTick, func() bool { return n.Status().Role == Leader })

	proposeAndWait(t, n, clk, "a")
	proposeAndWait(t, n, clk, "b")

	// After applying index 2, the periodic checkpoint must have advanced the
	// durable snapshot to 2 (interval 1 => checkpoint every apply).
	advanceUntil(t, clk, testTick, func() bool {
		snap, _ := st.LoadSnapshot()
		return snap.Index == 2
	})
}
