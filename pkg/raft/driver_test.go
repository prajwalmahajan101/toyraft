package raft

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
)

// driver_test.go + apply_test.go cover SC3 (Propose blocks-until-applied +
// ErrNotLeader + ctx.Err), SC4 (Apply panic recovered, loop keeps draining),
// and API-05 (bounded apply channel, slow Apply does not storm).
//
// These tests drive a REAL single-node raft.Node through the public surface on
// a clock.Fake. N=1 is ODD and valid (self is its own quorum, Validate accepts
// Peers=["n0"]), so the node elects itself after one election timeout without
// needing peer transport — the cleanest harness for the apply/propose path.
//
// IMPORTANT — Storage: these tests need a Storage whose Append/Entries actually
// round-trips, because the driver's drainCommitsToApply reads committed entries
// back via Storage.Entries(i, i+1). The 07-02 in-package fakeStorage returns
// (nil, nil) for Entries — the applier would never see any entry. We CANNOT
// import pkg/storage/memory from a white-box `package raft` test (cycle:
// memory -> raft). So we provide memStorage below: a minimal in-package,
// append-backed, mutex-guarded Storage that supports the half-open [lo, hi)
// Entries read the driver relies on. (Same import-cycle workaround spirit as the
// 07-02 step_test.go doubles, but with a real append-backed log.)

// memStorage is a tiny append-backed, in-package Storage double. The log is
// 1-based: entries[i-1] holds the entry at index i. Concurrency-safe (the
// applier reads via Entries while the core appends via Append). It is the
// minimum needed for the driver's commit->apply read path.
type memStorage struct {
	mu      sync.Mutex
	entries []Entry
	hs      HardState
}

func (m *memStorage) Append(es []Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range es {
		// Contiguity: append at LastIndex()+1; truncate-then-append handled by
		// TruncateSuffix. For the single-leader propose path entries arrive in
		// strict order, so a plain append suffices.
		m.entries = append(m.entries, e)
	}
	return nil
}

func (m *memStorage) TruncateSuffix(from Index) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if int(from) <= len(m.entries) && from >= 1 {
		m.entries = m.entries[:from-1]
	}
	return nil
}

func (m *memStorage) Entries(lo, hi Index) ([]Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if lo < 1 || hi <= lo || int(lo-1) >= len(m.entries) {
		return nil, nil
	}
	end := int(hi - 1)
	if end > len(m.entries) {
		end = len(m.entries)
	}
	out := make([]Entry, end-int(lo-1))
	copy(out, m.entries[lo-1:end])
	return out, nil
}

func (m *memStorage) Term(index Index) (Term, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if index < 1 || int(index) > len(m.entries) {
		return 0, nil
	}
	return m.entries[index-1].Term, nil
}

func (m *memStorage) FirstIndex() (Index, error) { return 1, nil }

func (m *memStorage) LastIndex() (Index, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Index(len(m.entries)), nil
}

func (m *memStorage) SaveHardState(hs HardState) error {
	m.mu.Lock()
	m.hs = hs
	m.mu.Unlock()
	return nil
}

func (m *memStorage) LoadHardState() (HardState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hs, nil
}

func (m *memStorage) Snapshot() ([]byte, Index, error) { return nil, 0, ErrSnapshotUnsupported }
func (m *memStorage) Restore([]byte) error             { return ErrSnapshotUnsupported }

// recordingSM records every applied entry under a mutex so tests can assert
// apply order + apply-before-Propose-returns. An optional applyDelay simulates
// a slow consumer (API-05 backpressure test).
type recordingSM struct {
	mu         sync.Mutex
	applied    []Entry
	applyDelay time.Duration
}

func (s *recordingSM) Apply(e Entry) (any, error) {
	if s.applyDelay > 0 {
		time.Sleep(s.applyDelay)
	}
	s.mu.Lock()
	s.applied = append(s.applied, e)
	s.mu.Unlock()
	return e.Data, nil
}
func (s *recordingSM) Snapshot() ([]byte, Index, error) { return nil, 0, ErrSnapshotUnsupported }
func (s *recordingSM) Restore([]byte) error             { return ErrSnapshotUnsupported }

func (s *recordingSM) has(data string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.applied {
		if string(e.Data) == data {
			return true
		}
	}
	return false
}

// newSingleNode builds a started single-node (N=1) Node on a fresh clock.Fake
// with the given StateMachine and a real memory.Storage. It registers Stop +
// goleak-clean teardown via t.Cleanup. The returned clock drives ticks. An
// optional logger (nil = silent default) captures slog output.
func newSingleNode(t *testing.T, sm StateMachine, logger *slogBuf) (Node, *clock.Fake) {
	t.Helper()
	clk := clock.NewFake()
	cfg := Config{
		NodeID:       "n0",
		Peers:        []NodeID{"n0"}, // N=1 odd, self-quorum: elects itself
		Storage:      &memStorage{},
		Transport:    nopTransport{},
		StateMachine: sm,
		Clock:        clk,
		Seed:         1,
	}
	if logger != nil {
		cfg.Logger = logger.logger()
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = n.Stop() })
	return n, clk
}

// advanceUntil advances the fake clock one tick at a time (capped) until cond
// holds, giving the runTicker goroutine a brief real-time window after each
// synchronous tick send to finish Step/Ready/Send/drain processing. It fails
// the test if cond never holds within the cap.
//
// The clock.Fake tick send is synchronous (it blocks until runTicker drains the
// channel), but the post-drain processing is async; the short sleep lets it
// settle before we re-check cond. This is a scheduling hint, not a determinism
// boundary — cond is monotone (role/index only advance), so polling is safe.
func advanceUntil(t *testing.T, clk *clock.Fake, tick time.Duration, cond func() bool) {
	t.Helper()
	for i := 0; i < 400; i++ {
		if cond() {
			return
		}
		clk.Advance(tick)
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatalf("advanceUntil: condition never held after 400 ticks")
	}
}

// testTick is one tick for the default config (HeartbeatInterval default 50ms).
const testTick = 50 * time.Millisecond

// TestProposeBlocksUntilApplied (SC3 / API-03 / API-07) — a single-node leader's
// Propose returns (idx, term, nil) ONLY after the entry has been applied
// (recordingSM saw it BEFORE Propose returned), and Status().ApplyIndex == idx
// afterward (proving the applier, not the driver's enqueue frontier, advances
// ApplyIndex).
func TestProposeBlocksUntilApplied(t *testing.T) {
	t.Parallel()
	sm := &recordingSM{}
	n, clk := newSingleNode(t, sm, nil)

	advanceUntil(t, clk, testTick, func() bool { return n.Status().Role == Leader })

	type result struct {
		idx  Index
		term Term
		err  error
	}
	done := make(chan result, 1)
	go func() {
		idx, term, err := n.Propose(context.Background(), []byte("x"))
		done <- result{idx, term, err}
	}()

	// Drive ticks until Propose returns. A single-node leader commits its own
	// entry immediately (self-quorum); the applier then applies it and signals.
	var res result
	deadline := time.After(5 * time.Second)
	for {
		select {
		case res = <-done:
			goto returned
		case <-deadline:
			t.Fatal("Propose did not return within 5s")
		default:
			clk.Advance(testTick)
			time.Sleep(time.Millisecond)
		}
	}
returned:
	if res.err != nil {
		t.Fatalf("Propose: unexpected error %v", res.err)
	}
	if res.idx < 1 {
		t.Fatalf("Propose: idx = %d, want >= 1", res.idx)
	}
	if res.term < 1 {
		t.Fatalf("Propose: term = %d, want >= 1", res.term)
	}
	// Apply-then-return: the SM must have observed the entry BEFORE Propose
	// returned (Propose returning is the happens-after of applyOne's signal).
	if !sm.has("x") {
		t.Fatal("recordingSM did not observe entry before Propose returned (commit-then-return, not apply-then-return)")
	}
	// ApplyIndex (read from appliedIdx — the applier-owned atomic) must equal idx.
	if got := n.Status().ApplyIndex; got != res.idx {
		t.Fatalf("Status().ApplyIndex = %d, want %d (applier advances appliedIdx, not the enqueue frontier)", got, res.idx)
	}
}

// TestProposeNotLeader (SC3 / API-04) — a node that has NOT been driven to
// leadership (no ticks advanced) is a Follower; Propose returns *ErrNotLeader.
func TestProposeNotLeader(t *testing.T) {
	t.Parallel()
	sm := &recordingSM{}
	// Build WITHOUT advancing the clock: the node stays Follower.
	n, _ := newSingleNode(t, sm, nil)

	if n.Status().Role != Follower {
		t.Fatalf("precondition: role = %v, want Follower", n.Status().Role)
	}
	_, _, err := n.Propose(context.Background(), []byte("x"))
	var nl *ErrNotLeader
	if !errors.As(err, &nl) {
		t.Fatalf("Propose on follower: err = %v, want *ErrNotLeader", err)
	}
}

// TestProposeCtxCancel (SC3 / API-09) — a leader Propose whose ctx is cancelled
// before the entry can apply returns ctx.Err() and does NOT retain the waiter
// (a second Propose proceeds with no stale signal / no panic).
func TestProposeCtxCancel(t *testing.T) {
	t.Parallel()
	// Slow apply so the entry stays in-flight while we cancel ctx.
	sm := &recordingSM{applyDelay: 50 * time.Millisecond}
	n, clk := newSingleNode(t, sm, nil)
	advanceUntil(t, clk, testTick, func() bool { return n.Status().Role == Leader })

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel BEFORE proposing so the select takes the ctx.Done branch
	_, _, err := n.Propose(ctx, []byte("y"))
	if err != context.Canceled {
		t.Fatalf("Propose(cancelled ctx): err = %v, want context.Canceled", err)
	}

	// A subsequent Propose must still work (waiter for the cancelled idx was
	// deleted — Pitfall 5): drive it to applied with a healthy ctx.
	done := make(chan error, 1)
	go func() {
		_, _, e := n.Propose(context.Background(), []byte("z"))
		done <- e
	}()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case e := <-done:
			if e != nil {
				t.Fatalf("second Propose: %v", e)
			}
			return
		case <-deadline:
			t.Fatal("second Propose did not return")
		default:
			clk.Advance(testTick)
			time.Sleep(time.Millisecond)
		}
	}
}
