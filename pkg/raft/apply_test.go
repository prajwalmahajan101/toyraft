package raft

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// apply_test.go covers SC4 (Apply panic recovered + stack logged + fatal-status
// + applier keeps draining) and API-05 (bounded channel + slow Apply does not
// storm). It shares the single-node harness + recordingSM from driver_test.go.

// slogBuf is a thread-safe slog logger writing into a buffer so tests can assert
// on logged content (the panic stack trace, SC4).
type slogBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *slogBuf) logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&lockedWriter{s: s}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func (s *slogBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// lockedWriter serialises writes into slogBuf.buf so the slog handler is
// -race-clean across the applier and the test goroutines.
type lockedWriter struct{ s *slogBuf }

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	return w.s.buf.Write(p)
}

// panicSM panics on the entry whose Index == panicAt and otherwise records +
// returns (entry.Data, nil), so the applier's panic-recovery + keep-draining
// behaviour (SC4) is exercised against a known index.
type panicSM struct {
	panicAt Index
	mu      sync.Mutex
	applied []Entry
}

func (s *panicSM) Apply(e Entry) (any, error) {
	if e.Index == s.panicAt {
		panic("boom in Apply")
	}
	s.mu.Lock()
	s.applied = append(s.applied, e)
	s.mu.Unlock()
	return e.Data, nil
}
func (s *panicSM) Snapshot() ([]byte, Index, error) { return nil, 0, ErrSnapshotUnsupported }
func (s *panicSM) Restore([]byte) error             { return ErrSnapshotUnsupported }

func (s *panicSM) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.applied)
}

// TestApplyPanic (SC4 / API-06) — a consumer panic in Apply is recovered: the
// process does NOT die, the stack is logged via slog, the node flips to
// fatal-status, ApplyIndex advances PAST the poisoned index (so the node does
// not re-block forever), and the applier KEEPS DRAINING (a second entry still
// applies).
func TestApplyPanic(t *testing.T) {
	t.Parallel()
	logbuf := &slogBuf{}
	sm := &panicSM{panicAt: 1} // panic on the FIRST committed entry (index 1)
	n, clk := newSingleNode(t, sm, logbuf)
	advanceUntil(t, clk, testTick, func() bool { return n.Status().Role == Leader })

	// First proposal: its applied entry (index 1) panics. Propose unblocks with
	// the apply error (the applier signals the waiter on the panic path).
	done := make(chan error, 1)
	go func() {
		_, _, e := n.Propose(context.Background(), []byte("first"))
		done <- e
	}()
	deadline := time.After(5 * time.Second)
	var firstErr error
	for {
		select {
		case firstErr = <-done:
			goto firstDone
		case <-deadline:
			t.Fatal("first Propose did not return")
		default:
			clk.Advance(testTick)
			time.Sleep(time.Millisecond)
		}
	}
firstDone:
	if firstErr == nil {
		t.Fatal("first Propose: expected apply-panic error, got nil")
	}

	// (a) process alive — we are still running.
	// (b) fatal-status set, surfaced off-interface (frozen Node iface unchanged).
	fr, ok := n.(fatalReporter)
	if !ok {
		t.Fatal("Node does not expose fatalReporter")
	}
	if fr.Fatal() == nil {
		t.Fatal("fatal-status not set after Apply panic")
	}
	// (c) the stack was logged via slog.
	logs := logbuf.String()
	if !strings.Contains(logs, "panicked") {
		t.Fatalf("log did not contain 'panicked': %q", logs)
	}
	if !strings.Contains(logs, "applyOne") && !strings.Contains(logs, "Apply") {
		t.Fatalf("log did not contain a stack frame: %q", logs)
	}
	// ApplyIndex advanced PAST the panicked index (>=1) — the entry is committed,
	// so the applier marked it consumed on the panic path.
	if got := n.Status().ApplyIndex; got < 1 {
		t.Fatalf("ApplyIndex = %d after panic, want >= 1 (panic path must advance appliedIdx)", got)
	}

	// (d) the applier KEEPS DRAINING: a SECOND entry (index 2, no panic) applies.
	done2 := make(chan error, 1)
	go func() {
		_, _, e := n.Propose(context.Background(), []byte("second"))
		done2 <- e
	}()
	deadline = time.After(5 * time.Second)
	for {
		select {
		case e := <-done2:
			if e != nil {
				t.Fatalf("second Propose: %v", e)
			}
			goto secondDone
		case <-deadline:
			t.Fatal("second Propose did not return — applier stopped draining after panic")
		default:
			clk.Advance(testTick)
			time.Sleep(time.Millisecond)
		}
	}
secondDone:
	if sm.count() < 1 {
		t.Fatal("second entry was not applied — loop did not continue")
	}
	if got := n.Status().ApplyIndex; got < 2 {
		t.Fatalf("ApplyIndex = %d after second entry, want >= 2", got)
	}
}

// TestApplyChannelBounded (API-05 / Pitfall 1) — the apply channel has a finite
// cap, and a deliberately slow Apply does not stall the tick loop into an
// election-storm: with a slow recordingSM, the leader keeps its role across many
// ticks (heartbeats kept flowing — the slow applier never blocks runTicker).
func TestApplyChannelBounded(t *testing.T) {
	t.Parallel()
	sm := &recordingSM{applyDelay: 5 * time.Millisecond}
	n, clk := newSingleNode(t, sm, nil)
	advanceUntil(t, clk, testTick, func() bool { return n.Status().Role == Leader })

	// Internal accessor: the channel is bounded (cap > 0), never unbounded.
	impl := n.(*nodeImpl)
	if cap(impl.applyCh) <= 0 {
		t.Fatalf("applyCh cap = %d, want > 0 (API-05: bounded, never unbounded)", cap(impl.applyCh))
	}

	// Drive many ticks; the slow applier must not stall the tick loop into a
	// re-election. A single-node leader has nobody to lose quorum to, but a
	// blocked tick loop would stop driving MsgTick and the node could still
	// thrash; assert Role stays Leader throughout.
	for i := 0; i < 50; i++ {
		clk.Advance(testTick)
		time.Sleep(time.Millisecond)
		if r := n.Status().Role; r != Leader {
			t.Fatalf("tick %d: role = %v, want Leader (slow Apply stalled the tick loop)", i, r)
		}
	}
}
