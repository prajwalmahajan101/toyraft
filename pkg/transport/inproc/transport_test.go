package inproc_test

import (
	"context"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	"github.com/prajwalmahajan101/toyraft/pkg/transport/inproc"
	"github.com/prajwalmahajan101/toyraft/pkg/transport/transporttest"
)

// compile-time assertion: Hub.Transport's return type is raft.Transport. This
// is checked at compile time via the method-value type, without CONSTRUCTING a
// Hub (which would leak its dispatcher into the goleak baseline).
var _ func(raft.NodeID) raft.Transport = (*inproc.Hub)(nil).Transport

// newInprocPair builds a LOSSLESS Hub (no DropRate/Delay/Reorder/Duplicate) on
// the real clock and returns two connected raft.Transports A and B, with cleanup
// closing both wrappers then the Hub. Real clock is used because the conformance
// suite waits on bounded channel receives (real wall-clock ceilings), not on
// logical timer advances — a lossless real-clock Hub delivers essentially
// immediately.
func newInprocPair(t *testing.T) func() (raft.Transport, raft.Transport, func()) {
	t.Helper()
	return func() (raft.Transport, raft.Transport, func()) {
		hub, err := inproc.NewHub(inproc.HubConfig{Clock: clock.NewReal()})
		if err != nil {
			t.Fatalf("NewHub: %v", err)
		}
		a := hub.Transport("A")
		b := hub.Transport("B")
		cleanup := func() {
			_ = a.Close()
			_ = b.Close()
			_ = hub.Close()
		}
		return a, b, cleanup
	}
}

// TestInprocTransport runs the full shared behavioural suite (C1-C9) against the
// inproc Hub — the inproc half of SC1.
func TestInprocTransport(t *testing.T) {
	transporttest.RunConformance(t, newInprocPair(t))
}

// TestConnectClose is the explicit SC6 assertion: closing ONE node's transport
// disconnects ONLY that node. After A.Close(), A no longer delivers inbound, but
// B's transport keeps delivering messages from a third node — the Hub and every
// other endpoint are unaffected (pump-stop semantic, ADR-0014).
func TestConnectClose(t *testing.T) {
	hub, err := inproc.NewHub(inproc.HubConfig{Clock: clock.NewReal()})
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	defer func() { _ = hub.Close() }()

	a := hub.Transport("A")
	b := hub.Transport("B")
	c := hub.Transport("C")
	// a is Closed mid-test (the SC6 subject); b and c are joined here so their
	// pumps are reaped before goleak fires. a.Close is idempotent (C4).
	defer func() { _ = a.Close(); _ = b.Close(); _ = c.Close() }()

	rxA := make(chan raft.Message, 16)
	rxB := make(chan raft.Message, 16)
	a.Register(func(_ context.Context, m raft.Message) error { rxA <- m; return nil })
	b.Register(func(_ context.Context, m raft.Message) error { rxB <- m; return nil })

	// Baseline: both A and B receive from C before any Close.
	mustSend(t, c, raft.Message{Type: raft.MsgAppendEntries, From: "C", To: "A"})
	mustSend(t, c, raft.Message{Type: raft.MsgAppendEntries, From: "C", To: "B"})
	receiveWithin(t, rxA, time.Second)
	receiveWithin(t, rxB, time.Second)

	// Close ONLY A's transport.
	if err := a.Close(); err != nil {
		t.Fatalf("a.Close: %v", err)
	}

	// A no longer delivers inbound.
	mustSend(t, c, raft.Message{Type: raft.MsgAppendEntries, From: "C", To: "A"})
	if m, ok := tryReceive(rxA, 200*time.Millisecond); ok {
		t.Fatalf("A delivered %+v after its transport was closed", m)
	}

	// B is UNAFFECTED — the Hub and B's pump keep running.
	mustSend(t, c, raft.Message{Type: raft.MsgAppendEntries, From: "C", To: "B"})
	receiveWithin(t, rxB, time.Second)
}

func mustSend(t *testing.T, tr raft.Transport, msg raft.Message) {
	t.Helper()
	if err := tr.Send(context.Background(), msg); err != nil {
		t.Fatalf("Send: %v", err)
	}
}

func receiveWithin(t *testing.T, ch <-chan raft.Message, d time.Duration) raft.Message {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(d):
		t.Fatalf("timed out after %s waiting for delivery", d)
		return raft.Message{}
	}
}

func tryReceive(ch <-chan raft.Message, d time.Duration) (raft.Message, bool) {
	select {
	case m := <-ch:
		return m, true
	case <-time.After(d):
		return raft.Message{}, false
	}
}
