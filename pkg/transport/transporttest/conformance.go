package transporttest

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// receiveTimeout bounds every wait-for-delivery in the suite. It is a real
// wall-clock ceiling on a lossless local transport (channel hop or loopback
// HTTP), NOT a logical timer — the suite never advances a fake clock, it only
// asserts that a delivery either arrives promptly or the test fails. This keeps
// the suite implementation-agnostic (it must not know an impl's clock seam).
const receiveTimeout = 2 * time.Second

// RunConformance drives a transport implementation through the C1-C9 behavioural
// contract (RESEARCH §"Shared transporttest conformance suite"). newPair returns
// two connected transports — a can Send to b, b can Send to a — plus a cleanup
// that releases both and any shared bus. The suite is impl-agnostic: it imports
// only testing, context, sync, reflect, time and pkg/raft, never a concrete
// transport. It MUST pass under -race.
//
// The pair MUST be lossless (no drop/delay/reorder): loss-tolerance is a
// core-Raft property, not a transport-conformance property.
func RunConformance(t *testing.T, newPair func() (a, b raft.Transport, cleanup func())) {
	t.Helper()

	// C1: a message Sent after B registers its step is delivered to that step,
	// equal to what was sent.
	t.Run("C1_RegisterBeforeSendDelivery", func(t *testing.T) {
		a, b, cleanup := newPair()
		defer cleanup()

		rx := newReceiver()
		b.Register(rx.step)

		msg := raft.Message{Type: raft.MsgAppendEntries, Term: 3, From: "A", To: "B"}
		if err := a.Send(context.Background(), msg); err != nil {
			t.Fatalf("Send returned error: %v", err)
		}

		got := rx.receiveWithin(t, receiveTimeout)
		if !reflect.DeepEqual(got, msg) {
			t.Fatalf("step observed %+v, want %+v", got, msg)
		}
	})

	// C2: a fully-populated Message (every field, Entries with non-empty Data,
	// fast-rollback hints) survives the transport unchanged.
	t.Run("C2_RoundTripFidelity", func(t *testing.T) {
		a, b, cleanup := newPair()
		defer cleanup()

		rx := newReceiver()
		b.Register(rx.step)

		msg := raft.Message{
			Type:          raft.MsgAppendEntriesResp,
			Term:          7,
			From:          "A",
			To:            "B",
			LastLogIndex:  11,
			LastLogTerm:   6,
			VoteGranted:   true,
			PrevLogIndex:  9,
			PrevLogTerm:   5,
			LeaderCommit:  8,
			Success:       true,
			MatchIndex:    10,
			ConflictTerm:  4,
			ConflictIndex: 2,
			Entries: []raft.Entry{
				{Term: 6, Index: 10, Data: []byte("PUT foo bar")},
				{Term: 7, Index: 11, Data: []byte{0x00, 0x01, 0xff}},
			},
		}
		if err := a.Send(context.Background(), msg); err != nil {
			t.Fatalf("Send returned error: %v", err)
		}

		got := rx.receiveWithin(t, receiveTimeout)
		if !reflect.DeepEqual(got, msg) {
			t.Fatalf("round-trip mismatch:\n got  %+v\n want %+v", got, msg)
		}
	})

	// C3: Send is safe from any goroutine — N concurrent Sends produce no
	// panic/race (the suite runs under -race).
	t.Run("C3_SendSafeFromAnyGoroutine", func(t *testing.T) {
		a, b, cleanup := newPair()
		defer cleanup()

		rx := newReceiver()
		b.Register(rx.step)

		const n = 50
		var wg sync.WaitGroup
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(i int) {
				defer wg.Done()
				msg := raft.Message{Type: raft.MsgAppendEntries, Term: raft.Term(i), From: "A", To: "B"}
				_ = a.Send(context.Background(), msg)
			}(i)
		}
		wg.Wait()

		// Drain n deliveries to confirm none were lost or panicked.
		for i := 0; i < n; i++ {
			rx.receiveWithin(t, receiveTimeout)
		}
	})

	// C4: Close is idempotent — a second Close returns nil and does not panic.
	t.Run("C4_CloseIdempotent", func(t *testing.T) {
		_, b, cleanup := newPair()
		defer cleanup()

		if err := b.Close(); err != nil {
			t.Fatalf("first Close returned error: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("second Close returned non-nil: %v", err)
		}
	})

	// C5: after B.Close(), further A.Send to B no longer reach B's step (SC6
	// shape — closing a node's transport stops its inbound delivery).
	t.Run("C5_CloseStopsInboundDelivery", func(t *testing.T) {
		a, b, cleanup := newPair()
		defer cleanup()

		rx := newReceiver()
		b.Register(rx.step)

		// Sanity: delivery works before Close.
		if err := a.Send(context.Background(), raft.Message{Type: raft.MsgAppendEntries, From: "A", To: "B"}); err != nil {
			t.Fatalf("pre-Close Send error: %v", err)
		}
		rx.receiveWithin(t, receiveTimeout)

		if err := b.Close(); err != nil {
			t.Fatalf("Close error: %v", err)
		}

		// Post-Close Sends MUST NOT reach step. Send itself may error or be a
		// no-op; either is fine (asserted by C6). We only assert non-delivery.
		for i := 0; i < 5; i++ {
			_ = a.Send(context.Background(), raft.Message{Type: raft.MsgAppendEntries, Term: raft.Term(i), From: "A", To: "B"})
		}
		if got, ok := rx.tryReceive(200 * time.Millisecond); ok {
			t.Fatalf("step received %+v after Close; inbound delivery must stop", got)
		}
	})

	// C6: Send after Close returns an error OR is a no-op — it MUST NOT panic.
	t.Run("C6_SendAfterClose", func(t *testing.T) {
		a, _, cleanup := newPair()
		defer cleanup()

		if err := a.Close(); err != nil {
			t.Fatalf("Close error: %v", err)
		}
		// Assert non-panic. The error value is unconstrained by the contract.
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Send after Close panicked: %v", r)
				}
			}()
			_ = a.Send(context.Background(), raft.Message{Type: raft.MsgAppendEntries, From: "A", To: "B"})
		}()
	})

	// C7: on a lossless pair, N Sends produce exactly N step invocations.
	t.Run("C7_ExactlyPerDelivered", func(t *testing.T) {
		a, b, cleanup := newPair()
		defer cleanup()

		rx := newReceiver()
		b.Register(rx.step)

		const n = 20
		for i := 0; i < n; i++ {
			if err := a.Send(context.Background(), raft.Message{Type: raft.MsgAppendEntries, Term: raft.Term(i), From: "A", To: "B"}); err != nil {
				t.Fatalf("Send %d error: %v", i, err)
			}
		}
		for i := 0; i < n; i++ {
			rx.receiveWithin(t, receiveTimeout)
		}
		// No surplus deliveries.
		if got, ok := rx.tryReceive(200 * time.Millisecond); ok {
			t.Fatalf("received surplus delivery %+v; want exactly %d", got, n)
		}
	})

	// C8: Register(nil) is a safe no-op (matches cluster.go:67 — the pull
	// adapter tolerates a nil step). It must not panic, and a later real
	// registration must still work if the impl supports re-registration; at
	// minimum, Register(nil) alone must not crash the transport.
	t.Run("C8_RegisterNilTolerated", func(t *testing.T) {
		_, b, cleanup := newPair()
		defer cleanup()

		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Register(nil) panicked: %v", r)
				}
			}()
			b.Register(nil)
		}()
	})

	// C9: the transport does not mutate the caller's Message/Entries after
	// Send. We keep an independent deep copy and assert the caller's value is
	// byte-identical after Send + delivery.
	t.Run("C9_CallerMessageImmutable", func(t *testing.T) {
		a, b, cleanup := newPair()
		defer cleanup()

		rx := newReceiver()
		b.Register(rx.step)

		entries := []raft.Entry{{Term: 6, Index: 10, Data: []byte("payload")}}
		msg := raft.Message{Type: raft.MsgAppendEntries, Term: 6, From: "A", To: "B", Entries: entries}
		want := cloneMessage(msg)

		if err := a.Send(context.Background(), msg); err != nil {
			t.Fatalf("Send error: %v", err)
		}
		rx.receiveWithin(t, receiveTimeout)

		if !reflect.DeepEqual(msg, want) {
			t.Fatalf("Send mutated caller's Message:\n got  %+v\n want %+v", msg, want)
		}
		// Deep-check the Entries backing array specifically.
		if len(entries) > 0 && string(entries[0].Data) != "payload" {
			t.Fatalf("Send mutated caller's Entries Data: got %q", entries[0].Data)
		}
	})
}

// receiver collects step invocations over a buffered channel so the suite can
// assert deliveries with bounded waits instead of time.Sleep.
type receiver struct {
	ch chan raft.Message
}

func newReceiver() *receiver {
	return &receiver{ch: make(chan raft.Message, 256)}
}

// step is the raft.Transport inbound callback. It never blocks the pump: the
// channel is generously buffered and the suite drains it eagerly.
func (r *receiver) step(_ context.Context, msg raft.Message) error {
	r.ch <- msg
	return nil
}

// receiveWithin returns the next delivered message or fails the test on timeout.
func (r *receiver) receiveWithin(t *testing.T, d time.Duration) raft.Message {
	t.Helper()
	select {
	case msg := <-r.ch:
		return msg
	case <-time.After(d):
		t.Fatalf("timed out after %s waiting for delivery to step", d)
		return raft.Message{} // unreachable
	}
}

// tryReceive returns (msg, true) if a delivery arrives within d, else
// (zero, false). Used to assert NON-delivery (C5, C7 surplus).
func (r *receiver) tryReceive(d time.Duration) (raft.Message, bool) {
	select {
	case msg := <-r.ch:
		return msg, true
	case <-time.After(d):
		return raft.Message{}, false
	}
}

// cloneMessage returns a deep copy of msg, including a fresh Entries slice and
// per-entry Data, so a mutation of the original is detectable.
func cloneMessage(msg raft.Message) raft.Message {
	out := msg
	if msg.Entries != nil {
		out.Entries = make([]raft.Entry, len(msg.Entries))
		for i, e := range msg.Entries {
			out.Entries[i] = e
			if e.Data != nil {
				out.Entries[i].Data = append([]byte(nil), e.Data...)
			}
		}
	}
	return out
}
