package inproc_test

import (
	"context"
	"os"
	"regexp"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	"github.com/prajwalmahajan101/toyraft/pkg/transport/inproc"
)

// newTestHub builds a Hub on a fresh Fake clock with the given options
// applied to a base config. Tests use this to keep boilerplate down.
func newTestHub(t *testing.T, mutate func(*inproc.HubConfig)) *inproc.Hub {
	t.Helper()
	cfg := inproc.HubConfig{Clock: clock.NewFake(), Seed: 1}
	if mutate != nil {
		mutate(&cfg)
	}
	h, err := inproc.NewHub(cfg)
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	return h
}

func msg(from, to raft.NodeID, term raft.Term) raft.Message {
	return raft.Message{
		Type: raft.MsgAppendEntries,
		Term: term,
		From: from,
		To:   to,
	}
}

func TestNewHub_RequiresClock(t *testing.T) {
	if _, err := inproc.NewHub(inproc.HubConfig{}); err == nil {
		t.Fatal("NewHub with nil Clock returned nil error; want actionable error")
	}
}

// TestHub_FIFODelivery_SameSeed sends 10 messages A->B and asserts the
// receiver sees them in send order. Since deliverAt = clk.Now() with no
// chaos delay, every send is immediately due on the dispatcher's next
// drain pass; we don't need to Advance the FakeClock.
func TestHub_FIFODelivery_SameSeed(t *testing.T) {
	h := newTestHub(t, nil)
	defer h.Close()

	a := h.Connect("A")
	b := h.Connect("B")

	const n = 10
	done := make(chan []raft.Term, 1)
	go func() {
		got := make([]raft.Term, 0, n)
		for range n {
			m := <-b.Recv()
			got = append(got, m.Term)
		}
		done <- got
	}()

	for i := range raft.Term(n) {
		if err := a.Send(context.Background(), msg("A", "B", i)); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}

	select {
	case got := <-done:
		for i := range raft.Term(n) {
			if got[i] != i {
				t.Fatalf("FIFO violated at idx %d: got %d, want %d (full: %v)", i, got[i], i, got)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("receiver did not drain 10 messages within 1s wall-clock")
	}
}

// TestHub_CloseIsIdempotent fires 5 concurrent Close calls; sync.Once
// must prevent any panic and all calls must return.
func TestHub_CloseIsIdempotent(t *testing.T) {
	h := newTestHub(t, nil)

	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			if err := h.Close(); err != nil {
				t.Errorf("Close returned %v; want nil", err)
			}
		})
	}

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("concurrent Close calls did not all return within 1s")
	}
}

// TestHub_CloseUnblocksSenders is THE SC4 test: with InboundCap=1 and no
// consumer, the dispatcher parks on the inbound send. Close cancels the
// hub context; the dispatcher returns; goleak verifies no residual
// goroutines. The wall-clock budget is 100ms (RESEARCH SC4); we allow
// 150ms slack for CI noise.
func TestHub_CloseUnblocksSenders(t *testing.T) {
	h := newTestHub(t, func(c *inproc.HubConfig) { c.InboundCap = 1 })

	a := h.Connect("A")
	_ = h.Connect("B") // B never drains

	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for i := range raft.Term(5) {
			// All sends are non-blocking on the sender side — the
			// dispatcher is the one that parks. We still loop to
			// guarantee at least one message is in flight before
			// the parking ones queue behind it.
			_ = a.Send(context.Background(), msg("A", "B", i))
		}
	}()

	select {
	case <-sent:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("sender goroutine did not return within 200ms (Send must be fire-and-forget)")
	}

	// Heuristic: by 20ms the first message has reached the inbound and
	// the dispatcher is parked trying to deliver the second.
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 150*time.Millisecond {
		t.Fatalf("Close took %v, want <=150ms (SC4 budget 100ms + 50ms wall-clock slack)", elapsed)
	}
	t.Logf("SC4 measured Close latency: %v (budget 100ms, slack 50ms)", elapsed)

	goleak.VerifyNone(t)
}

// TestHub_GoleakAfterCloseIsClean exercises 3 nodes bidirectionally with
// drained receivers, then Close + per-test goleak baseline.
func TestHub_GoleakAfterCloseIsClean(t *testing.T) {
	h := newTestHub(t, nil)

	a := h.Connect("A")
	b := h.Connect("B")
	c := h.Connect("C")

	endpoints := []*inproc.Endpoint{a, b, c}
	const perPair = 4
	expected := perPair * len(endpoints) * (len(endpoints) - 1)
	done := make(chan struct{})
	go func() {
		got := 0
		for got < expected {
			select {
			case <-a.Recv():
			case <-b.Recv():
			case <-c.Recv():
			}
			got++
		}
		close(done)
	}()

	for _, from := range endpoints {
		for _, to := range endpoints {
			if from.ID() == to.ID() {
				continue
			}
			for i := range raft.Term(perPair) {
				if err := from.Send(context.Background(), msg(from.ID(), to.ID(), i)); err != nil {
					t.Fatalf("Send %s->%s #%d: %v", from.ID(), to.ID(), i, err)
				}
			}
		}
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("receivers did not drain all messages within 1s")
	}

	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	goleak.VerifyNone(t)
}

// TestHub_PeerIteration_IsSortedSlice is a structural fence: hub.go
// must reference the sortedNodes slice AND must NOT iterate over the
// nodes map in delivery paths (RESEARCH Pitfall 2 — map iteration is a
// determinism leak under one seed).
func TestHub_PeerIteration_IsSortedSlice(t *testing.T) {
	src, err := os.ReadFile("hub.go")
	if err != nil {
		t.Fatalf("read hub.go: %v", err)
	}
	body := string(src)
	if !regexp.MustCompile(`sortedNodes`).MatchString(body) {
		t.Fatal("hub.go does not reference sortedNodes; deterministic iteration order would be lost")
	}
	// A map range over h.nodes is the documented anti-pattern; if any
	// future contributor adds one we want CI to red here.
	if regexp.MustCompile(`for\s+\w+\s*(?:,\s*\w+\s*)?:=\s*range\s+h\.nodes`).MatchString(body) {
		t.Fatal("hub.go iterates h.nodes via map range; switch to walking the sortedNodes slice (RESEARCH Pitfall 2)")
	}
}
