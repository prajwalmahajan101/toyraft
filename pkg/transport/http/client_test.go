package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// signalClock wraps a clock.Clock and pushes to `after` on every After call.
// Send registers its backoff timer via cfg.Clock.After, so the test goroutine
// can wait on `after` to learn — race-free — that Send has parked on the sleep
// and it is now safe to Advance the underlying Fake. Without this the test
// would have to poll/sleep, reintroducing the nondeterminism the fake clock
// exists to remove.
type signalClock struct {
	clock.Clock
	after chan struct{}
}

func (s *signalClock) After(d time.Duration) <-chan time.Time {
	ch := s.Clock.After(d)
	s.after <- struct{}{}
	return ch
}

// flakyHandler fails the first `failN` requests with the given status, then
// returns 204 for every subsequent request. It records the total hit count.
type flakyHandler struct {
	failN      int32
	failStatus int
	hits       atomic.Int32
}

func (h *flakyHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	n := h.hits.Add(1)
	if n <= h.failN {
		w.WriteHeader(h.failStatus)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// newTestClient builds a client whose PeerURLs maps peer "B" to serverURL and
// whose backoff/timeout are supplied. The returned signalClock lets the caller
// gate each Advance on Send having parked in backoff.
func newTestClient(t *testing.T, serverURL string, backoff BackoffConfig, sendTimeout time.Duration) (*client, *clock.Fake, *signalClock) {
	t.Helper()
	fake := clock.NewFake()
	sc := &signalClock{Clock: fake, after: make(chan struct{}, 16)}
	cfg := Config{
		NodeID:      "A",
		PeerURLs:    map[raft.NodeID]string{"B": serverURL},
		Clock:       sc,
		SendTimeout: sendTimeout,
		Backoff:     backoff,
	}
	return newClient(cfg), fake, sc
}

func testMsg() raft.Message {
	return raft.Message{Type: raft.MsgAppendEntries, Term: 1, From: "A", To: "B"}
}

// TestFlakyServer proves SC3: a server that 503s the first N requests then
// succeeds makes Send return nil within the attempt budget, driven entirely by
// a fake clock (no real sleeps). Exactly N+1 requests reach the server.
func TestFlakyServer(t *testing.T) {
	const failN = 3
	h := &flakyHandler{failN: failN, failStatus: http.StatusServiceUnavailable}
	srv := httptest.NewServer(h)
	defer srv.Close()

	c, fake, sc := newTestClient(t, srv.URL,
		BackoffConfig{Base: 10 * time.Millisecond, Factor: 2, MaxAttempts: failN + 2},
		time.Hour, // large enough not to bound anything here
	)

	done := make(chan error, 1)
	go func() { done <- c.Send(context.Background(), testMsg()) }()

	// Release each of the failN backoff sleeps. Send calls After exactly once
	// per retry (failN retries follow failN failures before the success).
	for i := 0; i < failN; i++ {
		<-sc.after // Send has parked on this retry's backoff sleep
		fake.Advance(time.Hour)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Send returned %v; want nil after flaky window", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not return within 2s wall-clock (deadlock?)")
	}

	if got := h.hits.Load(); got != failN+1 {
		t.Fatalf("server received %d requests; want %d (failN+1)", got, failN+1)
	}
}

// TestBackoff covers the three bounding conditions of the SC3 retry loop.
func TestBackoff(t *testing.T) {
	// (a) Failures exceeding MaxAttempts -> Send returns a non-nil wrapped
	// error after exactly MaxAttempts requests.
	t.Run("over_budget_returns_error", func(t *testing.T) {
		const maxAttempts = 3
		h := &flakyHandler{failN: 1000, failStatus: http.StatusServiceUnavailable}
		srv := httptest.NewServer(h)
		defer srv.Close()

		c, fake, sc := newTestClient(t, srv.URL,
			BackoffConfig{Base: 10 * time.Millisecond, Factor: 2, MaxAttempts: maxAttempts},
			time.Hour,
		)

		done := make(chan error, 1)
		go func() { done <- c.Send(context.Background(), testMsg()) }()

		// maxAttempts attempts -> maxAttempts-1 backoff sleeps between them.
		for i := 0; i < maxAttempts-1; i++ {
			<-sc.after
			fake.Advance(time.Hour)
		}

		select {
		case err := <-done:
			if err == nil {
				t.Fatal("Send returned nil; want wrapped error after exhausting attempts")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Send did not return within 2s (deadlock?)")
		}

		if got := h.hits.Load(); got != maxAttempts {
			t.Fatalf("server received %d requests; want %d (MaxAttempts)", got, maxAttempts)
		}
	})

	// (b) ctx cancelled mid-backoff -> Send returns ctx.Err() promptly (it
	// unblocks via ctx.Done() without needing the sleep to fire).
	t.Run("ctx_cancel_unblocks", func(t *testing.T) {
		h := &flakyHandler{failN: 1000, failStatus: http.StatusServiceUnavailable}
		srv := httptest.NewServer(h)
		defer srv.Close()

		c, _, sc := newTestClient(t, srv.URL,
			BackoffConfig{Base: 10 * time.Millisecond, Factor: 2, MaxAttempts: 10},
			time.Hour,
		)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- c.Send(ctx, testMsg()) }()

		<-sc.after // Send has parked on the first backoff sleep
		cancel()   // cancel WITHOUT advancing the clock

		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Send returned %v; want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Send did not unblock on ctx cancel within 2s")
		}
	})

	// (c) A non-transient 4xx response is NOT retried: the server is hit once.
	t.Run("non_transient_not_retried", func(t *testing.T) {
		h := &flakyHandler{failN: 1000, failStatus: http.StatusBadRequest}
		srv := httptest.NewServer(h)
		defer srv.Close()

		c, _, _ := newTestClient(t, srv.URL,
			BackoffConfig{Base: 10 * time.Millisecond, Factor: 2, MaxAttempts: 5},
			time.Hour,
		)

		err := c.Send(context.Background(), testMsg())
		if err == nil {
			t.Fatal("Send returned nil; want wrapped error for 4xx")
		}
		if got := h.hits.Load(); got != 1 {
			t.Fatalf("server received %d requests; want 1 (4xx not retried)", got)
		}
	})

	// (d) An unknown peer is a wrapped error, no request attempted.
	t.Run("unknown_peer_wrapped", func(t *testing.T) {
		c, _, _ := newTestClient(t, "http://127.0.0.1:0",
			BackoffConfig{Base: time.Millisecond, Factor: 2, MaxAttempts: 3},
			time.Hour,
		)
		msg := testMsg()
		msg.To = "Z" // not in PeerURLs
		if err := c.Send(context.Background(), msg); err == nil {
			t.Fatal("Send to unknown peer returned nil; want wrapped error")
		}
	})
}
