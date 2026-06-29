package raft

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// The shared in-test doubles (fakeStorage, nopTransport, nopSM) live in
// step_test.go (declared ONCE per package). This file reuses them rather than
// redeclaring. fakeStorage is used instead of pkg/storage/memory because a
// white-box `package raft` _test file CANNOT import pkg/storage/memory without
// the import cycle pkg/storage/memory -> pkg/storage -> pkg/raft (see the
// fakeStorage doc in step_test.go).

// newPublicConfig returns a valid odd-N Config for the public-surface tests.
// Seed is pinned so the per-node RNG draw is deterministic (P1-4).
func newPublicConfig() Config {
	return Config{
		NodeID:       "n0",
		Peers:        []NodeID{"n0", "n1", "n2"},
		Storage:      fakeStorage{},
		Transport:    nopTransport{},
		StateMachine: nopSM{},
		Seed:         1,
	}
}

// TestNewValidationSurface proves New() runs applyDefaults+Validate and SURFACES
// the Config.Validate error rather than panicking (SC1/API-01). The exhaustive
// validation matrix (even-N, nil-Transport, nil-StateMachine, missing-self, bad
// timings, nil-Storage) lives in TestConfigValidate (step_test.go) and is NOT
// duplicated here — this only confirms the New() WRAPPER path.
func TestNewValidationSurface(t *testing.T) {
	t.Parallel()

	// (a) An even-N config is rejected: New returns a nil Node + an error
	// wrapping ErrInvalidConfig.
	even := newPublicConfig()
	even.Peers = []NodeID{"n0", "n1"} // even N — split-quorum reject (R-1)
	n, err := New(even)
	if n != nil {
		t.Fatalf("New(even-N): expected nil Node, got %v", n)
	}
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New(even-N): expected error wrapping ErrInvalidConfig, got %v", err)
	}

	// (b) A valid odd-N config constructs a non-nil Node + nil error.
	ok, err := New(newPublicConfig())
	if err != nil {
		t.Fatalf("New(valid): unexpected error %v", err)
	}
	if ok == nil {
		t.Fatal("New(valid): expected non-nil Node")
	}
	// Tidy up the goroutines this Node never spawned (Start not called) — Stop
	// is safe before Start.
	if err := ok.Stop(); err != nil {
		t.Fatalf("Stop before Start: %v", err)
	}
}

// TestIdempotentLifecycle hammers Start and Stop from 100 concurrent goroutines
// each (SC2/API-02). It asserts no panic and that every Stop observes the same
// (nil) error. Run under the package goleak TestMain and -race.
func TestIdempotentLifecycle(t *testing.T) {
	node, err := New(newPublicConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	const N = 100

	var startWg sync.WaitGroup
	startWg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer startWg.Done()
			if err := node.Start(context.Background()); err != nil {
				t.Errorf("Start: unexpected error %v", err)
			}
		}()
	}
	startWg.Wait()

	var stopWg sync.WaitGroup
	stopWg.Add(N)
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		go func(idx int) {
			defer stopWg.Done()
			errs[idx] = node.Stop()
		}(i)
	}
	stopWg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Errorf("Stop call %d: expected nil, got %v", i, e)
		}
	}
}

// TestStopGoroutineLeak constructs, Starts, then Stops a node and asserts zero
// residual goroutines (SC5/API-08). The package goleak.VerifyTestMain is the
// suite-wide gate; the explicit goleak.VerifyNone here is a tighter per-test
// check that the three lifecycle goroutines exit on Stop.
func TestStopGoroutineLeak(t *testing.T) {
	node, err := New(newPublicConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := node.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	goleak.VerifyNone(t)
}

// TestStopBoundedByTimeout asserts Stop returns within Config.StopTimeout even
// if a goroutine were wedged (SC5/API-08). The placeholder goroutines exit
// promptly on cancel, so Stop should return well under the (default 5s) bound;
// here we simply assert Stop returns within a generous wall budget.
func TestStopBoundedByTimeout(t *testing.T) {
	t.Parallel()

	node, err := New(newPublicConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- node.Stop() }()
	select {
	case e := <-done:
		if e != nil {
			t.Fatalf("Stop: unexpected error %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return within 2s (StopTimeout bound violated)")
	}
}

// TestSilentDefault proves the "zero output without Config.Logger" contract
// (SC7/API-10) with two complementary assertions.
func TestSilentDefault(t *testing.T) {
	// (a) IDENTITY (fast unit check): applyDefaults with Logger nil yields the
	// canonical discard handler.
	cfg := newPublicConfig()
	cfg.applyDefaults()
	if cfg.Logger == nil {
		t.Fatal("applyDefaults left Logger nil")
	}
	if cfg.Logger.Handler() != slog.DiscardHandler {
		t.Errorf("default Logger handler = %T; want slog.DiscardHandler", cfg.Logger.Handler())
	}

	// (b) BEHAVIOURAL (primary, toolchain-independent): a node built with Logger
	// UNSET must emit ZERO bytes to stderr across a full lifecycle plus the
	// guarded Step/Propose paths. Redirect os.Stderr to a pipe for the duration.
	orig := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = w
	defer func() { os.Stderr = orig }()

	captured := make(chan []byte, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		captured <- buf.Bytes()
	}()

	node, err := New(newPublicConfig()) // Logger unset -> DiscardHandler default
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Exercise the paths that WOULD log on a non-discard handler.
	_ = node.Step(context.Background(), Message{Type: MsgTick})
	_, _, _ = node.Propose(context.Background(), []byte("x"))
	if err := node.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Close the write end so the copy goroutine sees EOF, then restore + assert.
	_ = w.Close()
	os.Stderr = orig
	out := <-captured
	if len(out) != 0 {
		t.Errorf("default-logger node wrote %d bytes to stderr (want 0): %q", len(out), out)
	}
}

// TestStatusLatency asserts Status() is a non-blocking copy-under-lock snapshot
// (SC6/API-07): it must return promptly under concurrent Step churn. The robust
// guarantee is "no blocking on channels/IO", which copy-under-lock provides
// structurally; the wall-budget assertion is generous to avoid CI flakiness.
func TestStatusLatency(t *testing.T) {
	node, err := New(newPublicConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := node.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	// Background churn: hammer Step (ticks) to contend the core lock.
	stop := make(chan struct{})
	var churnWg sync.WaitGroup
	churnWg.Add(1)
	go func() {
		defer churnWg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = node.Step(context.Background(), Message{Type: MsgTick})
			}
		}
	}()

	const calls = 10000
	start := time.Now()
	for i := 0; i < calls; i++ {
		s := node.Status()
		// MatchIndex must be nil on a follower (this node never wins): a
		// cheap self-consistency check that the snapshot is well-formed.
		if s.Role != Leader && s.MatchIndex != nil {
			t.Fatalf("non-leader Status returned non-nil MatchIndex: %v", s.MatchIndex)
		}
	}
	elapsed := time.Since(start)

	close(stop)
	churnWg.Wait()

	// Generous budget: 10k copy-under-lock snapshots under contention should
	// complete far under this. The real guarantee is structural (no channel/IO
	// in Status); this only guards against an accidental blocking regression.
	if elapsed > 2*time.Second {
		t.Errorf("%d Status() calls took %s (>2s) — possible blocking regression", calls, elapsed)
	}
}
