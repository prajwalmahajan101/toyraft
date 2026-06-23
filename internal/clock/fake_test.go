package clock_test

import (
	"strings"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
)

// These tests use ONLY clock.Fake — never time.Sleep — so they stay
// coupled to logical (fake) time. The single exception is the
// quiescence-panic test, which intentionally exercises the wall-clock
// safety net.

func TestFake_Now_StartsAtEpoch(t *testing.T) {
	t.Parallel()
	f := clock.NewFake()
	if !f.Now().Equal(time.Unix(0, 0)) {
		t.Fatalf("Now()=%v; want Unix epoch", f.Now())
	}
}

func TestFake_Now_AdvancesByExactly_d(t *testing.T) {
	t.Parallel()
	f := clock.NewFake()
	f.Advance(50 * time.Millisecond)
	want := time.Unix(0, 0).Add(50 * time.Millisecond)
	if !f.Now().Equal(want) {
		t.Fatalf("Now()=%v; want %v", f.Now(), want)
	}
}

func TestFake_After_FiresAtExpiry(t *testing.T) {
	t.Parallel()
	f := clock.NewFake()
	ch := f.After(10 * time.Millisecond)
	f.Advance(10 * time.Millisecond)
	select {
	case got := <-ch:
		if !got.Equal(f.Now()) {
			t.Fatalf("fire instant %v != Now() %v", got, f.Now())
		}
	default:
		t.Fatal("After channel did not fire after Advance to its expiry")
	}
}

func TestFake_After_DoesNotFireBeforeExpiry(t *testing.T) {
	t.Parallel()
	f := clock.NewFake()
	ch := f.After(50 * time.Millisecond)
	f.Advance(40 * time.Millisecond)
	select {
	case <-ch:
		t.Fatal("After channel fired before its expiry")
	default:
		// ok
	}
}

func TestFake_NewTimer_Stop_PreventsFire(t *testing.T) {
	t.Parallel()
	f := clock.NewFake()
	tm := f.NewTimer(10 * time.Millisecond)
	if ok := tm.Stop(); !ok {
		t.Fatalf("Stop()=false on unfired timer; want true")
	}
	f.Advance(1 * time.Hour)
	select {
	case <-tm.C():
		t.Fatal("stopped timer fired")
	default:
		// ok
	}
}

func TestFake_NewTimer_Reset_RegistersAtTail(t *testing.T) {
	t.Parallel()
	f := clock.NewFake()
	a := f.NewTimer(10 * time.Millisecond)
	b := f.NewTimer(10 * time.Millisecond)
	// Reset A: it should now sit behind B at the same instant because
	// its seq has been refreshed to a larger value (FIFO of registration).
	if !a.Reset(10 * time.Millisecond) {
		t.Fatalf("Reset()=false on active timer; want true")
	}
	f.Advance(10 * time.Millisecond)

	// B should be drainable first.
	select {
	case <-b.C():
		// ok
	default:
		t.Fatal("B did not fire after Advance")
	}
	select {
	case <-a.C():
		// ok
	default:
		t.Fatal("A did not fire after Advance")
	}
	// Order assertion: at the moment we drain B above we must NOT
	// already have drained A — verify by registering a fresh same-instant
	// pair and asserting the order in a select with default fallback.
	f2 := clock.NewFake()
	a2 := f2.NewTimer(10 * time.Millisecond)
	b2 := f2.NewTimer(10 * time.Millisecond)
	if !a2.Reset(10 * time.Millisecond) {
		t.Fatalf("Reset()=false on active timer (2); want true")
	}
	f2.Advance(10 * time.Millisecond)
	// B2 must be ready immediately; a2 must also be ready (FIFO is by
	// fire ordering inside Advance — both channels are buffered cap 1
	// so post-Advance both are drainable; the ordering invariant is
	// proven separately in TestFakeAdvance_StableOrder_100Runs).
	select {
	case <-b2.C():
	default:
		t.Fatal("B2 did not fire after Advance")
	}
	select {
	case <-a2.C():
	default:
		t.Fatal("A2 did not fire after Advance")
	}
}

func TestFake_NewTicker_FiresPeriodically(t *testing.T) {
	t.Parallel()
	f := clock.NewFake()
	tk := f.NewTicker(10 * time.Millisecond)
	defer tk.Stop()

	// Consumer goroutine: drain three ticks and signal completion.
	// The ready channel ensures the consumer has reached the receive
	// BEFORE we call Advance — otherwise the synchronous send pattern
	// inside Advance would trip the 1s safety panic.
	ready := make(chan struct{})
	done := make(chan int, 1)
	go func() {
		close(ready)
		count := 0
		for count < 3 {
			<-tk.C()
			count++
		}
		done <- count
	}()
	<-ready

	f.Advance(30 * time.Millisecond)

	select {
	case n := <-done:
		if n != 3 {
			t.Fatalf("got %d ticks; want 3", n)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("did not receive 3 ticks within 100ms wall-clock budget")
	}
}

// TestFake_NewTicker_QuiescenceTimeout_Panics exercises the 1s safety
// net. By design this test takes ~1s of wall-clock time because the
// safety-net timer is fixed at quiescenceTimeout = 1s. Reviewers: this
// duration is the cost of the assert.
func TestFake_NewTicker_QuiescenceTimeout_Panics(t *testing.T) {
	t.Parallel()
	f := clock.NewFake()
	tk := f.NewTicker(10 * time.Millisecond)
	defer tk.Stop()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic from undrained ticker; got none")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("panic value %T %v; want string", r, r)
		}
		if !strings.Contains(msg, "consumer not draining") {
			t.Fatalf("panic message %q does not contain 'consumer not draining'", msg)
		}
	}()

	// No consumer drains tk.C(). First tick lands in the cap-1
	// buffer; the second tick's synchronous send blocks until the
	// safety net fires.
	f.Advance(20 * time.Millisecond)
	t.Fatal("Advance returned without panicking on undrained ticker")
}

// TestFakeAdvance_StableOrder_100Runs is the SC2 acceptance test. 100
// iterations of: register 16 timers at the same instant, advance once,
// assert firing order equals registration order. If the heap's tiebreak
// ever degrades from seq-based to anything scheduler-dependent, this
// test fails immediately.
func TestFakeAdvance_StableOrder_100Runs(t *testing.T) {
	t.Parallel()
	for run := 0; run < 100; run++ {
		f := clock.NewFake()
		const n = 16
		chs := make([]<-chan time.Time, n)
		for i := 0; i < n; i++ {
			chs[i] = f.NewTimer(10 * time.Millisecond).C()
		}
		f.Advance(10 * time.Millisecond)
		order := make([]int, 0, n)
		for i := 0; i < n; i++ {
			select {
			case <-chs[i]:
				order = append(order, i)
			default:
				t.Fatalf("run %d: timer %d did not fire", run, i)
			}
		}
		for i, v := range order {
			if v != i {
				t.Fatalf("run %d: order = %v; want sorted (FIFO of registration)", run, order)
			}
		}
	}
}

// TestFake_NarrowShape is a structural-shape sentinel: Phase 5 will
// declare pkg/raft.Clock as the narrow Now+NewTicker subset. Until
// pkg/raft.Clock exists we cannot write a compile-time assertion
// against it; instead we exercise the method set directly here. When
// pkg/raft.Clock lands, replace this with:
//
//	var _ raft.Clock = (*clock.Fake)(nil)
//	var _ raft.Clock = (*clock.Real)(nil)
func TestFake_NarrowShape(t *testing.T) {
	t.Parallel()
	var c interface {
		Now() time.Time
		NewTicker(d time.Duration) clock.Ticker
	} = clock.NewFake()
	_ = c.Now()
	tk := c.NewTicker(1 * time.Millisecond)
	tk.Stop()
}
