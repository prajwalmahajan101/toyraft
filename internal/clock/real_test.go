package clock_test

import (
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
)

// These tests cover internal/clock.Real, which is itself the wall-clock
// implementation, so using real time.Sleep / time.After fallbacks here is
// appropriate — we are testing the Real wrapper, not consensus code.

func TestReal_Now_AdvancesMonotonically(t *testing.T) {
	t.Parallel()
	r := clock.NewReal()
	t1 := r.Now()
	time.Sleep(5 * time.Millisecond)
	t2 := r.Now()
	if !t2.After(t1) {
		t.Fatalf("expected t2 (%v) to be after t1 (%v)", t2, t1)
	}
}

func TestReal_After_FiresAfterDuration(t *testing.T) {
	t.Parallel()
	r := clock.NewReal()
	ch := r.After(20 * time.Millisecond)
	select {
	case <-ch:
		// ok
	case <-time.After(200 * time.Millisecond):
		t.Fatal("After channel did not fire within 200ms")
	}
}

func TestReal_NewTimer_FiresAndResets(t *testing.T) {
	t.Parallel()
	r := clock.NewReal()
	tm := r.NewTimer(20 * time.Millisecond)
	select {
	case <-tm.C():
		// ok
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timer did not fire within 200ms on initial duration")
	}

	if ok := tm.Reset(20 * time.Millisecond); ok {
		// Reset returns false when the timer has already fired or been
		// stopped, which is the path we took. Either return value is
		// legal per stdlib docs; we don't assert on it.
		_ = ok
	}
	select {
	case <-tm.C():
		// ok
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timer did not fire within 200ms after Reset")
	}
}

func TestReal_NewTimer_StopBeforeFire(t *testing.T) {
	t.Parallel()
	r := clock.NewReal()
	tm := r.NewTimer(1 * time.Second)
	if ok := tm.Stop(); !ok {
		t.Fatalf("Stop() returned false; expected true for unfired timer")
	}
	select {
	case <-tm.C():
		t.Fatal("timer fired after Stop()")
	case <-time.After(50 * time.Millisecond):
		// ok
	}
}

func TestReal_NewTicker_FiresPeriodically(t *testing.T) {
	t.Parallel()
	r := clock.NewReal()
	tk := r.NewTicker(10 * time.Millisecond)
	defer tk.Stop()

	deadline := time.After(500 * time.Millisecond)
	for i := 0; i < 3; i++ {
		select {
		case <-tk.C():
			// ok
		case <-deadline:
			t.Fatalf("missed tick %d within 500ms budget", i+1)
		}
	}

	tk.Stop()
	// Drain any tick already enqueued before Stop took effect, then
	// assert no further ticks land.
	select {
	case <-tk.C():
	default:
	}
	select {
	case <-tk.C():
		t.Fatal("ticker fired after Stop()")
	case <-time.After(50 * time.Millisecond):
		// ok
	}
}
