package clock

import (
	"container/heap"
	"sync"
	"time"
)

// quiescenceTimeout is the load-bearing safety net for FakeClock channel
// sends inside Advance. If a consumer fails to drain its tick channel
// within this window, Advance panics with an actionable message rather
// than hanging the test indefinitely. See ADR-0006.
const quiescenceTimeout = 1 * time.Second

// FakeClock is the Clock variant that also exposes Advance for tests.
//
// Implementations are expected to be deterministic: same sequence of
// register/Advance/Stop calls produces the same firing order across
// runs and goroutine schedules. The contract is documented in ADR-0006.
type FakeClock interface {
	Clock
	Advance(d time.Duration)
}

// Fake is the deterministic test clock. Allocate via NewFake. The zero
// value is NOT usable — use NewFake so the heap and now-anchor are
// initialised consistently.
//
// Fake.Advance(d) is synchronous: it fires every timer due by now+d in
// registration order (FIFO of seq) and waits for each consumer to drain
// the channel send before firing the next. Tickers re-register at the
// heap tail with a fresh seq after each fire. See ADR-0006 for the
// model rationale (synchronous quiescence + 1s panic-on-stuck instead
// of scheduler-yield-based probabilistic waits).
type Fake struct {
	mu     sync.Mutex
	now    time.Time
	seq    uint64
	timers fakeTimerHeap
}

// NewFake returns a Fake clock anchored at the Unix epoch. Tests
// typically rely only on relative time, so the absolute starting
// instant rarely matters; epoch makes the recorded HistoryEvent
// timestamps small and human-readable.
func NewFake() *Fake {
	return &Fake{now: time.Unix(0, 0)}
}

// Now returns the current fake time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	t := f.now
	f.mu.Unlock()
	return t
}

// After returns a channel that fires once after d of fake time has
// elapsed (via Advance). Equivalent to NewTimer(d).C().
func (f *Fake) After(d time.Duration) <-chan time.Time {
	return f.NewTimer(d).C()
}

// NewTimer registers a one-shot timer that fires d into the fake
// future.
func (f *Fake) NewTimer(d time.Duration) Timer {
	t := &fakeTimer{
		ch:     make(chan time.Time, 1),
		period: 0,
	}
	f.mu.Lock()
	t.expiry = f.now.Add(d)
	t.seq = f.nextSeqLocked()
	heap.Push(&f.timers, t)
	f.mu.Unlock()
	return &fakeTimerHandle{t: t, f: f}
}

// NewTicker registers a periodic ticker that fires every d of fake
// time. Tickers re-register at the heap tail (with a fresh seq) after
// each fire, preserving FIFO fairness across periods.
func (f *Fake) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock.Fake.NewTicker: non-positive interval")
	}
	t := &fakeTimer{
		ch:     make(chan time.Time, 1),
		period: d,
	}
	f.mu.Lock()
	t.expiry = f.now.Add(d)
	t.seq = f.nextSeqLocked()
	heap.Push(&f.timers, t)
	f.mu.Unlock()
	return &fakeTickerHandle{t: t, f: f}
}

// Advance moves the fake clock forward by d, firing every timer whose
// expiry falls within the new window. Same-instant timers fire in
// registration order (FIFO of seq). Each fire is a synchronous channel
// send: if the consumer does not drain within quiescenceTimeout (1s),
// Advance panics with a "consumer not draining" message.
//
// f.mu is released around each channel send so a tick handler can
// re-enter the Fake (Reset/Stop/NewTimer) without deadlocking. The
// mutex is re-acquired before the next heap operation.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	deadline := f.now.Add(d)
	for {
		if f.timers.Len() == 0 {
			break
		}
		top := f.timers[0]
		if top.expiry.After(deadline) {
			break
		}
		t := heap.Pop(&f.timers).(*fakeTimer)
		if t.stopped {
			continue
		}
		f.now = t.expiry
		fireAt := t.expiry
		f.mu.Unlock()
		select {
		case t.ch <- fireAt:
		case <-time.After(quiescenceTimeout):
			panic("FakeClock: tick send blocked > 1s; consumer not draining")
		}
		f.mu.Lock()
		if t.period > 0 && !t.stopped {
			t.expiry = fireAt.Add(t.period)
			t.seq = f.nextSeqLocked()
			heap.Push(&f.timers, t)
		}
	}
	f.now = deadline
	f.mu.Unlock()
}

// nextSeqLocked returns the next monotonic registration counter.
// Caller must hold f.mu.
func (f *Fake) nextSeqLocked() uint64 {
	f.seq++
	return f.seq
}

// fakeTimer is the heap element. idx is the live heap index per
// container/heap conventions so Stop/Reset can find-and-remove by
// index without an O(n) search.
type fakeTimer struct {
	expiry  time.Time
	seq     uint64
	period  time.Duration
	ch      chan time.Time
	stopped bool
	idx     int // -1 when not in heap
}

// fakeTimerHeap implements container/heap.Interface keyed on
// (expiry, seq). Same-instant timers order by seq → FIFO of
// registration.
type fakeTimerHeap []*fakeTimer

func (h fakeTimerHeap) Len() int { return len(h) }

func (h fakeTimerHeap) Less(i, j int) bool {
	if h[i].expiry.Equal(h[j].expiry) {
		return h[i].seq < h[j].seq
	}
	return h[i].expiry.Before(h[j].expiry)
}

func (h fakeTimerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idx = i
	h[j].idx = j
}

func (h *fakeTimerHeap) Push(x any) {
	t := x.(*fakeTimer)
	t.idx = len(*h)
	*h = append(*h, t)
}

func (h *fakeTimerHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	old[n-1] = nil
	t.idx = -1
	*h = old[:n-1]
	return t
}

// fakeTimerHandle is the wrapper returned by NewTimer. It carries
// pointers back to the underlying *fakeTimer and the parent *Fake so
// Stop/Reset can find-and-remove via the live heap index.
type fakeTimerHandle struct {
	t *fakeTimer
	f *Fake
}

func (h *fakeTimerHandle) C() <-chan time.Time { return h.t.ch }

// Stop mirrors stdlib *time.Timer.Stop: returns true if the call
// actually removed the timer before it fired, false if the timer had
// already fired or been stopped.
func (h *fakeTimerHandle) Stop() bool {
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	if h.t.stopped {
		return false
	}
	if h.t.idx >= 0 {
		heap.Remove(&h.f.timers, h.t.idx)
		h.t.stopped = true
		return true
	}
	h.t.stopped = true
	return false
}

// Reset mirrors stdlib *time.Timer.Reset: returns true if the timer
// was still active (in the heap), false if it had already fired or
// been stopped. The timer re-registers at the tail with a fresh seq,
// preserving the FIFO contract.
func (h *fakeTimerHandle) Reset(d time.Duration) bool {
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	wasActive := h.t.idx >= 0
	if wasActive {
		heap.Remove(&h.f.timers, h.t.idx)
	}
	h.t.expiry = h.f.now.Add(d)
	h.t.seq = h.f.nextSeqLocked()
	h.t.stopped = false
	h.t.period = 0
	heap.Push(&h.f.timers, h.t)
	return wasActive
}

// fakeTickerHandle is the wrapper returned by NewTicker.
type fakeTickerHandle struct {
	t *fakeTimer
	f *Fake
}

func (h *fakeTickerHandle) C() <-chan time.Time { return h.t.ch }

// Stop halts the ticker. Subsequent Advance calls will not fire it.
func (h *fakeTickerHandle) Stop() {
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	if h.t.stopped {
		return
	}
	if h.t.idx >= 0 {
		heap.Remove(&h.f.timers, h.t.idx)
	}
	h.t.stopped = true
}

// Compile-time interface assertions.
var (
	_ Clock     = (*Fake)(nil)
	_ FakeClock = (*Fake)(nil)
	_ Timer     = (*fakeTimerHandle)(nil)
	_ Ticker    = (*fakeTickerHandle)(nil)
)
