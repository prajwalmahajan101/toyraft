package clock

import "time"

// Real is a Clock backed by stdlib time. Allocate via clock.NewReal() so
// the zero value of clock.Real cannot leak into production paths
// uninitialised.
type Real struct{}

// NewReal returns the canonical Real clock.
func NewReal() *Real { return &Real{} }

// Now returns the current wall-clock time. The single call below is the
// only sanctioned wall-clock read in the entire production tree per
// check-no-time-now.sh (landing in plan 04-04); every other package must
// route through a Clock.
func (*Real) Now() time.Time { return time.Now() }

// After is a passthrough to time.After.
func (*Real) After(d time.Duration) <-chan time.Time { return time.After(d) }

// NewTimer wraps *time.Timer in the Timer interface.
func (*Real) NewTimer(d time.Duration) Timer { return &realTimer{t: time.NewTimer(d)} }

// NewTicker wraps *time.Ticker in the Ticker interface.
func (*Real) NewTicker(d time.Duration) Ticker { return &realTicker{t: time.NewTicker(d)} }

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time        { return r.t.C }
func (r *realTimer) Stop() bool                 { return r.t.Stop() }
func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

type realTicker struct{ t *time.Ticker }

func (r *realTicker) C() <-chan time.Time { return r.t.C }
func (r *realTicker) Stop()               { r.t.Stop() }

// Compile-time interface assertions.
var (
	_ Clock  = (*Real)(nil)
	_ Timer  = (*realTimer)(nil)
	_ Ticker = (*realTicker)(nil)
)
