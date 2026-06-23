// Package clock provides a Clock abstraction for ToyRaft. The Real impl
// wraps stdlib time; the Fake impl (added in plan 04-02) drives all timers
// from explicit Advance(d) calls so consensus tests are reproducible from
// a seed. See ADR-0006 (landing in plan 04-02).
package clock

import "time"

// Clock is the FakeClock-compatible extension of pkg/raft.Clock used by
// internal test infrastructure. internal/clock.Real also satisfies the
// narrower pkg/raft.Clock surface declared in LLD §3 (Now + NewTicker).
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	NewTimer(d time.Duration) Timer
	NewTicker(d time.Duration) Ticker
}

// Timer mirrors stdlib *time.Timer with a channel accessor.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

// Ticker matches the LLD §3 frozen Ticker surface.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}
