// Package clock — design notes (godoc continuation).
//
// # Quiescence model
//
// The Fake implementation (landing in plan 04-02; see ADR-0006) drives all
// timers from explicit Advance(d) calls. Fake.Advance is "quiescent": it
// returns only once every goroutine woken by the advance has reached its
// next blocking point, so consensus tests are reproducible from a seed and
// free of real wall-clock dependence. The Real implementation in this
// package is the production-equivalent counterpart and simply delegates to
// stdlib time.
//
// # Wall-clock-leak ban
//
// All Raft core code under pkg/raft, pkg/transport/inproc, and
// internal/raftest MUST receive a Clock and call clk.Now() — never the
// stdlib wall-clock entry point — and similarly route all timer/ticker
// creation through the Clock. The only sanctioned site in the production
// tree for the stdlib time entry points (the current-time call plus After,
// NewTimer, NewTicker) is internal/clock/real.go.
//
// This rule is enforced by scripts/check-no-time-now.sh, landing in plan
// 04-04. Until that script is wired, the ban is documented here as the
// authoritative source.
//
// pkg/raft.Clock compatibility
//
// TODO(phase-5): once pkg/raft declares its narrow Clock interface (Now +
// NewTicker per LLD §3), add a compile-time assertion in real_test.go
// proving *internal/clock.Real structurally satisfies pkg/raft.Clock. This
// package cannot import pkg/raft today because pkg/raft.Clock does not yet
// exist; the structural-compat guarantee is therefore deferred.
package clock
