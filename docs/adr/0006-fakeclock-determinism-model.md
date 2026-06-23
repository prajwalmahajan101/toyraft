# ADR-0006: Deterministic FakeClock Model

**Status:** Accepted
**Date:** 2026-06-23
**Scope:** `internal/clock` (Fake impl + Advance contract), downstream consumers in `pkg/transport/inproc`, `internal/raftest`, `pkg/raft` (phase 5+).

## Context

Phase 4's whole reason for existing is the **deterministic substrate**
every later test phase (5, 6, 7, 11, 12) consumes. Its correctness is
judged by one acceptance test: spin up N nodes on a single seed, record
the message trace and the client history, run twice, diff — bit-for-bit
identical or the build is red. The FakeClock sits at the bottom of that
stack; if it loses determinism the rest of the pyramid collapses.

ROADMAP Phase 4 makes the requirement explicit:

- **SC1** — `internal/clock` ships `Real` and `Fake` covering `Now`,
  `After`, `NewTimer`, `NewTicker`.
- **SC2** — `Fake.Advance(d)` fires timers in deterministic order,
  proven by an N-timers-same-instant test over 100 runs (TESTING.md §3
  rule 2: "FIFO of registration").

The Go ecosystem has two stable answers for fake-clock quiescence:

1. **`benbjohnson/clock`** uses scheduler-yield sprinkles
   (`runtime.Gosched()`) to "wait for timers". The library's own
   README notes it "makes no hard promises" about quiescence; Coder's Quartz blog post documents reproducible
   flake under `-race -count=100`. Probabilistic, unacceptable for a
   library whose acceptance test is bit-identical histories.
2. **`coder/quartz`** uses an explicit `Wait()` on `AdvanceWaiter` plus
   a `Trap` API. Deterministic, but the `Wait` contract wraps consumer
   code in `TickerFunc`/`AfterFunc` callbacks — and the ToyRaft LLD §3
   freeze commits to `<-chan time.Time` ticker channels. Adapter
   friction would be non-trivial.

A third option, `testing/synctest` in Go 1.25, advances time when all
bubble goroutines are blocked. It is experimental, hides explicit
`Advance(d)` semantics the LLD makes contractual, and is documented to
re-evaluate once it stabilises.

ToyRaft has a much narrower concurrency surface than either Quartz or
synctest targets. CONCURRENCY.md §1 enumerates every long-lived
goroutine per node, and the tick loop is the **single consumer** of
`Clock.NewTicker().C` per node. We can therefore demand the consumer
ack each tick before the next fires, without the combinatorial blow-up
that motivates Quartz's trap shape. ADR-0004's single-mutex policy
gives us the same lever on the cluster harness side.

PROC-06 also bites: every implementation phase lands at least one ADR.
The FakeClock model is the first locking decision of Phase 4 and the
one that downstream phases (5+) cannot drift without breaking the
deterministic-substrate contract — exactly the shape ADRs exist to
pin.

## Decision

We will land `internal/clock.Fake` with the following load-bearing
properties. Each is a contract that downstream tests rely on; drifting
any of them requires a superseding ADR per [ADR-0000].

**1. `Fake.Advance(d)` is synchronous.** It returns only after every
timer due by `now+d` has been fired AND the consumer has read from the
fire channel. There is no "background draining" goroutine inside the
Fake.

**2. The timer heap is keyed on `(expiry, seq)`.** `seq` is a monotonic
registration counter incremented under `Fake.mu`. Same-instant timers
fire in registration order — TESTING.md §3 rule 2 "FIFO of
registration" verbatim. Tiebreaks based on pointer addresses, map
iteration order, or any other allocator/scheduler-dependent value are
forbidden.

**3. Each fire is a synchronous channel send under a 1-second
safety-net timeout.** The Advance loop is:

```go
select {
case t.ch <- t.expiry:
case <-time.After(quiescenceTimeout):
    panic("FakeClock: tick send blocked > 1s; consumer not draining")
}
```

where `quiescenceTimeout = 1 * time.Second`. The panic is a
**load-bearing assert**: silent hangs are the worst test outcome, and a
buggy consumer that forgot to drain its ticker gets a clear actionable
message in <1s instead of an indefinite block. The 1s budget is
deliberately generous so legitimate slow setup paths (e.g. the cluster
harness wiring up a node's first receive) are not flagged.

**4. `Fake.mu` is released around each channel send.** Re-acquired
before the next heap pop. This is what allows a consumer's tick handler
to call back into the FakeClock (`Reset`, `Stop`, `NewTimer`,
`NewTicker`) without deadlocking. Heap operations themselves still run
under the lock.

**5. Tickers re-register at the heap tail with a fresh `seq` after
each fire.** This preserves FIFO fairness across periods: if tickers A,
B, C registered in that order all fire at the same instant, the next
period's instant fires them in the same order again because each
re-registration grabs a strictly larger `seq`.

**6. No scheduler-yield calls. No `time.AfterFunc`. No callback shape on
the FakeClock.** TESTING.md §3 rule 4 already forbids invoking user
code on the FakeClock's goroutine; we do not even import the stdlib
APIs that would tempt that pattern. Channel sends only. The only
permitted reference to `time.After` inside `internal/clock/fake.go` is
the 1s safety-net timer in the Advance select.

**7. The narrower `pkg/raft.Clock` interface (Now + NewTicker per LLD
§3) is satisfied structurally by `*Fake`.** A compile-time assertion
will land alongside `pkg/raft.Clock`'s declaration in phase 5; until
then the `internal/clock/doc.go` TODO and the
`TestFake_NarrowShape` runtime check guard the contract.

### Alternative considered: callback-shaped `Fake`

A `Fake.AdvanceWithCallback(d, func(t time.Time))` shape, mirroring
`quartz.Mock`, would let the FakeClock invoke a registered consumer
inline. Rejected because:

- LLD §3 freezes `Ticker` to return `<-chan time.Time` and `Timer`
  similarly. A callback shape would either contradict the LLD or
  require parallel "callback" and "channel" surfaces.
- The single-tick-consumer-per-ticker invariant from CONCURRENCY.md §1
  removes the combinatorial pressure that motivates Quartz's trap
  shape in the first place.

### Alternative considered: pointer-address tiebreak

Using `uintptr(unsafe.Pointer(t))` as the heap tiebreak instead of a
seq counter was considered for terseness. Rejected: pointer addresses
are heap-allocator dependent and not stable across runs — exactly the
property SC2 forbids.

### Alternative considered: `testing/synctest`

Go 1.25's `testing/synctest` is observably the direction the stdlib is
moving. Rejected for v1 because the "bubble" model advances time only
when every bubble goroutine is blocked — too coarse for tests that want
to inspect node state mid-advance — and because it hides the explicit
`Advance(d)` call the LLD §3 wording makes contractual. Revisit in v2
if the API graduates from experimental.

## Consequences

**Positive**

- SC2 is provable: `TestFakeAdvance_StableOrder_100Runs` registers 16
  same-instant timers, advances once, asserts firing order equals
  registration order — 100 runs, no flake, no `-count=100` retry
  harness needed.
- Tests can express "fire every due timer in deterministic order" as a
  single `Advance(d)` call. Phase 5 election timeouts become bisectable
  from a seed.
- A buggy consumer (forgot to drain its ticker) gets a clear panic in
  <1s with an actionable message instead of an indefinite hang.
- The contract is small enough to audit: `internal/clock/fake.go` is
  ~270 lines and every public method either touches the heap under the
  lock or runs the documented synchronous-send select. No hidden
  goroutines.

**Negative**

- Consumers MUST be parked on the receive **before** `Advance` is
  called. The Phase 5 cluster harness (plan 04-05) will own this
  invariant by structuring each node's goroutine to enter the receive
  before yielding back to the test driver. The `TestFake_NewTicker_FiresPeriodically`
  test uses a `chan struct{}` ready handshake to model the same pattern.
- `TestFake_NewTicker_QuiescenceTimeout_Panics` takes ~1s of wall-clock
  time by design — the safety net is fixed at 1s. Acceptable cost
  (one test, one second, paid only when running the full suite).
- `Fake.mu` release around the channel send means a consumer's tick
  handler observes a possibly-stale `Fake.now` only between Pop and
  channel send; that is the documented model and consumers should not
  read `Fake.now` from inside a tick handler if they need pre-fire
  semantics.

**Follow-ups**

- Plan 04-04 wires `scripts/check-no-time-now.sh` to ban
  `time.{Now,After,NewTimer,NewTicker}` outside `internal/clock/real.go`
  and the documented `time.After` site in `internal/clock/fake.go`'s
  Advance safety net.
- Phase 5: replace the `TestFake_NarrowShape` runtime check with
  `var _ raft.Clock = (*clock.Fake)(nil)` once `pkg/raft.Clock` lands.
- If a phase 11 chaos test ever needs an intentionally-undrained ticker
  (e.g. to simulate a crashed node) for longer than 1s without
  panicking, the panic budget must be re-litigated in a superseding ADR
  rather than silently lengthened.
- ADR-0007 (planned, plan 04-04) will record the Hub's seed-splitting
  + single-dispatcher determinism model — the FakeClock contract here
  is its bottom-of-stack precondition.

## Usage

Every test under `internal/clock`, `pkg/transport/inproc`,
`internal/raftest`, and `pkg/raft` that wants logical time uses
`*clock.Fake` allocated via `clock.NewFake()`. Production code receives
a `clock.Clock` via injection (`Config.Clock`) and never imports the
Fake.

When reviewing a PR that touches `internal/clock/fake.go`:

- Any new goroutine spawn inside `Fake.Advance` is a red flag — the
  Advance contract is single-goroutine end-to-end except for the
  consumer goroutines reading the channels.
- Any scheduler-yield call (`runtime.Gosched()` or equivalent) is
  forbidden by Decision 6; flag and revert.
- A new tiebreak value in `fakeTimerHeap.Less` that is not
  `(expiry, seq)` is a red flag — it likely breaks SC2.
- The 1s `quiescenceTimeout` constant is load-bearing; changing it
  requires a superseding ADR.

When reviewing a test that calls `Advance`:

- The consumer (goroutine reading the timer/ticker channel) MUST be
  parked on the receive **before** `Advance` is invoked. If the test
  uses a goroutine, a `chan struct{}` ready handshake or equivalent is
  the standard idiom — see `TestFake_NewTicker_FiresPeriodically` for
  the canonical pattern.

## References

- `.planning/phases/04-test-infrastructure-fakeclock-inproc-hub/04-RESEARCH.md`
  §Pattern 1, §Pitfalls 1 + 10.
- `.planning/ROADMAP.md` Phase 4 SC1, SC2.
- `.planning/REQUIREMENTS.md` TEST-01, TEST-02, PROC-06.
- `docs/TESTING.md` §3 (FakeClock contract, rules 2 and 4).
- `docs/LLD.md` §3 (Clock/Ticker frozen interfaces).
- `docs/CONCURRENCY.md` §1 (goroutine census, single-tick-consumer
  invariant).
- `internal/clock/fake.go` — source of truth for the implementation.
- `internal/clock/fake_test.go` — SC2 100-run stability test and the
  quiescence-panic test.
- [ADR-0000] — record architecture decisions (status transitions are
  append-only; supersession lands as a new ADR).
- [ADR-0004] — single-mutex policy informs the `Fake.mu` lock
  discipline.
- [ADR-0005] — storage interface freeze precedent for Phase-N freeze
  ADRs.

[ADR-0000]: ./0000-record-architecture-decisions.md
[ADR-0004]: ./0004-single-mutex-state-machine.md
[ADR-0005]: ./0005-storage-interface-freeze.md
