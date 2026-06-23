# 0007 — inproc Hub Chaos: Seed-Splitting and Single Dispatcher

**Status:** Accepted
**Date:** 2026-06-23
**Scope:** `pkg/transport/inproc`

## Context

Phase 4 SC3 demands "same `int64` seed → byte-identical message
delivery trace" across the five chaos knobs the LLD §6 PUBLIC surface
exposes: `Partition` / `Heal` / `DropRate` / `Delay` / `Reorder` /
`Duplicate`. Two independent sources of nondeterminism would break
SC3:

1. **Scheduler-dependent RNG draw order.** If chaos decisions happened
   on per-message goroutines, Go's scheduler would interleave them
   non-reproducibly under `-race`, and same-seed runs would diverge.
2. **Sub-RNG sub-seed correlation.** If the per-knob RNG streams were
   derived from the master seed by a thin transform (e.g. XOR with a
   tag), toggling one knob could shift another's stream — bisecting a
   chaos failure becomes impossible because changing the test
   configuration changes the seed-derived input to every other knob.

LLD §6 freezes the chaos surface; any change requires an ADR and an
LLD amendment in the same PR (Working Agreement 4). ADR-0006 already
locked the FakeClock determinism model; this ADR locks the seed-
splitting + single-dispatcher model the Hub layers on top.

References: RESEARCH §Patterns 2 + 3, RESEARCH §Pitfalls 2 / 4 / 6 / 7,
ROADMAP Phase 4 SC3, TESTING §2, LLD §6, ADR-0004 (single-mutex),
ADR-0006 (FakeClock).

## Decision

We will:

1. **Run all delivery on a single dispatcher goroutine** ordered by a
   `(deliverAt, seq)` `container/heap` min-heap. RNG draws happen on
   the goroutine that calls `Hub.send` (the node's tick loop, itself
   driven synchronously by `FakeClock.Advance` per ADR-0006). NO per-
   message goroutines.
2. **Allocate one `*rand.Rand` per knob.** Sub-RNGs are PCG sources
   from `math/rand/v2`. The knobs and their tags:
   `"drop"`, `"delay"`, `"reorder"`, `"duplicate"`.
3. **Derive sub-seeds via SHA-256 of `(seed_be8 || tag)`.** Specifically
   `sub_seed = int64(SHA-256(BigEndian.PutUint64(seed) || []byte(tag))[:8])`.
   PCG is then seeded with `(uint64(sub_seed), uint64(sub_seed) ^
   0x9E3779B97F4A7C15)` so a small `seed` does not yield a tiny PCG
   state.
4. **Treat the tag strings as ABI.** Renaming or reordering a tag re-
   keys every committed test seed in the repository. Forbidden post-
   ADR; a rename requires a new ADR superseding this one and a sweep
   of every committed seed.
5. **Install partitions symmetrically.** `Partition(a, b)` writes both
   `{a, b}` and `{b, a}` to the partitions map; `Heal(a, b)` deletes
   both. Asymmetric partitions are out of scope for v1.
6. **Iterate peers via `Hub.sortedNodes` (slice), never the `nodes`
   map.** Map iteration would leak Go's randomised order into the
   delivery trace.
7. **Define reorder semantics as bucketed window-shuffle.** When
   `chaos.reorderOn == true`, the dispatcher drains the heap of every
   due pending into a single batch, buckets the batch per receiver
   walking `sortedNodes`, and shuffles each bucket in `queueDepth`-
   sized windows via `reorderRNG.Shuffle`. `queueDepth < 2` degrades
   to FIFO by construction — a one-element window has nothing to
   permute. Documented in `Hub.Reorder` godoc and guarded by
   `TestHub_Reorder_QDOneDegradesToFIFO`.
8. **Ban wall-clock leakage outside `internal/clock/real.go`.**
   `scripts/check-no-time-now.sh` greps for `time.Now()` in
   `pkg/raft`, `pkg/transport/inproc`, and `internal/raftest`
   (excluding `*_test.go`). The Makefile `verify` target invokes both
   `check-lld-drift` and `check-no-time-now`.
9. **Pump the dispatcher off the same FakeClock the chaos layer reads
   for `deliverAt`.** The dispatch loop arms a single reusable
   `clock.Timer` for the next due `deliverAt` and selects on
   `(wake | ctx.Done | timer.C)`. FakeClock.Advance therefore
   synchronously fires the timer and wakes the dispatcher — delayed
   messages become observable inside the same `Advance` call that
   advances time past their `deliverAt`.

## Consequences

**Positive**

- **SC3 is verifiable.** `TestHub_SameSeedIdenticalTrace` runs two
  scenarios at the same seed and asserts the per-receiver delivery
  trace is `reflect.DeepEqual`. Drops, delays, and duplicates all
  participate.
- **Bisection of chaos failures is possible.** Toggling one knob's
  threshold does not shift any other knob's stream — when a Phase 11
  chaos test fails at `seed=K, drop=0.1`, the operator can rerun at
  `seed=K, drop=0.0` and see exactly which messages the drop knob
  removed without disturbing the delay or duplicate decisions.
- **No goroutine leaks.** Single dispatcher + sync.Once-guarded Close
  + select-on-ctx.Done across every channel send keeps `goleak`
  clean.
- **Wall-clock leaks are caught at lint time.** A grep gate makes
  Pitfall 3 fail loud during the commit that introduced it, not three
  phases later during a flaky CI run.

**Negative**

- **Reorder with `queueDepth=1` silently degrades to FIFO.** The
  limitation is documented in the `Reorder` godoc and pinned by a
  test — but a future contributor who reads only the signature and
  expects "reorder=true means reordered" will be surprised. The test
  name `TestHub_Reorder_QDOneDegradesToFIFO` is deliberately verbose
  so a grep over the test corpus surfaces the acknowledgment.
- **Renaming a tag string re-keys every committed seed.** The tag
  strings are now ABI. A rename requires a new ADR and a sweep of
  every committed seed in the repository.
- **One extra reusable `FakeClock` timer is registered per dispatcher
  cycle.** Negligible in practice — the timer is `Stop`+`Reset`ed in
  place, not freshly allocated — but tests that count items in the
  FakeClock heap mid-Advance must account for it.

**Follow-ups**

- Phase 11 chaos tests will exercise multi-knob scenarios at varying
  seeds; this ADR's SHA-256-tagged sub-RNGs are the contract those
  tests rely on.
- A per-link `Delay(from, to, min, max)` variant has been deferred to
  v2; LLD §6's global `Delay(min, max)` is sufficient for Phase 4–6.
- `internal/raftest.Recorder` (plan 04-05) will derive history-event
  timestamps from `FakeClock.Now().UnixNano()`, NOT `time.Now()`, so
  the wall-clock-ban lint extends naturally to that package.

## Alternatives Considered

- **XOR-fold seed and tag.** Cheap but documented to produce
  correlated sub-streams when the tag namespace and seed namespace
  overlap. SHA-256 is free at test scale and bulletproof.
- **Per-message goroutine for delivery.** Re-introduces scheduler-
  dependent RNG draw interleaving (RESEARCH Pitfall 4). Rejected.
- **Single shared `*rand.Rand` for all knobs.** Toggling one knob's
  threshold shifts the others' streams. Rejected — defeats the
  bisection use case the per-knob split exists for.
- **Per-link `Delay(from, to, ...)` in v1.** LLD §6's signature
  `Delay(min, max)` is global; a per-link variant is a v2 extension
  and would require an LLD amendment.
- **`testing/synctest` (Go 1.25) for quiescence.** Promising but
  experimental, hides the Advance semantics the LLD makes
  contractual. Revisit if Go 1.26 stabilises it.

## References

- RESEARCH §Patterns 2 + 3, §Pitfalls 2 / 4 / 6 / 7, §State of the Art
  (`.planning/phases/04-test-infrastructure-fakeclock-inproc-hub/04-RESEARCH.md`)
- ROADMAP Phase 4 SC3 (`.planning/ROADMAP.md`)
- TESTING §2 — Hub contract
- LLD §6 — chaos surface freeze
- ADR-0004 — single-mutex policy
- ADR-0006 — FakeClock determinism model
