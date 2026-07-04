# 0017 — Synchronous delivery-drain seam for byte-deterministic chaos traces

**Status:** Accepted
**Date:** 2026-07-04
**Scope:** `pkg/transport/inproc` (`config.go`, `hub.go`, `dispatcher.go`), `internal/raftest` (`cluster.go`)

## Context

Phase 11's seeded in-process chaos matrix (`test/chaos/inproc`) binds SC1 on a
byte-determinism gate — `TestSeededMatrix_SameSeedIdenticalTrace`: the same
`-seed` must produce a byte-identical committed history, mirroring the Hub's own
`TestHub_SameSeedIdenticalTrace`. That gate FAILED under chaos.

Root cause was NOT in the chaos RNGs (those are split-seed deterministic,
ADR-0007) but in the delivery SCHEDULE observed by the harness:

- The `inproc.Hub` schedules every delivery deterministically on a
  `(deliverAt, seq)` min-heap, drained by a single background dispatcher
  goroutine (`dispatch`).
- `internal/raftest.Cluster.Tick` (model ii, ADR after 07-04) drives a
  synchronous two-pass loop and, between passes, called `deliveryQuiesce()` —
  which was a literal `time.Sleep(2 * time.Millisecond)` waiting for that async
  dispatcher goroutine to land due deliveries onto receiver inbound channels.
- WHICH `Tick` observed a given delivery therefore depended on how far the
  dispatcher goroutine progressed inside that 2 ms wall-clock window. Under
  chaos (drop/delay/reorder/duplicate/partition) that timing jitter changed the
  committed set run-to-run: e.g. at seed 42 run A committed `k21` at commit index
  3 while run B committed `k25` at the same index. Same seed, divergent trace.

The `time.Sleep` was also the standing `check-no-time-now` blocker on
`cluster.go` carried since 07-04.

## Decision

Add a SYNCHRONOUS delivery-drain seam to the Hub and route the harness through
it, eliminating the goroutine/wall-clock handoff:

1. `HubConfig.SyncDelivery bool`. When true, `NewHub` does **not** start the
   background `dispatch` goroutine. Default false — every other caller
   (`hub_test`, `chaos_test`, `transporttest` conformance, and any production
   consumer) keeps the async single-dispatcher model UNCHANGED.

2. `func (h *Hub) DrainDueSync()`. Delivers, on the CALLING goroutine, every
   message whose `deliverAt <= clk.Now()` onto its receiver's inbound channel,
   reusing the exact drain (`drainDueLocked`) and reorder (`orderLocked`) the
   async dispatcher uses, then returns when no due message remains. Blocking
   channel send is guarded by `h.ctx.Done` so a concurrent `Close` unblocks a
   full-buffer send. Valid only on a `SyncDelivery` Hub (no competing drainer).

3. `raftest.NewCluster` builds its Hub with `SyncDelivery: true`, and
   `Cluster.deliveryQuiesce` now calls `Hub.DrainDueSync()` instead of sleeping.

Because the sync drain runs at exactly `clk.Now()` with no goroutine race, the
set of messages each pass observes is a pure function of `(seed, FakeClock
state, chaos seed)`.

Rejected alternative: keeping the async dispatcher and making the harness poll
the heap until quiescent. That still races the dispatcher goroutine against the
poller for the same heap and reintroduces a wall-clock dependence; a single
in-line drainer with no competing goroutine is strictly simpler and race-free.

## Consequences

- `TestSeededMatrix` and `TestSeededMatrix_SameSeedIdenticalTrace` pass under
  `-race`; re-running any seed produces byte-identical `history.json`.
- The async delivery path is untouched: `TestHub_SameSeedIdenticalTrace` and the
  full `pkg/transport/inproc` / `transporttest` suites stay green.
- The 1000-seed Phase 5-7 invariant sweeps (`TestAtMostOneLeaderPerTerm_Chaos`,
  `TestNoLogDivergence_Chaos`) — which drive `raftest.NewCluster`/`Cluster.Tick`
  — pass under `-race` with `TOYRAFT_CHAOS_FULL=1`, so the sync-drain seam is a
  behaviour-preserving change to the shared harness, not just a chaos-test hack.
- `cluster.go` no longer contains `time.Sleep`; `make check-no-time-now` is now
  clean there.
- New exported surface (`HubConfig.SyncDelivery`, `Hub.DrainDueSync`) is additive;
  `docs/lld-go-doc-golden.txt` regenerated via `make lld-drift-update`.

## Usage

- Deterministic in-process chaos / invariant harnesses: build the Hub with
  `HubConfig{SyncDelivery: true}` and pump deliveries via `DrainDueSync()` at each
  logical tick between driving nodes. Never mix with the async dispatcher.
- Production / async callers: leave `SyncDelivery` at its false default and rely
  on the background dispatcher; do NOT call `DrainDueSync()` (it would race the
  dispatcher over the same heap).
