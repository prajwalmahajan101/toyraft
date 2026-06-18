# ToyRaft — TESTING

**Status:** Accepted
**Date:** 2026-06-18
**Scope:** project-wide; binds Phase 4 (test infra), Phase 11 (chaos),
Phase 12 (linearizability), Phase 13 (netns)

## Purpose

This document declares the **shape** of the test surface: what we test
at each layer, what each layer proves (and does NOT prove), the
contracts the test harness must satisfy, and the project's policy on
flaky tests. Phase 4 builds the harness to these contracts; Phase 11
implements chaos scenarios against the in-process Hub; Phase 12 wires
in Porcupine and the linearizability scenarios; Phase 13 adds the
linux-only `netns` chaos layer.

This is a **contract document**. Tests that violate it (e.g., introduce
a Hub that never reorders, or add a `t.Skip` for a flake) are
review-blocking.

---

## 1. Test layers

| Layer                                  | Tool / location                              | What it proves                                                                | What it does NOT prove                                                |
| -------------------------------------- | -------------------------------------------- | ----------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| Unit                                   | `pkg/raft/*_test.go`                          | Log invariants, election predicates, type round-trip, Figure 7 up-to-date check | End-to-end behaviour, multi-node convergence, partition handling      |
| Integration                            | `internal/raftest/cluster.go`                | Multi-node convergence on `FakeClock` + in-process `Hub` (no chaos knobs lit) | Wall-clock timing, OS-level effects, real network reordering          |
| Seeded chaos                           | `test/chaos/inproc/`                         | Reproducible partition / reorder / drop / delay / duplicate behaviour from a single `-seed=<int64>` flag | Real-process crash semantics; OS-level network behaviour              |
| Process-kill chaos                     | `test/chaos/processkill/`                    | Real `fsync`, real network, PGID cleanup, mid-fsync crash recovery (T-6, T-9) | Network-layer partitions (use netns for those)                        |
| Netns chaos (linux-only, build-tagged) | `test/chaos/netns/` (`//go:build linux`)     | Real `iptables` / `tc` partitions, latency injection, real loss              | Non-linux platforms                                                   |
| Linearizability                        | `test/linearizability/` + Porcupine          | KV register history is linearizable across leader churn + packet loss        | Liveness; absence of fault-handling regressions; performance          |

### Rules per layer

- **Unit tests scale timing down.** `HeartbeatInterval = 1ms`,
  `ElectionTimeoutMin = 5ms`, `ElectionTimeoutMax = 10ms`. This forces
  the race window to actually open — see T-4 in `research/PITFALLS.md`.
- **Integration tests use `FakeClock` exclusively.** No `time.Sleep`,
  no real timers. The cluster is deterministic; the same seed
  reproduces the same message trace.
- **Seeded chaos is the v1 acceptance gate.** Any partition-related bug
  must be reproducible from a printed seed. CI logs the seed on every
  run.
- **Process-kill chaos uses subprocess `exec.Cmd` with a private PGID.**
  Cleanup kills the group, not the parent (T-9).
- **Netns chaos lives behind `//go:build linux` and is skipped
  elsewhere.** macOS / Windows developers see "skipped, linux-only"
  rather than a failure.
- **Linearizability is the v1 release gate.** A green Porcupine run on
  the three required scenarios (§5) is required for `v1.0.0`.

---

## 2. In-process Hub contract

The in-process transport (`pkg/transport/inproc.Hub`) is the heart of
the seeded chaos suite. Its contract is below; an implementation that
omits any of the five knobs is a false-negative trap (PITFALL T-1 /
P1-10: "an always-deliver, always-in-order Hub is a false-negative
trap").

### MUST inject

| Knob          | Behaviour                                                                                          |
| ------------- | -------------------------------------------------------------------------------------------------- |
| **Reorder**   | Queue messages per destination; deliver in a seeded-random permutation, not FIFO                   |
| **Duplicate** | With probability `p_dup`, deliver a message twice (independent `Send` invocations from receiver side) |
| **Drop**      | With probability `p_drop`, never deliver a message                                                  |
| **Delay**     | Apply a seeded-random delay per message, integrated against `FakeClock`                            |
| **Partition** | Filter by `(source, dest)` pair; drop all messages for a configurable wall-clock duration          |

### Determinism contract

All five knobs are driven by a single `int64` seed, plumbed through:

```go
type HubConfig struct {
    Seed        int64
    DropRate    float64 // 0..1
    DuplicateRate float64 // 0..1
    MaxDelay    time.Duration // seeded-random in [0, MaxDelay]
    Partitions  []PartitionSpec // {From, To, Start, End} on the FakeClock timeline
}

func NewHub(cfg HubConfig, clk Clock) *Hub
```

**Same seed → identical message trace.** This is verified by a test
that runs the same seed twice and diffs the message history; any
divergence is a Hub bug.

### Why all five matter

- **Reorder** catches algorithms that rely on per-pair message order
  (Raft does not — but implementations sometimes accidentally do via
  `if msg.Term == n.currentTerm + 1` style checks). HTTP/1.1 with a
  client pool reorders requests across connections; the in-process
  transport must match that worst case.
- **Duplicate** catches non-idempotent handlers — e.g., a follower that
  double-applies an `AppendEntries` whose `prevLogIndex` matches but
  whose `entries[0]` is already present (P0-3 variant).
- **Drop** is the simplest fault mode and the most likely to be
  forgotten in a chaos config — explicitly assert `p_drop > 0` in at
  least one scenario.
- **Delay** widens the window for races between RPCs and ticks (C-6 /
  C-7 territory).
- **Partition** is the canonical Raft failure mode. The Hub MUST
  support time-bounded partitions (`{Start, End}` on `FakeClock`) so
  scenarios can script partition / heal sequences deterministically.

### Hub knobs are public surface

`pkg/transport/inproc.HubConfig` is public so consumer projects writing
their own chaos tests against a ToyRaft cluster can reuse the Hub. The
five knobs are part of the documented LLD public API (see
`docs/LLD.md` §"Test-only public surface").

---

## 3. FakeClock determinism contract

`internal/clock.FakeClock` is the only clock allowed in unit and
integration tests. The contract:

```go
type FakeClock interface {
    Now() time.Time
    NewTimer(d time.Duration) Timer
    NewTicker(d time.Duration) Ticker
    Advance(d time.Duration) // ONLY way to move time forward
}
```

### Rules

1. **`Advance(d)` is the only way to move time forward in tests.** No
   `time.Sleep` in test bodies. Reviewers reject PRs that introduce
   `time.Sleep` in `_test.go` files.
2. **Timers registered at the same instant fire in a stable order**
   (TEST-02). The stable order is FIFO of registration. This makes
   tests that register multiple timers at the same logical instant
   deterministic across runs.
3. **`Advance` fires all timers that would have expired in `[now, now+d]`
   in registration order**, advancing the clock incrementally to each
   timer's expiry before invoking the next. A timer registered by a
   callback fired during `Advance` is queued for the remainder of the
   call.
4. **No callbacks via `time.AfterFunc`.** Test code consumes timer
   channels in a `select`; the FakeClock never invokes user code on its
   own goroutine.

### Why this matters

- Without rule 2, the same seed could produce different message orders
  on different runs (because of map-iteration nondeterminism in the
  timer-fire loop) — the entire seeded-chaos premise collapses.
- Without rule 1, tests would re-introduce wall-clock dependence and
  the test suite would become slow + flaky.

---

## 4. "Flake = P0 bug" policy

> CI does NOT retry. A test that fails once and passes the second time
> IS the bug.

### Rules

- **No `t.Skip` for flaky tests.** Use `t.Skip` only for environment
  gates (e.g., "skip on non-linux for netns").
- **No `flaky-test` tags.** No retry-on-failure CI configuration.
- **The seed of every chaos run is logged.** Reproduction is
  `go test -run X -seed N`; if the seed isn't logged, the harness is
  broken.
- **A flake found in CI is filed as a P0 bug** with the seed, the test
  name, and the failure mode. It blocks merges until reproduced and
  fixed.
- **If a test is flaky because of the test (not the SUT), the test is
  rewritten or deleted.** Tests that "usually work" are negative-value
  — they erode trust in the suite.

This matches REQUIREMENTS.md CHAOS-07 verbatim. The rationale is that
in a consensus protocol, "rare under chaos" and "real correctness bug"
are the same thing 99% of the time — retrying hides the very bug the
chaos suite exists to find.

### Operational mechanics

- CI prints `seed=N` on every chaos test start.
- On failure, CI uploads `test/artifacts/<test-name>/<seed>/*` (logs,
  message traces, linearizability HTML if applicable).
- A failed seed is committed back into the suite as a regression test
  (`test/chaos/seeds/<test-name>.txt`) so the same seed runs forever.

---

## 5. Linearizability harness

Linearizability is the v1 acceptance gate (PROJECT.md). The harness:

- **Tool:** Porcupine (`github.com/anishathalye/porcupine`).
- **Model:** KV register (Get/Put/Delete returning the last value or
  not-found). Defined in `test/linearizability/model.go`.
- **Driver:** Concurrent clients issue ops to the leader (via WIRE.md
  `/kv/{key}` endpoint or the in-process equivalent), record
  `(op, response, start_ts, end_ts)`, feed history to Porcupine.

### Output artifact

On a linearizability violation, the harness writes an HTML
visualization to `test/artifacts/linearizability/<seed>.html` (LIN-03).
This is the artifact a maintainer opens to debug a violation — it
shows the call timeline, the proposed sequential order Porcupine
tried, and the operation that broke linearizability.

The HTML is uploaded as a CI artifact on failure.

### Required CI scenarios

Three scenarios are required to run on every CI build (LIN-04):

| Scenario                  | Setup                                                                                                          | Asserts                                                |
| ------------------------- | -------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| **Steady-state**          | 3-node cluster, no faults, 5 clients, 1000 ops total                                                            | History is linearizable; ≥ 900 ops succeeded           |
| **Leader-churn**          | 3-node cluster, kill the leader every 500ms (FakeClock), 5 clients, 30 s logical time                          | History is linearizable; ≥ 5 leader changes occurred   |
| **Packet-loss**           | 3-node cluster, `p_drop = 0.2`, `MaxDelay = 50ms`, 5 clients, 30 s logical time                                | History is linearizable; ≥ 100 ops committed           |

Each runs against at least three distinct seeds in CI; flake = P0
(§4).

### Scripted Figure 7 + Figure 8

LIN-05 requires that the canonical hard cases are scripted (not left
to random chance):

- **Figure 7** — six log states from §5.4 of the Raft paper, each
  asserting that a node missing committed entries can never win an
  election.
- **Figure 8** — the previous-term-commit scenario from §5.4.2: S1
  leader term 2 replicates to S2, crashes; S5 leader term 3 with own
  uncommitted entry, crashes; S1 returns leader term 4; the term-2
  entry must NOT be committed merely because a majority has it.

These live under `test/linearizability/scripted/` and run on every CI
build (not gated behind a seed).

### Hand-written second checker (optional, ADR-0010 forward-ref)

`research/PITFALLS.md` recommends a hand-written register checker
alongside Porcupine ("two checkers catch checker bugs"). Implementing
this is a Phase 12 decision (ADR-0010); if it lands, it runs against
the same histories.

---

## 6. Fuzz targets

`go test -fuzz=Fuzz... -fuzztime=30s` runs clean on these targets in
CI:

| Target                                         | Location                       | Asserts                                                                                                  |
| ---------------------------------------------- | ------------------------------ | -------------------------------------------------------------------------------------------------------- |
| RPC JSON parser (TRAN-05)                      | `pkg/transport/http/fuzz_test.go` | No panic on arbitrary input; well-formed messages round-trip; malformed input returns a typed parse error |
| `Log.Match(prevIdx, prevTerm)` predicate       | `pkg/raft/log_fuzz_test.go`    | No panic; result is deterministic for the same log + inputs                                              |
| `Log.Term(idx)` predicate                      | `pkg/raft/log_fuzz_test.go`    | No panic; returns 0 for indices before the log base; matches the entry term for in-range indices         |

`-fuzztime=30s` is the per-target CI budget; longer fuzz runs are
nightly (Phase 11 extends this with longer budgets via `go test -fuzz`
in a scheduled job).

---

## 7. Positive oracles

Every chaos test asserts **positive** outcomes — not merely "no errors
raised" (CHAOS-05 / PITFALL D-5).

| Scenario                | Positive assertion                                                              |
| ----------------------- | ------------------------------------------------------------------------------- |
| Leader-churn            | `assert leaderChanges >= 5`                                                     |
| Steady-state            | `assert committed >= 900`                                                       |
| Packet-loss             | `assert committed >= 100 && partitions >= 1 && partitionsHealed >= 1`           |
| Process-kill            | `assert restartedNodes >= 1 && reconverged == true`                             |
| Netns                   | `assert partitionApplied == true && committedAfterHeal > committedAfterCut`    |

Rationale: a chaos test that runs for 10 minutes, never crashes, and
also never commits anything is a green test that proves nothing.
Positive oracles prevent that failure mode. If a future scenario can't
be expressed with a positive oracle, it doesn't belong in the suite.

---

## 8. `goleak` baseline

Every test in `pkg/raft`, `internal/raftest`, `pkg/transport/inproc`,
`pkg/transport/http` runs under a `goleak` baseline check in
`TestMain`:

```go
func TestMain(m *testing.M) {
    goleak.VerifyTestMain(m)
}
```

After every test, `runtime.NumGoroutine()` delta from baseline must be
0 (modulo runtime-owned goroutines that `goleak` whitelists). This
catches C-4 (goroutine leaks on shutdown) immediately, not three
months later in production.

The list of allowed long-lived goroutines is in
`docs/CONCURRENCY.md` §1 — anything outside that list is a leak.

---

## Cross-references

- `docs/CONCURRENCY.md` §1 (goroutine census), §5 (channel ownership),
  §6 (shutdown ordering) — the contract `goleak` verifies.
- `docs/LLD.md` §Clock, §Transport, §Storage — interfaces the harness
  exercises.
- `docs/WIRE.md` — HTTP/JSON envelope the fuzz parser target operates
  on.
- `research/PITFALLS.md` §Testing (T-1..T-9) and §Demo / Chaos (D-1..D-5)
  — exhaustive list of test pitfalls this contract prevents.
- `.planning/REQUIREMENTS.md` — TEST-02, CHAOS-05, CHAOS-07, LIN-03,
  LIN-04, LIN-05, TRAN-05.
- Future ADR-0009 — deterministic chaos harness with seed-driven
  reordering / duplication / drops / delays.
- Future ADR-0010 — linearizability via Porcupine (+ optional
  hand-written register checker).
