# ADR-0004 — Single mutex guards the Raft state machine

**Status:** Accepted
**Date:** 2026-06-20
**Scope:** `pkg/raft` (Node state machine + Log sub-component)

## Context

`docs/CONCURRENCY.md §2` opens with the rule that one `sync.Mutex` named
`mu` on the `Node` struct guards the entire core Raft state, and carries
the parenthetical: *"Forward reference: this is **ADR-0001** (to be
written in Phase 2 alongside the first implementation commit)."* Phase
1.1 subsequently inserted [ADR-0002] (bring CI forward) and [ADR-0003]
(golangci-lint configuration), so the next free slot for the
single-mutex decision is **0004**, not 0001. This ADR discharges that
forward reference; the CONCURRENCY.md back-patch lands in the same
commit.

The design question is concrete. A Raft node mutates a tightly-coupled
state bundle — `role`, `currentTerm`, `votedFor`, `log`, `commitIndex`,
`lastApplied`, `leaderID`, and the leader-volatile `nextIndex` /
`matchIndex` / `votesReceived` maps — and almost every operation
touches several of those fields in one read-modify-write. AppendEntries
receive, for example, reads `currentTerm` + `log` + `commitIndex` and
may write `log` + `commitIndex` + `currentTerm` + `votedFor` + `role` +
`leaderID` in a single critical section. Splitting `mu` into finer
locks (state / log / peer-progress / persistent-vote) imports the
entire C-1..C-8 pitfall catalogue from CONCURRENCY.md §7 — lock
inversion (C-1), TOCTOU on step-down (P0-5), lock-across-send (C-2),
and platform-dependent map iteration (C-8) — none of which buys
contention relief at v1's target load (3–5 nodes, single-digit
thousands of ops/sec).

The production references agree: `etcd-io/raft` serialises via channels
into a single goroutine (functionally equivalent to one lock);
`hashicorp/raft` uses a single mutex per Raft instance. The trilogy's
sibling toykv runs a single store-level mutex for the same reasons.
CONCURRENCY.md §2 already records the rationale; this ADR ratifies it
and pins the policy so future contributors cannot silently re-shard the
lock.

## Decision

We will guard **every** Raft state-machine field on `Node` with a
single `sync.Mutex` field `Node.mu`:

- `role`, `currentTerm`, `votedFor`, `log`, `commitIndex`, `lastApplied`,
  `leaderHint` / `leaderID`, `nextIndex`, `matchIndex`, `votesReceived`,
  `lastHeartbeat`.

Sub-components called from inside the state machine — notably
`pkg/raft/Log` — are **NOT internally synchronised**. Their exported
doc comments state explicitly: *"NOT safe for concurrent use; caller
holds Node.mu."* Plan 02-03 lands `pkg/raft/log.go` with this comment
verbatim, citing this ADR.

I/O (`Storage.Append`, `Storage.SaveHardState`, `Transport.Send`) is
**never** performed while `Node.mu` is held except where REPL-09
explicitly requires fsync-before-RPC-response inside the critical
section. Outbound RPCs follow the copy-under-lock pattern from
CONCURRENCY.md §4: snapshot the message under `mu`, release `mu`, then
send.

Tests on `pkg/raft/Log` in isolation MUST NOT call `t.Parallel()` while
sharing a `*Log`, because the type carries no internal synchronisation
by deliberate policy — parallelising such a test would violate this
ADR and silently pass under `-race` only because no concurrent caller
exists in production.

### Alternative considered: lock hierarchy

A finer-grained hierarchy of the form `state-mu → log-mu → storage-mu`
was considered and rejected. It imports lock-ordering proofs, requires
re-validating role and term after every inner lock release (TOCTOU),
and delivers no measurable contention benefit at v1 cluster sizes.
Revisiting this trade is a future ADR superseding 0004; silent
re-sharding of `mu` is forbidden by [ADR-0000]'s append-only status
rule.

## Consequences

**Positive**

- Eliminates entire classes of pitfalls from CONCURRENCY.md §7: C-1
  (lock inversion impossible — only one mutex), P0-5 (TOCTOU on
  step-down trivial — no inner lock to drop), C-6 (`time.Now()` race —
  `lastHeartbeat` lives under `mu`), C-8 (map-iteration order — peer
  IDs sorted under `mu` once at construction).
- Matches the mental model: Raft's safety properties (Election Safety,
  Leader Append-Only, Log Matching, Leader Completeness, State Machine
  Safety) all reason about transitions on the single state bundle. One
  mutex matches one bundle.
- `Log` stays a plain Go type with no `sync.*` field — easier to test,
  easier to reason about, and unit tests cannot be misused into
  parallel-sharing it (the doc comment forbids it).
- Sibling-project parity: aligns with hashicorp/raft (one mutex) and
  the toykv house style.

**Negative**

- Theoretical contention ceiling on Apply throughput: every RPC
  handler, every tick, every client proposal contends on `mu`.
  Mitigated by holding `mu` only for O(microseconds) and never across
  network I/O (CONCURRENCY.md §4). At v1 target load this is invisible.
- The Log API is awkward outside its intended caller: there is no way
  to use `Log` concurrently without the caller installing its own
  mutex. That is by design — the only intended caller is `Node`, and
  it already holds `mu`.

**Follow-ups**

- Plan 02-03 lands `pkg/raft/log.go` whose doc comment states *"NOT
  safe for concurrent use; caller holds Node.mu (ADR-0004)."*
- Phase 7 (`pkg/raft/node.go`) adds a build-tagged sanity check (build
  tag `raftdebug`) that `mu` is held on entry to state-machine
  methods. The build tag avoids paying for the check in release
  builds; it is not a separate ADR because it is implementation
  detail of enforcing this policy.
- If v2 benchmarks force finer-grained locking, the supersession lands
  as a new ADR per [ADR-0000]; this ADR is never amended in place.

## Usage

Every method on `Node` that touches a state-machine field acquires
`n.mu.Lock()` (or `n.mu.RLock()` only for the two pure-read fields
`currentTerm` and `role`, per CONCURRENCY.md §3). Every method on
`pkg/raft/Log` documents *"NOT safe for concurrent use; caller holds
Node.mu"* in its doc comment and cites this ADR.

When reviewing a PR that touches `pkg/raft`:

- A new `sync.Mutex` / `sync.RWMutex` field anywhere under `pkg/raft`
  is a red flag — it requires a superseding ADR, not a code review
  comment.
- A new `t.Parallel()` in a `pkg/raft/log_test.go` that shares a
  `*Log` across goroutines is a bug — flag and revert.
- An outbound `Transport.Send` inside a `Node.mu.Lock()` /
  `n.mu.Unlock()` pair is a bug per CONCURRENCY.md §4 — flag and
  revert.

## References

- `docs/CONCURRENCY.md` §2 (single-mutex policy), §3 (lock hierarchy),
  §4 (copy-under-lock pattern), §7 (C-1..C-8 pitfall catalogue).
- `docs/LLD.md` Global Invariant 1 (single-mutex policy is the source
  of truth for state-machine field access).
- `.planning/research/PITFALLS.md` §Concurrency P0-5, C-1, C-2, C-8.
- [ADR-0000] — record architecture decisions (status transitions are
  append-only; supersession lands as a new ADR).
- [ADR-0002] — bring CI forward to Phase 1.1.
- [ADR-0003] — golangci-lint configuration.

[ADR-0000]: ./0000-record-architecture-decisions.md
[ADR-0002]: ./0002-bring-ci-forward.md
[ADR-0003]: ./0003-golangci-lint-config.md
