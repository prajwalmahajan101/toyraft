# 0011 — Mirror the replicated log into the local Storage subset

**Status:** Accepted
**Date:** 2026-06-29
**Scope:** `pkg/raft` (`proposeLocked` in `leader.go`; `mirrorLogWriteLocked`
+ the `AppendEntries` receiver in `append_entries.go`)

## Context

Through Phase 5 the node used its local `pkg/raft.Storage` subset (ADR-0005
freeze; the duplication noted in LLD §3 and `.journal/M5.md`) for
**HardState only**. The replicated log lived in `n.log`, an in-memory
structure, and was never written to `Storage`.

That left **REPL-09** — "a node MUST NOT emit an outbound RPC whose
correctness depends on durable state until that state is persisted" — with
**no code path on the log side** (Pitfall 8). The append-then-respond
obligation for log entries (the entry-side half of **P0-4**, distinct from
the vote-side half already retired in Phase 5) was *vacuous*: there was
nothing on disk to order the response against. A follower could acknowledge
`MatchIndex = M` in an `AppendEntriesResponse`, the node could crash, and on
restart those `M` entries would be gone — yet the leader had already
counted them toward a quorum commit. P0-4 was real but unprovable.

References: LLD §5 invariant 1 (fsync ordering / REPL-09), LLD §3 (local
`pkg/raft.Storage` subset, Phase-7-reconciliation note), PITFALLS P0-4
(append-precedes-response) + Pitfall 8 (vacuous durability),
`pkg/raft/leader.go::proposeLocked`,
`pkg/raft/append_entries.go::mirrorLogWriteLocked`,
`internal/raftest/ordering_storage.go`
(`AssertAppendPrecedesAppendEntriesResponse`),
`pkg/raft/ready_assert_debug.go::assertAppendPrecedesAEResponseLocked`,
ADR-0005 (Storage interface freeze), `.journal/M5.md` (known divergence).

## Decision

Every replicated-log write is mirrored into the local `pkg/raft.Storage`
subset **in lockstep** with the in-memory `n.log`, and the mirror completes
**before** any outbound response that claims those entries:

1. **Leader `proposeLocked`** appends the proposed entry to `n.log`, then
   mirrors it into `n.storage.Append` **before** marking it locally
   replicated (`matchIndex[self]`). On a mirror error it rolls the
   in-memory append back (`TruncateSuffix`) and returns `(0, false)` — it
   never advertises durability it has not achieved.
2. **The `AppendEntries` receiver** mirrors its truncate+append via
   `mirrorLogWriteLocked` **before** queuing the `Success`
   `AppendEntriesResponse` (the P0-4-final ordering). A mirror failure
   replies `Success = false`; the follower never claims an unpersisted
   `MatchIndex`.

`n.log` remains the fast in-memory **read** path; `n.storage` is the
durable **mirror**, written on every `Append` / `TruncateSuffix`.

We explicitly **DEFER** reconciling the `pkg/raft.Storage` subset against
the canonical `pkg/storage.Storage` to **Phase 7's ADR**. The Phase-5
known-divergence note (LLD §3, `.journal/M5.md`) carries forward unchanged:
`pkg/storage` imports `pkg/raft` for the shared types per ADR-0005, so
`pkg/raft` cannot import `pkg/storage.Storage` directly. This ADR ratifies
*what the log writes through* (the local subset), not *how the two Storage
views are unified* (Phase 7).

## Consequences

**Positive**

- **REPL-09 / P0-4 become real and provable.** Append-precedes-response is
  proven by `OrderingStorage.AssertAppendPrecedesAppendEntriesResponse`
  (positive **and** a non-vacuous negative test, Pitfall 8) plus the
  raftdebug `assertAppendPrecedesAEResponseLocked` Ready-invariant
  (a `Success` AE response's `MatchIndex <= n.log.LastIndex()`). The
  entry-side half of P0-4 is retired.
- **Failure declines durability rather than hiding it.** Both the leader
  (rollback + `(0,false)`) and the follower (`Success=false`) refuse to
  advertise entries they could not persist — no failure-masking fallback.
- The three-layer ordering proof (driver discipline + raftdebug invariant +
  `OrderingStorage` event-log) generalises the Phase-5 SC5 HardState-before-vote
  pattern to log entries at zero new architectural cost.

**Negative**

- The `pkg/raft.Storage` vs `pkg/storage.Storage` duplication **persists
  one more phase**. The mirror writes through the local subset, doubling
  the conceptual "where does the log live" surface until Phase 7 unifies it.
- Each log write now costs an extra `Storage` call under the single mutex
  (ADR-0004). At ToyRaft's scale (3–5 nodes) this is within budget; a
  high-throughput implementation would batch.
- `commitIndex` persistence stays best-effort / unasserted (Pitfall 7) —
  only **log-entry** durability is the hard P0-4 obligation; `commitIndex`
  is recoverable from the log on restart.

**Follow-ups**

- **Phase 7 ADR** reconciles the two `Storage` interfaces: either move the
  shared Raft types to a leaf package both import, or canonicalise the
  local subset as the Raft-side view. Tracked in LLD §3 and carried from
  `.journal/M5.md`.
