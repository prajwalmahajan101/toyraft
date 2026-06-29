# 0010 — Current-term commit rule (Raft Figure 8 fix)

**Status:** Accepted
**Date:** 2026-06-29
**Scope:** `pkg/raft` (`maybeAdvanceCommitLocked` in `commit.go`; leader
replication response path in `leader.go`)

## Context

Phase 6 closes the replication loop: a leader fans out `AppendEntries`,
collects `matchIndex` per peer on each response, and advances `commitIndex`
once a quorum has replicated an entry. The naive rule — "commit index `N`
as soon as a majority of `matchIndex >= N`" — is the textbook Raft mistake
that Raft §5.4.2 / Figure 8 exists to warn against.

Under leader churn it loses committed entries. A leader from an earlier
term `T1` can observe a quorum `matchIndex` for an entry it replicated at
`T1`, conclude that entry is committed, and acknowledge it to a client —
only for a later leader to overwrite that index with a different entry,
because the §5.4 election restriction does not by itself forbid a
sufficiently up-to-date candidate from winning and truncating a
not-yet-current-term-committed prefix. This is the phase risk **P0-1**
(Figure 8): a committed entry must never be lost or overwritten.

A second, narrower trap lives in the quorum computation itself
(**REPL-08 / C-8**): computing the quorum index by iterating the
`matchIndex` map in Go's randomised map-iteration order yields a
non-deterministic commit index across runs with identical inputs,
defeating seeded chaos replay (`TestNoLogDivergence_Chaos`).

References: Raft §5.4.2 (Figure 8), LLD §5 (global invariants), PITFALLS
P0-1 (Figure 8) + C-8 (map-iteration determinism), REPL-04 / REPL-06 /
REPL-08, `pkg/raft/commit.go::maybeAdvanceCommitLocked`,
`pkg/raft/figure8_test.go::TestFigure8`, ADR-0004 (single mutex),
ADR-0008 (step/Ready event loop).

## Decision

A leader advances `commitIndex` to `N` only when **both** hold:

1. a majority of nodes report `matchIndex >= N` (counting the leader's own
   `LastIndex()`), **and**
2. `log[N].term == currentTerm`.

Entries from a prior term are **never** committed by replica count alone;
they commit **indirectly** once an entry from the leader's current term
commits above them (Raft §5.4.2). A freshly elected leader therefore
cannot advance `commitIndex` until it has appended and replicated at least
one entry of its own term (**P1-2**).

The quorum index is computed from a **`slices.Sort`'d snapshot of the
`matchIndex` values** (REPL-08 / C-8) — never by map-iteration order. The
leader's own position is substituted as `LastIndex()` into the sorted
snapshot for the quorum scan only; it does **not** mutate `matchIndex[self]`
(Pitfall 6 — avoids a redundant self-write that other implementations
carry).

## Consequences

**Positive**

- **Safety under churn.** A committed entry is never lost or overwritten —
  proven end-to-end by `TestFigure8` (the scripted §5.4.2 churn sequence)
  and held every tick across the 1000-seed `TestNoLogDivergence_Chaos`
  sweep (REPL-10 no-lost-commit invariant). P0-1 retired.
- **Deterministic commit index.** The sorted-snapshot quorum makes the
  commit index a pure function of the `matchIndex` multiset, so seeded
  chaos replay is byte-reproducible (C-8 retired, `TestQuorumSortedNotMapOrder`).
- **Self-evident liveness contract.** "A fresh leader must append a
  current-term entry before it can commit" (P1-2) is encoded directly in
  the commit rule rather than scattered across handlers.

**Negative**

- A new leader cannot immediately commit entries it inherited from a prior
  term, even if they are already on a quorum of disks. In practice leaders
  append a no-op (or the next client entry) on election; ToyRaft surfaces
  this through `TestFreshLeaderDoesNotCommit` rather than auto-appending a
  no-op (the auto no-op is a Phase 7 driver concern).
- The current-term check means a leader must read `log[N].term`, adding a
  log lookup to the commit-advance path. At ToyRaft's scale this is free.

**Follow-ups**

- None for v1. Fast log back-tracking on `AppendEntries` rejection
  (`ConflictTerm` / `ConflictIndex`, **REPL-04**) is deferred to a v1.1
  ADR; the current decrement-and-retry probe (floored at index 1) is
  correct, just not optimal.
