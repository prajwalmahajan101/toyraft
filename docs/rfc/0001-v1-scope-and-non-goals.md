# RFC 0001 — v1 Scope and Non-goals

**Status:** Accepted
**Author:** project owner
**Date:** 2026-06-18
**Tracking issue:** N/A (seed RFC — locks the v1 contract at project
start; no upstream issue)

## Summary

This RFC locks the **v1 contract** for ToyRaft. Anything not listed in
the v1 Active requirements (`.planning/REQUIREMENTS.md` §v1) is
deferred to v2 or later. Subsequent phases — and any subsequent
proposal — **cannot widen v1 scope without a new RFC** that
supersedes the relevant section here. This RFC is the lock; the only
way to undo it is to write another one.

## Motivation

ToyRaft is a documentation-heavy, contract-first project: 12 spec
documents land in Phase 1 before a single `feat:` commit (`DOC-15`).
The trilogy sibling projects (`toykv`, `toymq`) both shipped
v1s in ~5k LOC by holding the line on scope; ToyRaft must do the same
or it won't ship.

**Why lock scope before code:**

1. **Prevents scope creep.** During implementation phases, the
   shortest path between "I want feature X" and "feature X ships" is
   "write a v2 RFC and let the discussion run." That asymmetry is
   intentional.
2. **Forces v2 ideas to incubate.** Snapshots, membership changes,
   fast-rollback, and TLS each have well-formed v2 buckets in
   `REQUIREMENTS.md` (SNAP-*, MEMB-*, PERF-*, PROD-*). Pulling one
   forward "because it's not that hard" is exactly how etcd v1 painted
   itself into a wire-incompatible corner (see Prior Art).
3. **Keeps the trilogy coherent.** `toykv-cluster` and `toymq-cluster`
   are the consumer integration target. They depend on a ToyRaft v1
   contract that doesn't shift under them mid-integration.
4. **Reviewers have an objective bar.** "Does this widen v1 scope?" is
   a binary question against this RFC. PR reviews don't need to
   relitigate the scope discussion every time.

## Detailed design

### v1 is exactly this

The v1 surface is the union of every checked-by-design requirement in
`.planning/REQUIREMENTS.md` §v1. Briefly:

| Bucket                    | What ships in v1                                                                                 | Requirements        |
| ------------------------- | ------------------------------------------------------------------------------------------------ | ------------------- |
| Consensus core            | Leader election, log replication, term safety, Figure-7/8 correctness                            | `FOUND-*`, `ELEC-*`, `REPL-*` |
| Storage                   | `Storage` interface; `memory` impl; `file` impl with fsync + atomic rename + torn-tail recovery | `STOR-01..STOR-07`  |
| Transport                 | HTTP/JSON + in-process Hub; both behind the same `Transport` interface                           | `TRAN-01..TRAN-06`  |
| Library API               | `Node`, `StateMachine`, `Status`, sentinel errors; `slog`-only logging; bounded ApplyChannel    | `API-01..API-10`    |
| Reference demo            | `pkg/kvsm`, `cmd/toyraftd`, `cmd/toyraftctl`, `make demo`                                       | `DEMO-01..DEMO-09`  |
| Layered testing           | Unit + seeded chaos + linearizability (Porcupine) + netns chaos                                  | `TEST-*`, `CHAOS-*`, `LIN-*` |
| Observability + release   | `slog` + `expvar` + `/status` + GoReleaser for `toyraftd`                                       | `OBS-*`, `QUAL-*`   |
| Documentation + process   | 12 specs + ADR/RFC/journal/PR templates                                                          | `DOC-*`, `PROC-*`   |

### v1 is explicitly NOT this

Mirrored from `.planning/REQUIREMENTS.md` §Out of Scope +
`.planning/PROJECT.md` §Out of Scope, with each non-goal linked to its
v2 requirement bucket so the deferral has a forward home.

| Non-goal                                          | v2 bucket                          | Why deferred (full rationale below)                                              |
| ------------------------------------------------- | ---------------------------------- | -------------------------------------------------------------------------------- |
| Snapshots / log compaction                        | `SNAP-01`, `SNAP-02`, `SNAP-03`, `SNAP-04` | Meaningful complexity; v1 stubs interface methods so v2 can add without break    |
| Cluster membership changes (joint consensus)      | `MEMB-01`, `MEMB-02`, `MEMB-03`    | Joint-consensus design is its own correctness problem; needs separate RFCs       |
| Fast-rollback `AppendEntries` (leader-side)       | `PERF-01`                          | Wire fields reserved in v1; leader optimisation deferred — slow probe is enough  |
| Batched / pipelined replication                   | `PERF-02`, `PERF-03`               | Need a baseline to optimise against; basic v1 AppendEntries is enough            |
| ReadIndex / lease reads (linearizable reads)      | `PERF-04`                          | v1 redirects all reads to the leader (DEMO-04, 307); RFC needed to relax         |
| TLS / mTLS between nodes                          | `PROD-01`                          | localhost-only trust model in v1 (SECURITY.md); needs deployment story           |
| Pre-vote (avoid disruptive elections)             | `PROD-02`                          | Optimisation, not a correctness requirement                                      |
| CheckQuorum (leader steps down on isolation)      | `PROD-03`                          | Optimisation; v1 timeout-based step-down is sufficient                           |
| Leadership transfer API                           | `PROD-04`                          | Convenience API; not on v1 critical path                                         |
| gRPC / protobuf transport                         | (no v2 bucket — possibly never)    | Breaks stdlib-only aesthetic; `Transport` interface keeps the door open          |
| Multi-Raft / sharding                             | (separate project — `toyshard`)    | Different problem domain                                                         |
| Shared `toywire` protocol with toymq/toykv        | (trilogy-coherence work post-v1)   | Tempting but creates cross-project coupling without value at v1                  |
| Reflection-based FSM (`interface{}` entries)      | (no v2 bucket)                     | `Entry.Data` is `[]byte`; consumer encodes — keeps the core simple               |
| Third-party logger imposed on consumers           | (no v2 bucket)                     | `slog` only, silent by default                                                   |
| Built-in metrics push (Prometheus / OTel)         | (no v2 bucket)                     | `expvar` is enough for the demo; consumers can wrap                              |
| Web UI for cluster inspection                     | (no v2 bucket)                     | `toyraftctl status` + `/status` JSON cover the demo                              |
| Encrypted-at-rest log                             | (no v2 bucket; tied to `PROD-01`)  | localhost-only trust model                                                       |

## Drawbacks

- **A locked v1 contract makes "small improvements" harder.** Any
  contributor who notices, mid-Phase-6, that fast-rollback would be
  ~50 lines must instead write `RFC 0002` and wait for discussion.
  That friction is intentional — it's the whole point of this RFC —
  but it is friction.
- **Some non-goals will look obviously wrong in hindsight.** Locking
  before code means we're locking based on `research/` predictions,
  not battle-tested implementation experience. The mitigation: every
  non-goal has a v2 bucket; nothing is lost, only deferred.
- **The RFC itself is now a thing to maintain.** If
  `REQUIREMENTS.md` §Out of Scope changes, this RFC is stale by
  Working Agreement 4 (specs are source of truth). Mitigation: this
  RFC is short, and any change to the out-of-scope table is *itself*
  the kind of substantive proposal that requires a new RFC anyway.

## Rationale and alternatives

**Why a locked-scope RFC at all, vs. just trusting the requirements
doc?** Because `REQUIREMENTS.md` is a checklist; it doesn't carry the
*why*. Six months in, when someone proposes adding pre-vote,
`REQUIREMENTS.md` says "PROD-02: deferred." This RFC says *why*: it's
an optimisation, not a correctness fix, and the goal is v1
correctness over v1 polish.

**Per-item rationale (the "why deferred"):**

- **Snapshots / SNAP-* (deferred).** Snapshots add three pieces of
  complexity simultaneously: a state-machine serialisation contract
  (`Snapshot()` / `Restore()`), a new wire RPC (`InstallSnapshot`),
  and storage compaction (pruning log entries below the snapshot
  index). Each is independently valuable, but pulling them forward
  before v1 election + replication correctness is stable would make
  every chaos failure ambiguous between "consensus bug" and "snapshot
  bug." v1 mitigation: `Storage` interface has `Snapshot` /
  `Restore` stubs that return `ErrSnapshotUnsupported` (`STOR-01`).
  Adding v2 snapshots is purely additive.

- **Membership changes / MEMB-* (deferred).** Joint consensus has its
  own correctness theorems (Raft paper §6) and its own chaos surface.
  Doing it right requires separate RFCs for the configuration-change
  protocol, learner role, and operational story. None of that work
  blocks demo-cluster correctness, but trying to bolt it on after
  freezing membership is harder than starting with a fixed cluster
  and adding a v2 RFC.

- **Performance optimisations / PERF-* (deferred).** Pre-mature
  optimisation in a correctness-first project is actively harmful —
  it makes regressions ambiguous (was that a slowdown, or a
  correctness regression masquerading as throughput change?). v1 ships
  a baseline; v2 RFCs add optimisations with before/after numbers
  against that baseline. Special case: fast-rollback wire fields
  (`ConflictTerm` / `ConflictIndex`) **do** ship in v1's `Message`
  schema so the wire format is forward-compatible; only the
  leader-side optimisation is deferred (`REPL-04`).

- **Production hardening / PROD-* (deferred).** TLS, pre-vote,
  CheckQuorum, and leadership transfer are all things a real
  deployment needs. ToyRaft v1 is explicit that it is *not* a real
  deployment — `docs/SECURITY.md` documents the localhost-only trust
  model. v2 production hardening will be a coherent package, not
  scattered patches.

**Alternative: ship v1 in two flavours (stripped + full).** Rejected.
Maintaining two versions doubles the test matrix and undermines the
"correct, understandable" goal.

**Alternative: no scope lock, just discipline.** Rejected. The whole
trilogy ships precisely because the discipline is codified.

**Impact of not locking:** v1 ships ~6 months later, with twice the
LOC, and with the same correctness bar reached via a longer chaos
debugging slog. Not catastrophic, but not the path.

## Prior art

**etcd v1 → v3 scope creep lesson.** etcd v1 (2014) shipped with a
JSON-over-HTTP API. v2 (2015) added a richer key-space model
incrementally. v3 (2016) introduced a **wire-incompatible gRPC API**
because the v1/v2 surface couldn't carry the features v3 needed
(transactions, multi-key watches, leases) without breaking changes.
The lesson: features pulled into a v1 surface that wasn't designed for
them become permanent technical debt; the only fix is a wire break.

ToyRaft mitigates this by:

1. **Stubbing the snapshot methods in `Storage`** (`STOR-01`,
   `ErrSnapshotUnsupported`) so adding v2 snapshots is additive, not
   breaking.
2. **Reserving fast-rollback wire fields** (`ConflictTerm`,
   `ConflictIndex`) in v1's `Message` schema even though the leader
   doesn't use them — see `REPL-04` and `docs/WIRE.md`.
3. **JSON forward-compat rule** (`docs/WIRE.md`): unknown fields are
   ignored; new `MessageType` values are additive. v2 additions
   don't require a v2 endpoint.

**hashicorp/raft and etcd/raft as reference (not template).** Both
have learner roles, pre-vote, snapshots, batched replication, and
leadership transfer. ToyRaft v1 sees them as the v2 target shape, not
the v1 surface — copying their v1 vintage (≈2014) is the goal.

**Trilogy sibling precedent.** `toykv` and `toymq` both shipped v1
under explicit "what's intentionally skipped" sections in their
READMEs. Both deferred persistence variants, auth, and clustering to
v2 buckets that — so far — haven't blocked downstream adoption. The
pattern works at this scale.

## Unresolved questions

None. **This RFC IS the lock**, which is the opposite of a
question-driven document. If something needs resolving, it doesn't go
here — it goes in its own RFC.

## Future possibilities

The v2 buckets in `.planning/REQUIREMENTS.md` are the future-work
inbox:

- `SNAP-01..SNAP-04` — snapshots + log compaction.
- `MEMB-01..MEMB-03` — membership changes + learner role.
- `PERF-01..PERF-04` — fast-rollback (leader-side), batched +
  pipelined replication, ReadIndex/lease reads.
- `PROD-01..PROD-04` — TLS/mTLS, pre-vote, CheckQuorum, leadership
  transfer.

Each v2 bucket is expected to spawn at least one RFC. Discussion
happens in those RFCs, not here. `.planning/ROADMAP.md` tracks v1
phases; v2 work will get its own roadmap once v1 is shipped and the
toykv-cluster + toymq-cluster integrations have provided real-world
feedback.

## References

- `.planning/PROJECT.md` §Out of Scope — the canonical out-of-scope
  list this RFC mirrors.
- `.planning/REQUIREMENTS.md` §v1 / §v2 / §Out of Scope — the bucket
  identifiers used throughout.
- `.planning/research/SUMMARY.md` §Phase Sequence — why v1 ships in
  14 phases.
- `docs/PRD.md` — non-goals table (same content, consumer-facing
  framing).
- `docs/SECURITY.md` — localhost-only trust model (justifies
  `PROD-01` deferral).
- `docs/WIRE.md` — forward-compat rule (justifies additive v2 wire
  changes without a v2 endpoint).
