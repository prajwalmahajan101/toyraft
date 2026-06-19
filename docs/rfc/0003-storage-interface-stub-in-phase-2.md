# RFC 0003 — Stub Storage Interface in Phase 2

**Status:** Accepted
**Author:** project owner
**Date:** 2026-06-20
**Tracking issue:** N/A (executed under plan `02-07-PLAN.md`)

## Summary

Declare the LLD §3 `Storage` / `LogStorage` / `StateStorage` interfaces in
`pkg/raft/storage.go` during Phase 2, with **stub-only** method bodies that
return `ErrStorageNotImplemented`. The interface shape is frozen here, byte-
for-byte from `docs/LLD.md §3`. Phase 3 inherits the unchanged shape and
supplies the real in-RAM implementation in `pkg/storage/memory`, plus the
`pkg/storage/storagetest` conformance harness, plus snapshot stubs
(`Snapshot` / `Restore` returning `ErrSnapshotUnsupported`) — none of which
this RFC pre-empts.

This RFC enters Phase 3 territory by exactly one file and zero behaviour.
A user CHECKPOINT in plan 02-07 gates code commits; if rejected, the
fallback shrinks the deliverable to a single `ErrStorageNotImplemented`
sentinel inside `pkg/raft/types.go` and waits for Phase 3 to declare the
interfaces.

## Motivation

The ROADMAP assigns the `Storage` interface declaration to Phase 3 (see
ROADMAP "Phase 3: Storage Interface + Memory Impl"). That phase's load-
bearing deliverable is implementation quality — the `storage/memory` impl
plus the reusable `storagetest` conformance suite. Declaring the
interface alongside `pkg/raft/types.go` (plan 02-01) during Phase 2 buys
us three concrete things:

1. **Compile-checked references.** `pkg/raft/log.go` (plan 02-03) and the
   Phase-5 election work both reference `Storage` symbolically. With the
   interface in tree from Phase 2, doc comments and (later) function
   signatures resolve to a real symbol instead of dangling text.
2. **Phase 5's durable-vote invariant has a symbol to compile against.**
   LLD Global Invariant 1 (`fsync ordering`, REPL-09) and LLD §3's
   `SaveHardState` doc comment together require that election code calls
   `Storage.SaveHardState` BEFORE emitting a vote response. Having
   `SaveHardState` declared in Phase 2 means Phase-5 plans can stage
   compile checks (`var _ Storage = ...`) without circular phase
   dependencies.
3. **Phase 3 narrows to impl quality.** Phase 3's success criteria — full
   conformance suite, `storage/memory` correctness under chaos, snapshot-
   stub forward-compat — are exactly the work that benefits from
   uncontested attention. Separating interface declaration (Phase 2) from
   implementation (Phase 3) means Phase 3's review surface is purely
   semantic.

Working Agreement 4 (ROADMAP preamble) makes `docs/LLD.md` the byte-for-
byte source of truth. Plan 02-07 Task 1 extracts LLD §3 to
`/tmp/lld-section-3.txt` BEFORE this RFC was drafted, so the interface
shape recorded below is what LLD §3 actually declares — not what ROADMAP
summarises.

## Detailed design

### File and package

A new file `pkg/raft/storage.go` is created. Package declaration:
`package raft`. This matches the `pkg/raft/types.go` placement of the
related public types (`Entry`, `HardState`, `Index`, `Term`, `NodeID`)
and avoids creating the `pkg/storage/...` sub-package tree (which Phase 3
owns).

### Interface shape (byte-for-byte from LLD §3)

LLD §3 declares three interfaces:

- `LogStorage` — six methods: `Append`, `TruncateSuffix`, `Entries`,
  `Term`, `FirstIndex`, `LastIndex`.
- `StateStorage` — two methods: `SaveHardState`, `LoadHardState`.
- `Storage` — composes the two via embedding (`Storage interface {
  LogStorage; StateStorage }`).

The Phase-2 stub transcribes every signature and doc comment verbatim.
The drift check in plan 02-04 (`go doc -all ./pkg/raft` golden) is the
post-merge guardrail; this RFC + the byte-for-byte transcription is the
pre-merge one.

### Snapshot methods — deliberately omitted

LLD §3 does NOT declare `Snapshot` or `Restore` methods on `LogStorage`
or `StateStorage`. Snapshot stubs live on `StateMachine` (LLD §3.2) and
the v1 `ErrSnapshotUnsupported` sentinel lives in `pkg/storage` (per LLD
§4's commentary: "lives in `pkg/storage` ... re-exported from
`pkg/raft`"). Phase 3 owns:

- The `pkg/storage` package creation.
- The `ErrSnapshotUnsupported` sentinel declaration.
- Any snapshot hooks on `Storage` if v1 grows them.

This RFC explicitly **does not** declare `ErrSnapshotUnsupported` in
`pkg/raft/storage.go`. Phase 3 introduces it as part of its own scope so
the package boundary recorded in ROADMAP holds.

### Sentinel

A single sentinel is declared in `pkg/raft/storage.go`:

```go
var ErrStorageNotImplemented = errors.New("raft/storage: not implemented in phase 2 (see RFC-0003)")
```

Every stub method returns this sentinel. The deliberate consequence is
that any production code path that accidentally invokes the Phase-2 stub
fails loudly. LoadHardState returning `(HardState{}, ErrStorageNotImplemented)`
in particular guards the P0-4 footgun the LLD warns about (a nil error
from `LoadHardState` would let elections proceed without a persisted
vote).

### Stub implementation

A private `stubStorage` type implements every method, each body being
exactly one `return ErrStorageNotImplemented` line (plus zero values for
the non-error returns). A compile-time assertion
(`var _ Storage = (*stubStorage)(nil)`) catches drift if the interface
grows a method but the stub isn't updated.

The stub is NOT exported and NOT wired into any consumer in Phase 2.
Phase 3's `pkg/storage/memory` replaces both the stub and (potentially)
relocates the interface declaration; either path is signature-preserving.

### Commit shape

Two atomic commits on `feature/foundations`:

1. `docs(rfc): RFC-0003 stub Storage interface in Phase 2` — this file.
2. `feat(raft): stub Storage interface (RFC-0003)` — only after the
   plan 02-07 Task 2 CHECKPOINT receives `approve-stub`.

If the CHECKPOINT receives `shrink-sentinel-only`, the second commit is
replaced by a single sentinel line added to `pkg/raft/types.go` and this
RFC is amended to Status: Rejected with a Resolution section pointing at
the fallback commit.

## Drawbacks

- **Boundary crossing.** ROADMAP Phase 3 explicitly owns the Storage
  interface declaration. Crossing the boundary, even by one stub file,
  costs a paper-trail (this RFC) and a drift check (plan 02-04). The
  alternative — leaving Phase-2 doc comments pointing at a non-existent
  type — costs reviewer cognitive load on every Phase-2 PR.
- **Two-file review surface in Phase 3.** Phase 3 must verify the
  interface shape didn't drift between Phases 2 and 3, and decide
  whether to keep the declaration in `pkg/raft/storage.go` or relocate
  it to `pkg/storage/storage.go`. Both options are signature-preserving
  by construction, but Phase 3 will need to make the call.
- **Sentinel co-located with unrelated Phase-2 types.** `pkg/raft` now
  hosts `ErrStorageNotImplemented` alongside the consensus public types.
  This is acceptable — `pkg/raft` is the package consumers import — but
  it does broaden the package's surface area slightly ahead of schedule.

## Rationale and alternatives

**Wait for Phase 3.** Rejected. Forces Phase-2 plans (02-03 Log doc
comments, future Phase-5 election prep) to reference a non-existent
type. Increases coupling between Phase 2 closure and Phase 3 kickoff.

**Declare the interface but return `nil` from stub methods.** Rejected.
A nil return from `LoadHardState` would silently allow an election to
proceed against a zero-value `HardState` without a persisted vote —
exactly the P0-4 footgun LLD §5 warns against. The sentinel is what
makes the stub safe.

**Move the interface to a new `pkg/storage/storage.go` immediately.**
Rejected. Creates the sub-package Phase 3 owns and expands this RFC's
scope. Phase 3 may relocate the interface as part of its own work; this
RFC does not pre-empt that decision.

**Sentinel-only fallback (shrink path).** Acceptable. If user rejects
this RFC at the plan 02-07 Task 2 CHECKPOINT, `pkg/raft/types.go` gains
a single `var ErrStorageNotImplemented = errors.New(...)` line and the
interface declaration waits for Phase 3. This RFC's Status flips to
Rejected and a Resolution section records the fallback commit.

**Impact of doing nothing.** Phase-2 doc comments referencing `Storage`,
`LogStorage`, and `StateStorage` remain text rather than symbols. Phase
5's election plans defer compile-time checks of `SaveHardState`
signatures until Phase 3 lands. The project still ships; the friction is
spread across three phases instead of paid in one Phase-2 RFC.

## Prior art

- **etcd/raft `Storage` interface.** Declared in `raft.go` alongside the
  core types, not in a separate `storage` package. Snapshot methods
  (`Snapshot`, `ApplySnapshot`) live on the interface from day one. ToyRaft
  v1 deliberately omits snapshot methods from the interface (per RFC 0001,
  snapshots are v2); the snapshot stub lives on `StateMachine` instead.
- **hashicorp/raft `LogStore` + `StableStore` split.** Mirrors LLD §3's
  `LogStorage` + `StateStorage` split closely. ToyRaft's `Storage`
  composes the two via embedding — matches hashicorp's mental model
  while keeping a single concrete type to pass around.
- **RFC 0001 (this project).** Locks v1 scope to `STOR-01..STOR-07`
  including the snapshot-stub forward-compat rule. This RFC respects
  that boundary by not introducing `ErrSnapshotUnsupported` in Phase 2.
- **ADR-0004 (single-mutex policy).** All `Storage` calls execute under
  `Node.mu` in Phase 5+. The stub's no-op nature means ADR-0004's
  contention concerns are deferred to Phase 3's real impl.

## Unresolved questions

- **Should Phase 3 relocate the interface to `pkg/storage/storage.go`?**
  Deferred to Phase 3's planning. Both options preserve signatures.
- **Should the stub be exported?** No — `stubStorage` is unexported and
  not wired into any consumer in Phase 2. If a Phase-2 test ever needs
  a "compiles but doesn't run" Storage, it can use `(*stubStorage)(nil)`
  via the package-internal name. No external consumer needs this.

## Future possibilities

- Phase 3 supplies `pkg/storage/memory` and the `pkg/storage/storagetest`
  conformance harness against this exact interface shape. No interface
  change is required.
- Phase 8 supplies `pkg/storage/file` with fsync + atomic rename + torn-
  tail recovery, against the same harness. No interface change.
- v2 may add `Snapshot` / `Restore` methods to `LogStorage` or
  `StateStorage`. RFC 0001's snapshot-stub forward-compat rule (LLD
  Invariant 5, STOR-01) makes that purely additive.

## References

- `docs/LLD.md §3` ("Storage (LogStorage + StateStorage)") — byte-for-byte
  source of truth being stubbed. Heading text captured in
  `/tmp/lld-section-3.txt` ahead of this draft.
- `docs/LLD.md §4` — `ErrSnapshotUnsupported` lives in `pkg/storage` per
  the note "lives in `pkg/storage` ... re-exported from `pkg/raft`";
  this RFC respects that placement and does not pre-empt Phase 3.
- `docs/LLD.md §5` Global Invariant 1 (fsync ordering, REPL-09) — the
  P0-4 footgun the `ErrStorageNotImplemented` sentinel guards.
- `.planning/ROADMAP.md` "Phase 3: Storage Interface + Memory Impl" —
  the territory this RFC enters by one file.
- `.planning/phases/02-foundations/02-04-PLAN.md` — drift check
  (`depends_on` includes `02-07`, `wave: 4`); its `go doc -all`
  golden is generated after this plan lands and naturally includes the
  Storage symbols.
- `docs/adr/0004-single-mutex-state-machine.md` — single-mutex policy
  that Phase 3's real impl will operate under.
- `docs/rfc/0001-v1-scope-and-non-goals.md` — v1 scope lock and the
  snapshot-stub forward-compat rule (STOR-01).
