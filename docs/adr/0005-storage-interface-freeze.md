# 0005 — Storage Interface Freeze (Phase 3)

**Status:** Accepted
**Date:** 2026-06-20
**Scope:** `pkg/storage`, `pkg/raft` (sentinel + storage-stub cleanup), `docs/LLD.md` §3/§4

## Context

Phase 2 (RFC-0003) landed a Storage **interface stub** in `pkg/raft/storage.go` to lock the LLD §3 shape early, with three decisions deliberately deferred to Phase 3:

1. **Interface home.** Does the `Storage` / `LogStorage` / `StateStorage` declaration stay in `pkg/raft` or relocate to `pkg/storage`?
2. **`ErrSnapshotUnsupported` declaration site.** LLD §1 / §4 sidebar says it "lives in `pkg/storage` ... re-exported from `pkg/raft`," but no impl yet exists.
3. **Snapshot/Restore on `Storage`.** ROADMAP Phase 3 SC1 says `go doc ./pkg/storage` MUST show `Storage.Snapshot` and `Storage.Restore`; LLD §3 currently distributes the snapshot pair to `StateMachine` only.

Constraints forcing a same-PR resolution:

- **ROADMAP Phase 3 SC1** requires `go doc ./pkg/storage` to list `Storage` with `Append`, `TruncateSuffix`, `Entries`, `Term`, `FirstIndex`, `LastIndex`, `SaveHardState`, `LoadHardState`, `Snapshot`, `Restore`.
- **Working Agreement 4** (PROJECT.md): the LLD is the byte-for-byte source of truth; code that adds methods to a public interface MUST ship with the LLD amendment in the same atomic commit.
- **LLD §5 Global Invariant 5** already anticipates "any storage snapshot hooks return `ErrSnapshotUnsupported` in v1" — so the LLD's own invariants assume Snapshot/Restore exist on Storage even though §3 didn't declare them yet.
- **PROC-01 / PROC-06:** every implementation phase lands at least one ADR.

Phase 2's stub also declared `ErrStorageNotImplemented` and an unexported `notImplementedStorage` receiver as a loud-failure guard. With a real impl arriving in Phase 3 (`pkg/storage/memory`, plan 03-02), the guard is no longer needed and risks becoming a footgun if any consumer accidentally compiles against it.

## Decision

**1. Interface home.** `Storage`, `LogStorage`, and `StateStorage` live in `pkg/storage/storage.go`. `pkg/raft/storage.go` is deleted (no consumers were wired in Phase 2, per RFC-0003).

**2. Snapshot/Restore placement.** Snapshot/Restore are added to **`StateStorage`** (the state half of the composed `Storage`), mirroring the placement of the pair on `StateMachine`:

```go
Snapshot() (data []byte, lastIndex raft.Index, err error)
Restore(data []byte) error
```

Both return `ErrSnapshotUnsupported` in v1. This was option-a in the plan 03-01 decision matrix and is the only option that satisfies ROADMAP SC1 verbatim while aligning with LLD §5 Invariant 5 wording. LLD §3 is amended in this same commit to declare the two methods on `StateStorage` so the LLD remains source of truth.

**3. `ErrSnapshotUnsupported` declaration site.** The sentinel's home is **`pkg/raft`** (`pkg/raft/errors.go`); `pkg/storage` re-exports it via `var ErrSnapshotUnsupported = raft.ErrSnapshotUnsupported`. `errors.Is` and `errors.Unwrap` work transparently across the re-export.

This **inverts** the direction suggested by the LLD §4 sidebar wording ("lives in `pkg/storage` ... re-exported from `pkg/raft`"). The inversion is forced by the import-cycle constraint: `pkg/storage` imports `pkg/raft` for `Entry`, `HardState`, `Index`, `Term`, so the sentinel cannot live in `pkg/storage` without `pkg/raft` losing its ability to reference it (and pkg/raft DOES need to reference it — `StateMachine.Snapshot`/`Restore` doc comments name it). LLD §4 is amended in this commit to match.

**4. Phase-2 stub deletion.** `pkg/raft.ErrStorageNotImplemented` and `notImplementedStorage` are deleted. The compile-time interface assertion that protected against signature drift will be re-established in `pkg/storage/memory/memory.go` (plan 03-02).

**5. `commitIndex` persistence policy.** `commitIndex` is held in `Node.commitIndex` (in-memory only) and is NOT persisted via `Storage.SaveHardState`, even though `HardState.Commit` exists on the wire/struct. The `HardState.Commit` field is a best-effort hint per its LLD §3 doc comment ("Commit is persisted for fast restart but is also recoverable from the log; loss of Commit on crash is safe but slower to converge"). This bullet absorbs ROADMAP Phase 3 SC4's reference to "ADR-0004 (commitIndex in-memory only)" into ADR-0005 — ADR-0004 itself remains scoped to the single-mutex state-machine decision and is not amended.

## Consequences

**Positive**
- `go doc ./pkg/storage` lists the frozen 10-method interface, satisfying ROADMAP Phase 3 SC1.
- Forward-compat: v2 file-snapshot work can populate `Snapshot`/`Restore` without changing the v1 signatures (STOR-01).
- The Phase-2 footgun (`ErrStorageNotImplemented`) is gone; no code path can accidentally compile against the not-implemented sentinel.
- One-way import edge `pkg/raft → pkg/storage` does NOT exist; the only edge is `pkg/storage → pkg/raft` (for types). Sentinel re-export is in the same direction (`pkg/storage` reads `raft.ErrSnapshotUnsupported`), keeping the dependency graph acyclic and asymmetric.
- ADR-0004's scope remains clean (single-mutex policy only); the commitIndex decision is recorded here where it is directly observable (HardState.Commit appears in `StateStorage.SaveHardState`).

**Negative**
- LLD §4 sidebar wording about the sentinel's "home" had to be inverted from "lives in `pkg/storage`" to "lives in `pkg/raft`." The user-visible behaviour (both names resolve to the same `errors.Is` target) is unchanged.
- `Storage` impls now MUST implement two stub methods (`Snapshot`, `Restore`) returning the sentinel. This is mechanical — a few lines per impl — but is one more thing the conformance harness (plan 03-03) must exercise.

**Follow-ups**
- Plan 03-02: implement `pkg/storage/memory` against this frozen interface, including the `Snapshot`/`Restore` stubs.
- Plan 03-03: extend `pkg/storage/storagetest` conformance harness to exercise the snapshot stubs (`Snapshot` returns `(nil, 0, ErrSnapshotUnsupported)`; `Restore(nil)` returns `ErrSnapshotUnsupported`).
- Plan 03-04: extend `docs/lld-go-doc-golden.txt` drift check to also cover `pkg/storage`, and regenerate the golden.
- Phase 5+ (Node wiring): when `Node` grows a `Storage` field, ensure `commitIndex` is NEVER written into `HardState.Commit` on the leader's persist path beyond the best-effort hint level.

## Alternatives Considered

- **Snapshot/Restore directly on `Storage`** (option-c in plan 03-01): rejected because snapshot is conceptually state-shaped, not log-shaped; placing it on `StateStorage` makes the symmetry with `StateMachine.Snapshot`/`Restore` explicit.
- **Snapshot/Restore stay on `StateMachine` only; amend ROADMAP SC1** (option-b): rejected because it conflicts with LLD §5 Invariant 5 ("any storage snapshot hooks") and leaves Phase 8 (`pkg/storage/file`) with no interface contract to stub against.
- **Sentinel home in `pkg/storage` with `pkg/raft` importing `pkg/storage`**: rejected — produces an import cycle because `pkg/storage` already imports `pkg/raft` for the four shared types. Would require extracting `Entry`/`HardState`/`Index`/`Term` to a `raftpb` sub-package, which is scope creep beyond Phase 3.
- **Amending ADR-0004 to add a `commitIndex`-in-memory bullet**: rejected because ADR-0004's scope is single-mutex policy; PROC-01 treats ADRs as append-only. Recording `commitIndex` policy as a Consequences bullet on ADR-0005 is cleaner.

## References

- RFC-0003 (Phase-2 storage stub; defers three decisions to Phase 3)
- ADR-0004 (single-mutex state machine — scope clarified, not amended)
- LLD §1 (package surface), §3 (Storage interfaces), §4 (sentinels), §5 Invariant 5 (snapshot-stub forward-compat)
- ROADMAP Phase 3 SC1, SC4
- `.planning/phases/03-storage-interface-memory-impl/03-RESEARCH.md` (Open Questions 1 + 2)
