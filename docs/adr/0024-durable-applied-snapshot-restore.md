# 0024 — Durable applied checkpoint via Snapshot/Restore; restore in-memory log on restart

**Status:** Accepted
**Date:** 2026-09-11
**Scope:** `pkg/raft`, `pkg/storage`, `pkg/kvsm`

## Context

toymq (`github.com/prajwalmahajan101/toymq`) embedded ToyRaft as its consensus
library for its v3 milestone (single-node `toymq --replicate`), the v1.0.0
dogfood gate. M1 integration surfaced two release blockers against rc.2, each
reproduced against running embedded code — the same class of dogfood finding
that ADR-0023 fixed for the HTTP transport.

**B1 — the in-memory log is not restored on restart.** `newNode` (`node.go`)
constructs `log: &Log{}` and loads only `HardState` (`CurrentTerm`, `VotedFor`,
`Commit`). After a process restart `n.log.LastIndex()` returns 0 even though
`Storage` holds the persisted entries and `commitIndex` is recovered. Replay-apply
still works (the driver reads committed entries straight from `Storage`), so a
restarted node recovers its state — but a fresh `Propose` appends at
`LastIndex()+1 == 1`, `Storage.Append` sees a non-contiguous index over the
persisted `1..N`, fails, and `proposeLocked` rolls back and returns
`ErrProposalDropped`. **A restarted leader cannot accept new writes.**

**B2 — no durable applied index; replay double-applies.** `appliedIdx` /
`enqueuedIdx` are in-memory atomics zeroed on restart, and `Snapshot`/`Restore`
are stubbed (`ErrSnapshotUnsupported`) across `pkg/kvsm`, `pkg/storage/memory`,
and `pkg/storage/file`. Every restart replays the ENTIRE committed log through
`StateMachine.Apply`. An embedder with durable applied state (toymq appends to a
WAL inside `Apply`) therefore re-applies every committed entry. This also
violates LLD §5 invariant 4 / API-05 ("Apply is called exactly once per committed
entry"). toymq worked around it with an app-level applied-index marker, but every
embedder should not have to reinvent that.

Two `StateMachine` types pull in opposite directions on restart: an **in-memory**
SM (`pkg/kvsm`) needs the log replayed to rebuild its state; a **durable** SM
(toymq) must not re-apply already-persisted entries. Any single fix must serve
both. LLD §5 invariant 5 (STOR-01) freezes `Snapshot`/`Restore` as v1 stubs with
v2 filling them; this ADR amends that timeline for the durable-checkpoint path
below while keeping the frozen stub signatures byte-identical.

## Decision

**B1.** In `newNode`, after `LoadHardState` and before `started = true`, we restore
the in-memory `Log` from `Storage`: read `FirstIndex()..LastIndex()` and
`log.Append` the entries. `LastIndex()` then reflects the persisted log, so a
restarted leader appends at the correct next index. Empty-log (`LastIndex()==0`)
is a no-op.

**B2.** We make the **snapshot the durable applied floor**, resume-then-replay-tail:

1. The `StateMachine` tracks its own applied index — it already receives
   `entry.Index` in `Apply`. `pkg/kvsm` records `lastIndex` in `Apply`, returns it
   from `Snapshot`, and restores it in `Restore`.
2. `Storage` gains an explicit durable-snapshot pair. The frozen
   `StateStorage.Snapshot() ([]byte, Index, error)` / `Restore([]byte) error`
   signatures cannot express "save this blob at this index", so we add
   **`SaveSnapshot(Snapshot) error`** and **`LoadSnapshot() (Snapshot, error)`**
   to both `pkg/storage.Storage` and the local `pkg/raft.Storage` subset
   (`config.go`), kept byte-identical per ADR-0012 and guarded by
   `scripts/check-lld-drift.sh`. `Snapshot` is `{Index, Term, Data}`. The
   `file` backend persists it with the same atomic tmp+rename+SyncDir recipe as
   `hardstate.go` (ADR-0013); `memory` holds it in RAM. The frozen
   `Snapshot()`/`Restore()` stubs keep returning `ErrSnapshotUnsupported` —
   forward-compat (STOR-01) is preserved because those signatures are untouched;
   the new pair is the working durable path.
3. The driver checkpoints after applying: every `Config.SnapshotInterval` applied
   entries (default 1024; 0 disables), and unconditionally on `Stop` after the
   apply drain. A checkpoint calls `StateMachine.Snapshot()` and persists the blob
   + index via `Storage.SaveSnapshot`.
4. On construction / `Start`, the node loads the latest snapshot, calls
   `StateMachine.Restore(blob)`, and seeds `enqueuedIdx = appliedIdx = snap.Index`.
   `drainCommitsToApply` then replays only `(snap.Index, commitIndex]`.

A clean restart (snapshot taken at `commitIndex` on `Stop`) replays nothing —
**zero double-apply**. A crash between checkpoints replays `(lastSnapshot,
commit]`, a bounded window requiring SM idempotency only for that window.

**Rejected alternatives.** (a) *Persist appliedIndex alone, skip replay* — cheap,
but an in-memory SM starts empty on restart (regresses `pkg/kvsm`). (c) *Signal
`Apply` that an entry is a replay so durable SMs no-op* — changes the frozen
`Apply` signature and forces every SM to handle replay; it merely blesses toymq's
existing marker.

## Consequences

**Positive**
- A restarted single-node leader accepts writes (B1); unblocks toymq v3 M2
  (multi-node kill-leader + partition-heal linearizability depends on B1).
- Clean restarts no longer double-apply; API-05 exactly-once holds across the
  common restart path without embedder-side workarounds.
- In-memory and durable state machines are both correct on restart.

**Negative**
- New public storage surface (`SaveSnapshot`/`LoadSnapshot`) grows the LLD `go doc`
  golden; regenerated deliberately.
- A crash between checkpoints still replays `(lastSnapshot, commit]`; durable SMs
  must be idempotent over that window. Inherent to snapshot-based Raft, and not a
  regression (rc.2 replayed everything).
- `SnapshotInterval` is a durability/throughput knob with no universally-correct
  default; 1024 is a starting point, calibratable per embedder.

**Follow-ups**
- v2 log compaction (truncate below the snapshot horizon) — deferred; snapshots
  here do not yet compact the log, so `FirstIndex()` stays 1.
- The frozen `Snapshot()`/`Restore()` stubs remain reserved for the v2 contract.
- ADR-0005 / LLD §5 invariant 5 owe a note that the durable-checkpoint path landed
  at v1.0.0-rc.3 via this ADR while the frozen stubs stay reserved.
