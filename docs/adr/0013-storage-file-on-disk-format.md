# 0013 — storage/file on-disk format + in-process fault seam

**Status:** Accepted
**Date:** 2026-07-04
**Scope:** `pkg/storage/file` (`file.go`, `segment.go`, `hardstate.go`, `vfs.go`, `recover_test.go`)

## Context

Phase 8 ships `pkg/storage/file`, the second `storage.Storage`
implementation and a **behavioral drop-in** for `pkg/storage/memory`. It is
wired into the **same** `storagetest.RunConformance` suite (Phase 3), with a
factory that returns a fresh `file.New(t.TempDir())` per sub-test, and it
must pass **all 14 sub-tests with NO impl-specific carve-outs** (ROADMAP
Phase 8 SC1). The `Storage` interface is **frozen** (ADR-0005); the file impl
adds durability semantics *under* that contract — it does not change it. This
phase retires risks **P0-4 (full), Per-1..Per-5, and T-6**.

Forces requiring a ratified on-disk contract *now* (wave 1, before the impl
plans 08-01/08-03/08-04):

- **Per-5** mandates that the format's version byte be documented **up front**
  (an ADR), not discovered from code later. The on-disk format is a durable,
  forward-compat-sensitive contract; changing it must be deliberate and
  detectable.
- **PROC-01/PROC-06:** every implementation phase lands ≥1 ADR.
- The on-disk format is **greenfield** — no prior `pkg/storage/file` code or
  spec exists. The recovery scan, fsync ordering, and fault seam are the crux
  of the phase and need a single source of truth the impl plans build to
  (prevents drift across the parallel waves).

References: ADR-0005 (Storage interface freeze; `ErrSnapshotUnsupported`
home), ADR-0011 (log-storage mirror; Phase 8 makes that mirror *actually
durable*), ADR-0012 (Storage interface reconciliation; the file store is
passed to `raft.New` with no cast), ADR-0004 (single-mutex; `commitIndex`
in-memory only — a **node-level** policy, see the trap below),
ADR-0006/0007 (FakeClock / inproc Hub determinism precedent for the new fault
seam), LLD §3/§5, docs/TESTING.md (routes **real** process-kill to Phase 11),
docs/SECURITY.md (Linux/localhost v1 scope), `.planning/ROADMAP.md` Phase 8
SC1-SC5, REQUIREMENTS STOR-03..06, PITFALLS P0-4/Per-1..6/T-6.

**CRITICAL conformance trap — `HardState.Commit` MUST be persisted.** The
conformance `HardStateRoundtrip` sub-test sets `Commit: 5` and asserts
`LoadHardState()` struct-equals the saved `HardState` (all three fields:
`CurrentTerm`, `VotedFor`, `Commit`). The oft-repeated "`commitIndex` is
in-memory only" rule (ADR-0004/0005) is a **node-level** policy — the raft
*node* never writes its live `commitIndex` into `HardState.Commit` on the
persist path. It is **NOT** a storage-layer carve-out: `SaveHardState` /
`LoadHardState` must faithfully round-trip `Commit`. Dropping it fails
conformance, and SC1 forbids carve-outs.

## Decision

We adopt the industry-standard WAL recipe (etcd/bolt/LevelDB shape) — do not
invent — and record each choice explicitly.

**1. Record framing.** Each log entry is one length-prefixed, CRC-framed
record, **little-endian**:

```
[uint32 len][uint32 crc][payload]
 payload = Term(uint64 LE) ‖ Index(uint64 LE) ‖ Data(len-16 bytes)
 crc     = crc32.Castagnoli over the payload ONLY (not over the len field)
```

`len` bounds the read so recovery never over-reads; `Data`'s length is
implied by `len - 16`, so no separate Data-length field is needed. CRC uses
the **Castagnoli** polynomial (`crc32.MakeTable(crc32.Castagnoli)`), chosen
for its better error-detection than IEEE. Hardware acceleration is **not**
the deciding factor (Open Question 3): the choice stands on error-detection
grounds and is low-stakes at ToyRaft scale.

**2. Segments + version header.** The log is **append-only**, split into
segment files. Each segment begins with a header `magic(4) ‖ version(1)`
(version = **1**); the write path writes only the current version, the read
path branches on it. Segments are named `%016d.seg` by the **first index**
they hold, giving stable lexical ordering that matches index order.

**3. Rollover + parent-dir fsync (SC5/Per-3).** The active segment rolls to a
new file past a threshold (**default 1024 entries**, test-overridable so the
tests cheaply force a rollover). On rollover the impl `Create`s the new
segment, writes its header, and **fsyncs the parent directory before the next
`Append` into the new segment returns**. A data fsync without the directory
fsync can leave the new segment's directory entry non-durable — after a crash
the entries appear truncated (Per-3).

**4. Recovery / torn-tail truncation (SC2/STOR-06).** On open, scan each
segment in index order: verify the header version, then loop reading
`len` → `crc` → `payload`. On the **first** short read (torn tail) or CRC
mismatch (corrupt), stop and `Truncate` the segment at the byte offset
**immediately after the last CRC-valid record**, then `Sync`, and rebuild the
in-RAM index up to that last good entry. This truncates **exactly** the
trailing partial/corrupt record and nothing before it —
`TestTornTailRecovery` asserts both `LastIndex() == N-1` and the physical
file size (guarding the off-by-one in both directions).

**5. fsync strategy — per-op, no batching (P0-4/Per-1, Per-4).** `Append` and
`TruncateSuffix` call `Sync()` (and, on rollover, the parent-dir fsync)
**before returning**. There is **NO group-commit / fsync batching in v1**
(Per-4): naive batching would ack entries the leader counts toward quorum
before they are durable. v1 is **fsync-per-Append**; the perf cost (one
fsync per entry) is accepted and documented rather than optimized away.

**6. HardState — tmp + fsync + rename + dir-fsync (SC4/STOR-05).**
`SaveHardState`:

```
1. marshal HS (CurrentTerm, VotedFor, Commit) with a CRC prefix   // corruption detectable on load
2. write <dir>/hardstate.tmp ; f.Sync()                           // tmp durable BEFORE rename
3. os.Rename(hardstate.tmp, hardstate)                            // atomic on the same Unix filesystem
4. SyncDir(dir)                                                   // make the rename durable
```

`LoadHardState`: **missing file → `(HardState{}, nil)`** (not an error);
present but CRC/parse fails → **error** (wrapped with `%w`); an orphaned
pre-rename `.tmp` must not corrupt the read path. All three fields, including
`Commit`, round-trip (the trap above).

**7. In-RAM index.** On recovery the impl rebuilds a per-entry index
`{index, term, segment, offset, length}`, serving `FirstIndex`/`LastIndex`/
`Term` in O(1) and locating bytes for `Entries` reads. `Entries` returns
**freshly allocated**, caller-mutable slices (reading `Data` fresh from disk
via `ReaderAt` trivially satisfies `EntriesCallerCanMutate`). This mirrors
ADR-0011's "fast in-RAM read path + durable backing" shape at the storage
layer.

**8. In-process fault seam (T-6).** Durability is tested through an
**unexported `vfs` seam** inside `package file`: an `osVFS` real impl (thin
`*os.File`/`os.Rename`/`os.ReadDir` wrapper; `SyncDir` = `os.Open(dir).Sync()`)
and a `faultVFS` in-memory fake that models the **page cache** — writes land
in a per-file *unsynced* buffer, `Sync()` promotes unsynced→durable, and a
`Crash()` method **discards all unsynced bytes** (and orphaned pre-rename tmp
state), modeling "lose all writes since last fsync". A crash test must crash
**between** Write and Sync/Rename and include a **non-vacuous negative** (a
completed+synced write survives `Crash()`), else it proves nothing (Pitfall
3/T-6). Both `New` (with `osVFS`) and the fault tests (with `faultVFS`) go
through an unexported `openWith(fs, dir)`, keeping the fake referenced from
internal tests so the `unused` linter does not flag it (Pitfall 5).

**Real `kill -9` chaos is explicitly Phase 11** (`test/chaos/processkill/`),
**NOT** this phase. Phase 8 SC3/SC4 say *in-process* fault injection
("fault-injecting `vfs` test", "unit test"); a real subprocess crash suite is
out of scope here.

## Consequences

**Positive**

- Durability is real and *provable*: the entry-side of P0-4 lands on disk
  (retiring P0-4 full), and SC2-SC5 each have a targeted test. The fault seam
  gives deterministic "lose unsynced writes" that no library provides.
- The format is **versioned** (§2), so v2 can extend it without breaking v1
  readers; the version byte is ADR-ratified up front (Per-5).
- The file store drops straight into `raft.New(Config{Storage: fileStore})`
  with no cast (ADR-0012), and passes the frozen conformance suite unmodified
  (SC1) — the memory impl stays the reference behavior.

**Negative**

- **fsync-per-Append perf cost** (Per-4): one fsync per entry caps throughput.
  Accepted for v1 at ToyRaft's 3-5 node scale; a high-throughput impl would
  batch (deferred, not built).
- **Unix/Linux durability assumption.** `os.Rename` is atomic **only on the
  same Unix filesystem** (not on non-Unix). Directory fsync via
  `os.Open(dir).Sync()` is the established **Linux** idiom (fsync(2)) but is
  **not formally guaranteed by the stdlib** — a `syscall.Fsync(int(d.Fd()))`
  fallback is the escape hatch. v1 is Linux/localhost-scoped (docs/SECURITY.md;
  netns tests are linux-only). This assumption is recorded here, not hidden.
- The `vfs` seam adds a small indirection layer over `os` that every write
  path routes through — the cost of making crash behavior injectable.

**Follow-ups**

- Snapshotting stays an `ErrSnapshotUnsupported` stub (v2, STOR-01); Per-6
  (snapshot rename fsync) is **not** this phase.
- Group-commit / fsync batching is a v2/perf follow-up (Per-4).
- Real subprocess `kill -9` chaos lands in Phase 11 (`test/chaos/processkill/`),
  exercising the same durability contract this ADR ratifies.
- Whether `pkg/storage/file` joins the LLD go-doc golden drift gate is an impl
  decision for 08-01 (Open Question 4); it adds no public symbols to any
  currently-snapshotted package.

## Usage

- `pkg/storage/file/segment.go` — record framing (encode/decode), segment
  write + rollover, per-segment scan/truncate (§1-§4).
- `pkg/storage/file/hardstate.go` — `SaveHardState` (tmp+fsync+rename+dir-fsync)
  / `LoadHardState` (CRC-validated, missing→zero) (§6).
- `pkg/storage/file/vfs.go` — unexported `vfs`/`vfile` seam + `osVFS` (§8).
- `pkg/storage/file/file.go` — `New(dir)` / `openWith(fs, dir)` / recovery
  orchestration / in-RAM index / `Entries`/`Term`/`First`/`LastIndex` (§7).
- `pkg/storage/file/recover_test.go` — `TestTornTailRecovery` +
  Append/HardState crash-injection via `faultVFS` (§4, §8).
