// Package storage declares the Storage interface that pkg/raft consumes
// for log and hard-state persistence, plus the v1 ErrSnapshotUnsupported
// sentinel that storage snapshot stubs return.
//
// Implementations live in pkg/storage/memory (in-RAM, for tests and
// ephemeral clusters) and pkg/storage/file (append-only with fsynced
// HardState). Third-party implementors may exercise pkg/storage/storagetest
// for conformance.
//
// Source of truth: docs/LLD.md §3 (interface shape) and §4 (sentinel
// placement). See ADR-0005 for the Phase 3 freeze rationale.
package storage

import (
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// ErrSnapshotUnsupported is returned by every storage snapshot stub in
// v1. v2 will populate Snapshot/Restore with real semantics without
// breaking the v1 interface signatures (STOR-01 forward-compat;
// LLD §5 Global Invariant 5).
//
// The sentinel's home is pkg/raft (raft.ErrSnapshotUnsupported); this
// is a re-export so callers can write either storage.ErrSnapshotUnsupported
// or raft.ErrSnapshotUnsupported and errors.Is resolves identically
// (both reference the same *errorString value).
//
// Direction rationale: pkg/storage already imports pkg/raft for
// raft.Entry / raft.HardState / raft.Index / raft.Term, so the sentinel
// must live in pkg/raft to avoid an import cycle. See ADR-0005.
var ErrSnapshotUnsupported = raft.ErrSnapshotUnsupported

// Storage composes log and hard-state persistence. Implementations live in
// pkg/storage/memory and pkg/storage/file; consumers may write their own
// and exercise pkg/storage/storagetest for conformance.
type Storage interface {
	LogStorage
	StateStorage
}

// LogStorage persists the replicated log.
type LogStorage interface {
	// Append persists entries in order. Entries are contiguous and start at
	// LastIndex()+1.
	//
	// Invariants:
	//   - MUST fsync the underlying storage before returning success (REPL-09).
	//   - MUST NOT modify entries after return; caller may reuse the slice.
	//   - On error, the on-disk state is unchanged (atomic per-call).
	//
	// Error contract:
	//   - Wraps the underlying I/O error with %w.
	Append(entries []raft.Entry) error

	// TruncateSuffix discards entries with index >= from. Used on log conflict.
	//
	// Invariants:
	//   - MUST fsync before returning.
	//   - No-op (returns nil) if from > LastIndex().
	//
	// Error contract:
	//   - Wraps the underlying I/O error with %w.
	TruncateSuffix(from raft.Index) error

	// Entries returns the half-open range [lo, hi).
	//
	// Invariants:
	//   - Returned slice is freshly allocated; caller may mutate freely.
	//
	// Error contract:
	//   - Returns an error wrapping io.ErrUnexpectedEOF if hi > LastIndex()+1.
	//   - v2: returns ErrCompacted if lo is below the snapshot horizon.
	Entries(lo, hi raft.Index) ([]raft.Entry, error)

	// Term returns the term of the entry at index, or 0 if index == 0
	// (the implicit pre-log sentinel).
	//
	// Error contract:
	//   - Returns an error wrapping io.ErrUnexpectedEOF if index > LastIndex().
	Term(index raft.Index) (raft.Term, error)

	// FirstIndex returns the smallest index present in the log. 1 in v1
	// (no compaction); v2 returns snapshotIndex+1.
	FirstIndex() (raft.Index, error)

	// LastIndex returns the largest index present in the log, or 0 if the
	// log is empty.
	LastIndex() (raft.Index, error)
}

// StateStorage persists HardState (the durable Raft state) and exposes
// the v1 snapshot stubs.
type StateStorage interface {
	// SaveHardState durably persists the given HardState.
	//
	// Invariants:
	//   - MUST fsync before returning (REPL-09).
	//   - MUST be atomic: a crash mid-call leaves either the prior or the new
	//     HardState fully on disk, never a torn write.
	//   - Implementations SHOULD use the tmp+rename pattern on Unix.
	//
	// Error contract:
	//   - Wraps the underlying I/O error with %w.
	SaveHardState(hs raft.HardState) error

	// LoadHardState returns the most recently persisted HardState, or the
	// zero value if none has ever been saved (fresh node).
	//
	// Error contract:
	//   - A missing file is NOT an error; returns (raft.HardState{}, nil).
	//   - A corrupt file IS an error; wraps the parse error with %w.
	LoadHardState() (raft.HardState, error)

	// Snapshot serialises the persisted state up to lastIndex.
	//
	// v1: implementors MUST return (nil, 0, ErrSnapshotUnsupported). v2:
	// will define snapshot semantics; this signature is forward-compatible
	// (STOR-01; LLD §5 Global Invariant 5).
	Snapshot() (data []byte, lastIndex raft.Index, err error)

	// Restore replaces the persisted state from a snapshot produced by
	// Snapshot.
	//
	// v1: implementors MUST return ErrSnapshotUnsupported.
	Restore(data []byte) error
}
