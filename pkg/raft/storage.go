package raft

import "errors"

// ErrStorageNotImplemented is returned by every method on the Phase-2
// not-yet-implemented Storage receiver declared at the bottom of this
// file. Phase 3 replaces that receiver with a real in-RAM impl in
// pkg/storage/memory; consumers of Storage that depend on actual
// behaviour MUST NOT be wired up until then. See
// docs/rfc/0003-storage-interface-stub-in-phase-2.md.
//
// The sentinel is deliberately distinct from any I/O error: a caller
// that observes ErrStorageNotImplemented has wired the Phase-2
// not-yet-implemented receiver by mistake, not encountered a runtime
// fault.
var ErrStorageNotImplemented = errors.New("raft/storage: not implemented in phase 2 (see RFC-0003)")

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
	Append(entries []Entry) error

	// TruncateSuffix discards entries with index >= from. Used on log conflict.
	//
	// Invariants:
	//   - MUST fsync before returning.
	//   - No-op (returns nil) if from > LastIndex().
	//
	// Error contract:
	//   - Wraps the underlying I/O error with %w.
	TruncateSuffix(from Index) error

	// Entries returns the half-open range [lo, hi).
	//
	// Invariants:
	//   - Returned slice is freshly allocated; caller may mutate freely.
	//
	// Error contract:
	//   - Returns an error wrapping io.ErrUnexpectedEOF if hi > LastIndex()+1.
	//   - v2: returns ErrCompacted if lo is below the snapshot horizon.
	Entries(lo, hi Index) ([]Entry, error)

	// Term returns the term of the entry at index, or 0 if index == 0
	// (the implicit pre-log sentinel).
	//
	// Error contract:
	//   - Returns an error wrapping io.ErrUnexpectedEOF if index > LastIndex().
	Term(index Index) (Term, error)

	// FirstIndex returns the smallest index present in the log. 1 in v1
	// (no compaction); v2 returns snapshotIndex+1.
	FirstIndex() (Index, error)

	// LastIndex returns the largest index present in the log, or 0 if the
	// log is empty.
	LastIndex() (Index, error)
}

// StateStorage persists HardState (the durable Raft state).
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
	SaveHardState(hs HardState) error

	// LoadHardState returns the most recently persisted HardState, or the
	// zero value if none has ever been saved (fresh node).
	//
	// Error contract:
	//   - A missing file is NOT an error; returns (HardState{}, nil).
	//   - A corrupt file IS an error; wraps the parse error with %w.
	LoadHardState() (HardState, error)
}

// notImplementedStorage is the Phase-2 compile-time receiver. Every
// method returns ErrStorageNotImplemented (with matching zero values
// for non-error return positions). Phase 3 replaces this type with the
// real in-RAM impl in pkg/storage/memory.
//
// The type is unexported and not wired into any consumer. Its sole
// purpose is to keep the interface shape honest via the compile-time
// assertion below: if LLD §3 grows a method and the interface is
// updated, notImplementedStorage must be updated too or the build breaks.
type notImplementedStorage struct{}

// Compile-time assertion that notImplementedStorage satisfies Storage. Catches
// signature drift inside this file.
var _ Storage = (*notImplementedStorage)(nil)

// Append implements LogStorage.Append; Phase-2 stub returns
// ErrStorageNotImplemented.
func (*notImplementedStorage) Append(entries []Entry) error {
	return ErrStorageNotImplemented
}

// TruncateSuffix implements LogStorage.TruncateSuffix; Phase-2 stub
// returns ErrStorageNotImplemented.
func (*notImplementedStorage) TruncateSuffix(from Index) error {
	return ErrStorageNotImplemented
}

// Entries implements LogStorage.Entries; Phase-2 stub returns
// ErrStorageNotImplemented.
func (*notImplementedStorage) Entries(lo, hi Index) ([]Entry, error) {
	return nil, ErrStorageNotImplemented
}

// Term implements LogStorage.Term; Phase-2 stub returns
// ErrStorageNotImplemented.
func (*notImplementedStorage) Term(index Index) (Term, error) {
	return 0, ErrStorageNotImplemented
}

// FirstIndex implements LogStorage.FirstIndex; Phase-2 stub returns
// ErrStorageNotImplemented.
func (*notImplementedStorage) FirstIndex() (Index, error) {
	return 0, ErrStorageNotImplemented
}

// LastIndex implements LogStorage.LastIndex; Phase-2 stub returns
// ErrStorageNotImplemented.
func (*notImplementedStorage) LastIndex() (Index, error) {
	return 0, ErrStorageNotImplemented
}

// SaveHardState implements StateStorage.SaveHardState; Phase-2 stub
// returns ErrStorageNotImplemented.
func (*notImplementedStorage) SaveHardState(hs HardState) error {
	return ErrStorageNotImplemented
}

// LoadHardState implements StateStorage.LoadHardState; Phase-2 stub
// returns ErrStorageNotImplemented. Note: a real impl returns
// (HardState{}, nil) on a missing file; the stub deliberately surfaces
// the sentinel so any Phase-2 consumer that accidentally wires the stub
// fails loudly (guards the P0-4 footgun: a nil error would let an
// election proceed without a persisted vote).
func (*notImplementedStorage) LoadHardState() (HardState, error) {
	return HardState{}, ErrStorageNotImplemented
}
