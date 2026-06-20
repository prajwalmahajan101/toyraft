// Package memory provides an in-RAM implementation of storage.Storage
// suitable for tests, ephemeral clusters, and any caller that does not
// require crash recovery.
//
// # Concurrency
//
// All methods are safe for concurrent use. Reads (Entries, Term,
// FirstIndex, LastIndex, LoadHardState, Snapshot) use an RLock; writes
// (Append, TruncateSuffix, SaveHardState, Restore) use a Lock.
//
// This package is on the persistence side of the storage interface
// boundary, so ADR-0004's single-mutex-on-Node policy does NOT apply
// here (ADR-0004 forbids internal locking on the in-process pkg/raft.Log,
// not on Storage implementations). See ADR-0005.
//
// # Zero value
//
// The zero value of Storage is a valid, empty store. &memory.Storage{},
// new(memory.Storage), and memory.New() are equivalent.
//
// # Snapshot support
//
// Snapshot and Restore return storage.ErrSnapshotUnsupported in v1 per
// LLD §5 Global Invariant 5. v2 will populate them without changing
// the signatures (REQ-STOR-07).
package memory
