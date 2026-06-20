package memory

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	"github.com/prajwalmahajan101/toyraft/pkg/storage"
)

// Storage is an in-RAM implementation of storage.Storage. Safe for
// concurrent use via an internal RWMutex.
//
// The zero value is a valid, empty store (FOUND-05): &Storage{} or
// new(Storage) both work without initialization.
//
// Snapshot/Restore return storage.ErrSnapshotUnsupported in v1 per
// LLD §5 Global Invariant 5. v2 will populate them without changing
// signatures (REQ-STOR-07).
type Storage struct {
	mu      sync.RWMutex
	entries []raft.Entry // entries[i].Index == raft.Index(i+1)
	hs      raft.HardState
}

// Compile-time interface assertion. Catches drift if storage.Storage grows.
var _ storage.Storage = (*Storage)(nil)

// ErrNonContiguous is wrapped by Append when entries do not start at
// LastIndex()+1 or are not strictly increasing by 1.
var ErrNonContiguous = errors.New("memory storage: non-contiguous append")

// New returns an empty in-RAM Storage. Equivalent to &Storage{}.
func New() *Storage { return &Storage{} }

// Append persists entries in order per LLD §3. Entries MUST be contiguous
// and start at LastIndex()+1; otherwise the call returns an error wrapping
// ErrNonContiguous and leaves the store unchanged.
//
// Entry.Data is deep-copied per entry so the caller may reuse / mutate
// the input slice and its byte buffers after Append returns — see LLD §3
// caller-mutation contract and research pitfall §5.
func (m *Storage) Append(entries []raft.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(entries) == 0 {
		return nil
	}

	expected := raft.Index(len(m.entries)) + 1
	if entries[0].Index != expected {
		return fmt.Errorf("memory storage: append at index %d, got %d: %w", expected, entries[0].Index, ErrNonContiguous)
	}
	for i := 0; i+1 < len(entries); i++ {
		if entries[i+1].Index != entries[i].Index+1 {
			return fmt.Errorf("memory storage: append at index %d, got %d: %w", entries[i].Index+1, entries[i+1].Index, ErrNonContiguous)
		}
	}

	// deep-copy Data so caller may reuse / mutate the input slice and its
	// byte buffers after Append — see LLD §3 caller-mutation contract and
	// research pitfall §5. A bare append(m.entries, entries...) only copies
	// the Entry value (slice header), NOT the underlying Data bytes.
	for _, e := range entries {
		copied := e
		if len(e.Data) > 0 {
			copied.Data = make([]byte, len(e.Data))
			copy(copied.Data, e.Data)
		}
		m.entries = append(m.entries, copied)
	}
	return nil
}

// TruncateSuffix discards entries with index >= from per LLD §3.
// No-op (returns nil) if from > LastIndex().
func (m *Storage) TruncateSuffix(from raft.Index) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	last := raft.Index(len(m.entries))
	if from > last {
		return nil
	}
	if from < 1 {
		return fmt.Errorf("memory storage: truncate at invalid index %d", from)
	}
	m.entries = m.entries[:from-1]
	return nil
}

// Entries returns the half-open range [lo, hi) per LLD §3. The returned
// slice is freshly allocated AND Entry.Data is deep-copied per entry, so
// the caller may mutate freely without corrupting the store — symmetric
// with Append's write-path deep-copy.
//
// Returns an error wrapping io.ErrUnexpectedEOF if hi > LastIndex()+1.
func (m *Storage) Entries(lo, hi raft.Index) ([]raft.Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	last := raft.Index(len(m.entries))
	if lo < 1 || lo > hi {
		return nil, fmt.Errorf("memory storage: invalid range [%d,%d)", lo, hi)
	}
	if hi > last+1 {
		return nil, fmt.Errorf("memory storage: hi=%d > LastIndex+1=%d: %w", hi, last+1, io.ErrUnexpectedEOF)
	}
	if lo == hi {
		return []raft.Entry{}, nil
	}

	// fresh slice + per-entry Data deep-copy; caller may mutate freely
	// without corrupting the store — symmetric with Append.
	out := make([]raft.Entry, hi-lo)
	for i, e := range m.entries[lo-1 : hi-1] {
		copied := e
		if len(e.Data) > 0 {
			copied.Data = make([]byte, len(e.Data))
			copy(copied.Data, e.Data)
		}
		out[i] = copied
	}
	return out, nil
}

// Term returns the term of the entry at index, or 0 if index == 0
// (the implicit pre-log sentinel per LLD §3).
//
// Returns an error wrapping io.ErrUnexpectedEOF if index > LastIndex().
func (m *Storage) Term(index raft.Index) (raft.Term, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if index == 0 {
		return 0, nil
	}
	last := raft.Index(len(m.entries))
	if index > last {
		return 0, fmt.Errorf("memory storage: term at index %d > LastIndex %d: %w", index, last, io.ErrUnexpectedEOF)
	}
	return m.entries[index-1].Term, nil
}

// FirstIndex returns 1 always — v1 has no compaction per LLD §3.
func (m *Storage) FirstIndex() (raft.Index, error) {
	return 1, nil
}

// LastIndex returns the largest index present in the log, or 0 if the
// log is empty per LLD §3.
func (m *Storage) LastIndex() (raft.Index, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return raft.Index(len(m.entries)), nil
}

// SaveHardState durably persists the given HardState per LLD §3.
// In-RAM impl: value-copy assignment (HardState is a value type).
func (m *Storage) SaveHardState(hs raft.HardState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hs = hs
	return nil
}

// LoadHardState returns the most recently saved HardState, or the zero
// value on a fresh store per LLD §3 — NEVER an error. See research
// pitfall §6: missing state is NOT a sentinel error.
func (m *Storage) LoadHardState() (raft.HardState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.hs, nil
}

// Snapshot returns storage.ErrSnapshotUnsupported in v1 per LLD §5
// Global Invariant 5. v2 will populate without changing the signature.
func (m *Storage) Snapshot() ([]byte, raft.Index, error) {
	return nil, 0, storage.ErrSnapshotUnsupported
}

// Restore returns storage.ErrSnapshotUnsupported in v1 per LLD §5
// Global Invariant 5. v2 will populate without changing the signature.
func (m *Storage) Restore(data []byte) error {
	return storage.ErrSnapshotUnsupported
}
