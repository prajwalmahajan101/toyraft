package storagetest

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	"github.com/prajwalmahajan101/toyraft/pkg/storage"
)

// Factory produces a fresh, empty storage.Storage for each conformance
// sub-test. Implementations MUST return a NEW Storage value on every
// call; sub-tests share no state.
type Factory func(t *testing.T) storage.Storage

// RunConformance executes the full LLD §3 invariant suite against the
// Storage returned by f. Each invariant is a t.Run sub-test, so a
// single failure does not cascade.
//
// Coverage (one t.Run per invariant; names map to LLD §3 contract lines):
//
//   - Empty                       — zero-value getter contract
//   - AppendMonotonic             — contiguous append, LastIndex/Term advance
//   - AppendNonContiguousRejected — gap-after-empty is an error
//   - TruncateSuffixTail          — TruncateSuffix(from) drops [from, last]
//   - TruncateSuffixNoopAboveLast — TruncateSuffix above LastIndex is a no-op
//   - EntriesHalfOpen             — Entries(lo, hi) is half-open [lo, hi)
//   - EntriesOutOfRange           — hi > LastIndex+1 wraps io.ErrUnexpectedEOF
//   - TermOutOfRange              — Term(idx) above LastIndex wraps io.ErrUnexpectedEOF
//   - HardStateRoundtrip          — SaveHardState/LoadHardState preserve all fields
//   - HardStateFreshIsZero        — LoadHardState on fresh store returns (zero, nil)
//   - SnapshotStub                — Snapshot returns (nil, 0, ErrSnapshotUnsupported)
//   - RestoreStub                 — Restore returns ErrSnapshotUnsupported
//   - SnapshotFreshIsZero         — LoadSnapshot on fresh store returns (zero, nil)
//   - SnapshotRoundtrip           — SaveSnapshot/LoadSnapshot preserve all fields
//   - SnapshotOverwrite           — a second SaveSnapshot replaces the prior one
//   - SnapshotCallerCanMutate     — mutating SaveSnapshot input after return does not corrupt store
//   - EntriesCallerCanMutate      — mutating Entries' return does not corrupt store
//   - AppendCallerCanMutate       — mutating Append's input after return does not corrupt store
func RunConformance(t *testing.T, f Factory) {
	t.Helper()

	t.Run("Empty", func(t *testing.T) {
		s := f(t)

		last, err := s.LastIndex()
		if err != nil {
			t.Errorf("LastIndex on fresh store: err = %v, want nil", err)
		}
		if last != 0 {
			t.Errorf("LastIndex on fresh store = %d, want 0", last)
		}

		first, err := s.FirstIndex()
		if err != nil {
			t.Errorf("FirstIndex on fresh store: err = %v, want nil", err)
		}
		if first != 1 {
			t.Errorf("FirstIndex on fresh store = %d, want 1", first)
		}

		term, err := s.Term(0)
		if err != nil {
			t.Errorf("Term(0) on fresh store: err = %v, want nil", err)
		}
		if term != 0 {
			t.Errorf("Term(0) on fresh store = %d, want 0", term)
		}
	})

	t.Run("AppendMonotonic", func(t *testing.T) {
		s := f(t)
		mustAppend(t, s, []raft.Entry{
			{Term: 1, Index: 1, Data: []byte("a")},
			{Term: 1, Index: 2, Data: []byte("b")},
		})

		last, err := s.LastIndex()
		if err != nil {
			t.Fatalf("LastIndex: %v", err)
		}
		if last != 2 {
			t.Errorf("LastIndex = %d, want 2", last)
		}

		term, err := s.Term(2)
		if err != nil {
			t.Fatalf("Term(2): %v", err)
		}
		if term != 1 {
			t.Errorf("Term(2) = %d, want 1", term)
		}
	})

	t.Run("AppendNonContiguousRejected", func(t *testing.T) {
		s := f(t)
		err := s.Append([]raft.Entry{{Term: 1, Index: 3, Data: []byte("gap")}})
		if err == nil {
			t.Fatal("Append with gap on fresh store: err = nil, want non-nil")
		}

		last, err := s.LastIndex()
		if err != nil {
			t.Fatalf("LastIndex after rejected append: %v", err)
		}
		if last != 0 {
			t.Errorf("LastIndex after rejected append = %d, want 0", last)
		}
	})

	t.Run("TruncateSuffixTail", func(t *testing.T) {
		s := f(t)
		mustAppend(t, s, []raft.Entry{
			{Term: 1, Index: 1, Data: []byte("a")},
			{Term: 1, Index: 2, Data: []byte("b")},
			{Term: 1, Index: 3, Data: []byte("c")},
			{Term: 1, Index: 4, Data: []byte("d")},
			{Term: 1, Index: 5, Data: []byte("e")},
		})

		if err := s.TruncateSuffix(3); err != nil {
			t.Fatalf("TruncateSuffix(3): %v", err)
		}

		last, err := s.LastIndex()
		if err != nil {
			t.Fatalf("LastIndex: %v", err)
		}
		if last != 2 {
			t.Errorf("LastIndex after TruncateSuffix(3) = %d, want 2", last)
		}

		_, err = s.Term(3)
		if err == nil {
			t.Fatal("Term(3) after truncate: err = nil, want wrapped io.ErrUnexpectedEOF")
		}
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("Term(3) after truncate: err = %v, want errors.Is(io.ErrUnexpectedEOF)", err)
		}
	})

	t.Run("TruncateSuffixNoopAboveLast", func(t *testing.T) {
		s := f(t)
		mustAppend(t, s, []raft.Entry{
			{Term: 1, Index: 1, Data: []byte("a")},
			{Term: 1, Index: 2, Data: []byte("b")},
			{Term: 1, Index: 3, Data: []byte("c")},
		})

		if err := s.TruncateSuffix(99); err != nil {
			t.Fatalf("TruncateSuffix(99) above LastIndex: err = %v, want nil", err)
		}

		last, err := s.LastIndex()
		if err != nil {
			t.Fatalf("LastIndex: %v", err)
		}
		if last != 3 {
			t.Errorf("LastIndex after no-op TruncateSuffix = %d, want 3", last)
		}
	})

	t.Run("EntriesHalfOpen", func(t *testing.T) {
		s := f(t)
		mustAppend(t, s, []raft.Entry{
			{Term: 1, Index: 1, Data: []byte("a")},
			{Term: 1, Index: 2, Data: []byte("b")},
			{Term: 1, Index: 3, Data: []byte("c")},
		})

		got, err := s.Entries(1, 3)
		if err != nil {
			t.Fatalf("Entries(1,3): %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("Entries(1,3) len = %d, want 2", len(got))
		}
		if got[0].Index != 1 || got[1].Index != 2 {
			t.Errorf("Entries(1,3) indices = [%d,%d], want [1,2]", got[0].Index, got[1].Index)
		}

		got, err = s.Entries(1, 4)
		if err != nil {
			t.Fatalf("Entries(1,4): %v", err)
		}
		if len(got) != 3 {
			t.Errorf("Entries(1,4) len = %d, want 3", len(got))
		}

		got, err = s.Entries(2, 2)
		if err != nil {
			t.Errorf("Entries(2,2): err = %v, want nil", err)
		}
		if len(got) != 0 {
			t.Errorf("Entries(2,2) len = %d, want 0", len(got))
		}
	})

	t.Run("EntriesOutOfRange", func(t *testing.T) {
		s := f(t)
		mustAppend(t, s, []raft.Entry{
			{Term: 1, Index: 1, Data: []byte("a")},
			{Term: 1, Index: 2, Data: []byte("b")},
		})

		_, err := s.Entries(1, 4)
		if err == nil {
			t.Fatal("Entries(1,4) with LastIndex=2: err = nil, want wrapped io.ErrUnexpectedEOF")
		}
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("Entries(1,4): err = %v, want errors.Is(io.ErrUnexpectedEOF)", err)
		}
	})

	t.Run("TermOutOfRange", func(t *testing.T) {
		s := f(t)
		mustAppend(t, s, []raft.Entry{
			{Term: 1, Index: 1, Data: []byte("a")},
			{Term: 1, Index: 2, Data: []byte("b")},
		})

		_, err := s.Term(3)
		if err == nil {
			t.Fatal("Term(3) with LastIndex=2: err = nil, want wrapped io.ErrUnexpectedEOF")
		}
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("Term(3): err = %v, want errors.Is(io.ErrUnexpectedEOF)", err)
		}
	})

	t.Run("HardStateRoundtrip", func(t *testing.T) {
		s := f(t)
		hs := raft.HardState{
			CurrentTerm: 7,
			VotedFor:    raft.NodeID("node-42"),
			Commit:      5,
		}
		if err := s.SaveHardState(hs); err != nil {
			t.Fatalf("SaveHardState: %v", err)
		}

		got, err := s.LoadHardState()
		if err != nil {
			t.Fatalf("LoadHardState: %v", err)
		}
		if got != hs {
			t.Errorf("LoadHardState = %+v, want %+v", got, hs)
		}
	})

	t.Run("HardStateFreshIsZero", func(t *testing.T) {
		s := f(t)
		got, err := s.LoadHardState()
		if err != nil {
			t.Errorf("LoadHardState on fresh store: err = %v, want nil", err)
		}
		if got != (raft.HardState{}) {
			t.Errorf("LoadHardState on fresh store = %+v, want zero value", got)
		}
	})

	t.Run("SnapshotStub", func(t *testing.T) {
		s := f(t)
		data, idx, err := s.Snapshot()
		if data != nil {
			t.Errorf("Snapshot data = %v, want nil", data)
		}
		if idx != 0 {
			t.Errorf("Snapshot lastIndex = %d, want 0", idx)
		}
		if !errors.Is(err, storage.ErrSnapshotUnsupported) {
			t.Errorf("Snapshot err = %v, want errors.Is(storage.ErrSnapshotUnsupported)", err)
		}
	})

	t.Run("RestoreStub", func(t *testing.T) {
		s := f(t)
		err := s.Restore([]byte("anything"))
		if !errors.Is(err, storage.ErrSnapshotUnsupported) {
			t.Errorf("Restore err = %v, want errors.Is(storage.ErrSnapshotUnsupported)", err)
		}
	})

	t.Run("SnapshotFreshIsZero", func(t *testing.T) {
		s := f(t)
		got, err := s.LoadSnapshot()
		if err != nil {
			t.Errorf("LoadSnapshot on fresh store: err = %v, want nil", err)
		}
		if got.Index != 0 || got.Term != 0 || got.Data != nil {
			t.Errorf("LoadSnapshot on fresh store = %+v, want zero value", got)
		}
	})

	t.Run("SnapshotRoundtrip", func(t *testing.T) {
		s := f(t)
		snap := raft.Snapshot{Index: 9, Term: 3, Data: []byte("checkpoint-blob")}
		if err := s.SaveSnapshot(snap); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
		got, err := s.LoadSnapshot()
		if err != nil {
			t.Fatalf("LoadSnapshot: %v", err)
		}
		if got.Index != snap.Index || got.Term != snap.Term || !bytes.Equal(got.Data, snap.Data) {
			t.Errorf("LoadSnapshot = %+v, want %+v", got, snap)
		}
	})

	t.Run("SnapshotOverwrite", func(t *testing.T) {
		s := f(t)
		if err := s.SaveSnapshot(raft.Snapshot{Index: 4, Term: 1, Data: []byte("old")}); err != nil {
			t.Fatalf("SaveSnapshot(old): %v", err)
		}
		newer := raft.Snapshot{Index: 12, Term: 5, Data: []byte("new")}
		if err := s.SaveSnapshot(newer); err != nil {
			t.Fatalf("SaveSnapshot(new): %v", err)
		}
		got, err := s.LoadSnapshot()
		if err != nil {
			t.Fatalf("LoadSnapshot: %v", err)
		}
		if got.Index != newer.Index || got.Term != newer.Term || !bytes.Equal(got.Data, newer.Data) {
			t.Errorf("LoadSnapshot after overwrite = %+v, want %+v", got, newer)
		}
	})

	t.Run("SnapshotCallerCanMutate", func(t *testing.T) {
		s := f(t)
		blob := []byte("mutable")
		if err := s.SaveSnapshot(raft.Snapshot{Index: 1, Term: 1, Data: blob}); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
		blob[0] = 'X' // caller mutates the input AFTER SaveSnapshot returned
		got, err := s.LoadSnapshot()
		if err != nil {
			t.Fatalf("LoadSnapshot: %v", err)
		}
		if !bytes.Equal(got.Data, []byte("mutable")) {
			t.Errorf("after caller mutated SaveSnapshot input, store Data = %q, want %q", got.Data, "mutable")
		}
	})

	t.Run("EntriesCallerCanMutate", func(t *testing.T) {
		s := f(t)
		mustAppend(t, s, []raft.Entry{
			{Term: 1, Index: 1, Data: []byte("a")},
			{Term: 1, Index: 2, Data: []byte("b")},
			{Term: 1, Index: 3, Data: []byte("c")},
		})

		got, err := s.Entries(1, 4)
		if err != nil {
			t.Fatalf("Entries(1,4): %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("Entries(1,4) len = %d, want 3", len(got))
		}

		// Caller mutates the returned slice — both header fields and Data bytes.
		got[0].Term = 999
		if len(got[0].Data) > 0 {
			got[0].Data[0] = 'X'
		}

		got2, err := s.Entries(1, 4)
		if err != nil {
			t.Fatalf("Entries(1,4) second call: %v", err)
		}
		if got2[0].Term != 1 {
			t.Errorf("after caller mutated returned slice, store Term = %d, want 1", got2[0].Term)
		}
		if !bytes.Equal(got2[0].Data, []byte("a")) {
			t.Errorf("after caller mutated returned slice Data, store Data = %q, want %q", got2[0].Data, "a")
		}
	})

	t.Run("AppendCallerCanMutate", func(t *testing.T) {
		s := f(t)
		in := []raft.Entry{{Term: 1, Index: 1, Data: []byte("hello")}}
		if err := s.Append(in); err != nil {
			t.Fatalf("Append: %v", err)
		}

		// Caller mutates the input AFTER Append returned.
		in[0].Term = 999
		in[0].Data[0] = 'X'

		got, err := s.Entries(1, 2)
		if err != nil {
			t.Fatalf("Entries(1,2): %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("Entries(1,2) len = %d, want 1", len(got))
		}
		if got[0].Term != 1 {
			t.Errorf("after caller mutated Append input header, store Term = %d, want 1", got[0].Term)
		}
		if !bytes.Equal(got[0].Data, []byte("hello")) {
			t.Errorf("after caller mutated Append input Data, store Data = %q, want %q", got[0].Data, "hello")
		}
	})
}

// mustAppend appends entries and fails the test fatally if Append errors.
// Used by sub-tests where a failed setup makes subsequent assertions
// meaningless.
func mustAppend(t *testing.T, s storage.Storage, entries []raft.Entry) {
	t.Helper()
	if err := s.Append(entries); err != nil {
		t.Fatalf("Append unexpectedly failed: %v", err)
	}
}
