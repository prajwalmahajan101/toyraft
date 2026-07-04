package file

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// TestTornTailRecovery proves SC2 on a REAL on-disk file (osVFS via New): a
// bit-flip in the LAST record's CRC is treated as a torn tail, and recovery
// truncates EXACTLY that record — LastIndex drops to N-1 (not N-2), the first
// N-1 entries survive intact, and the segment file is PHYSICALLY shrunk to the
// end of record N-1. This retires the off-by-one risk (Pitfall 4): v1 recovery
// cuts the tail at the last CRC-valid record and no further.
//
// Scope note: v1 recovery only truncates the TAIL. Corrupting a MIDDLE record's
// CRC is out of scope for v1 (it would need re-log / hole handling); this test
// deliberately targets the final record.
func TestTornTailRecovery(t *testing.T) {
	dir := t.TempDir()

	const n = 5
	s, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for i := 1; i <= n; i++ {
		e := raft.Entry{Term: 1, Index: raft.Index(i), Data: []byte{byte('a' + i)}}
		if err := s.Append([]raft.Entry{e}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The whole log lives in one segment (default rollover 1024): 0..1 first idx.
	segPath := filepath.Join(dir, segmentName(1))

	// Walk the segment to find the byte offset of the FINAL record. That offset
	// is exactly the end of record N-1 == the expected torn-tail truncation
	// boundary (recovery cuts the tail at the last CRC-valid record).
	lastRecOff := lastRecordStart(t, segPath)
	goodEndNMinus1 := lastRecOff

	preSize := statSize(t, segPath)

	// Flip one bit in the last record's 4-byte CRC field (record layout:
	// [len(4)][crc(4)][payload]; CRC starts 4 bytes into the record).
	crcByteOff := lastRecOff + 4
	f, err := os.OpenFile(segPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open seg for corruption: %v", err)
	}
	var one [1]byte
	if _, err := f.ReadAt(one[:], crcByteOff); err != nil {
		t.Fatalf("read crc byte: %v", err)
	}
	one[0] ^= 0x01
	if _, err := f.WriteAt(one[:], crcByteOff); err != nil {
		t.Fatalf("write corrupted crc byte: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close after corruption: %v", err)
	}

	// Reopen: recovery must drop EXACTLY the last record.
	s2, err := New(dir)
	if err != nil {
		t.Fatalf("reopen after corruption: %v", err)
	}
	defer func() { _ = s2.Close() }()

	last, err := s2.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if last != n-1 {
		t.Fatalf("LastIndex after torn-tail recovery = %d, want %d (exactly one record dropped)", last, n-1)
	}

	// The first N-1 entries survive intact.
	got, err := s2.Entries(1, n) // [1, n) == indices 1..n-1
	if err != nil {
		t.Fatalf("Entries(1,%d): %v", n, err)
	}
	if len(got) != n-1 {
		t.Fatalf("recovered %d entries, want %d", len(got), n-1)
	}
	for i, e := range got {
		wantIdx := raft.Index(i + 1)
		wantData := []byte{byte('a' + (i + 1))}
		if e.Index != wantIdx || e.Term != 1 || !bytes.Equal(e.Data, wantData) {
			t.Fatalf("entry %d = %+v, want Index=%d Term=1 Data=%v", i, e, wantIdx, wantData)
		}
	}

	// PHYSICAL truncation: the file shrank to the end of record N-1.
	postSize := statSize(t, segPath)
	if postSize >= preSize {
		t.Fatalf("segment not physically truncated: pre=%d post=%d", preSize, postSize)
	}
	if postSize != goodEndNMinus1 {
		t.Fatalf("segment truncated to %d, want exactly goodEnd(N-1)=%d", postSize, goodEndNMinus1)
	}
}

// lastRecordStart walks a segment file on disk and returns the byte offset of
// the LAST record's start. Because records are laid out contiguously, that
// offset is exactly the end of the second-to-last record — i.e. the boundary at
// which a torn-tail recovery that drops the final record must truncate. It
// reuses the segment framing (header segHeaderLen bytes, then
// [len(4)][crc(4)][payload]).
func lastRecordStart(t *testing.T, segPath string) int64 {
	t.Helper()
	raw, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatalf("read seg: %v", err)
	}
	off := int64(segHeaderLen)
	lastRecStart := int64(-1)
	for off < int64(len(raw)) {
		if off+recHeaderLen > int64(len(raw)) {
			break
		}
		declaredLen := binary.LittleEndian.Uint32(raw[off : off+4])
		recEnd := off + recHeaderLen + int64(declaredLen)
		if recEnd > int64(len(raw)) {
			break
		}
		lastRecStart = off
		off = recEnd
	}
	if lastRecStart < 0 {
		t.Fatalf("no records found in segment %s", segPath)
	}
	return lastRecStart
}

func statSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return fi.Size()
}

// TestAppendFsyncCrash proves SC3/STOR-04 with the faultVFS page-cache fake: an
// entry whose active-segment fsync was suppressed (killed after Write, before
// the fsync landed) is ABSENT after a Crash + reopen, while a fully-synced
// Append survives the same Crash.
//
// NON-VACUOUSNESS (T-6/Pitfall 3): deleting the `activeFile.Sync()` call from
// Append (file.go) makes the ABSENT assertion below fail — the suppressed write
// would then be treated as durable. That is the proof this test is real, not a
// clean-shutdown flush. Verified by hand during 08-05 execution.
func TestAppendFsyncCrash(t *testing.T) {
	fs := newFaultVFS()
	dir := t.TempDir()

	s, err := openWith(fs, dir)
	if err != nil {
		t.Fatalf("openWith: %v", err)
	}
	// E1: a NORMAL, fully-synced append — creates the segment and makes its
	// header + record durable so the reopen has a valid segment to recover.
	e1 := raft.Entry{Term: 1, Index: 1, Data: []byte("committed")}
	if err := s.Append([]raft.Entry{e1}); err != nil {
		t.Fatalf("Append e1: %v", err)
	}

	// E2: arm suppressNextSync so Append's activeFile.Sync() is a no-op — E2's
	// bytes stay in the unsynced (dirty-page) buffer. Append still returns nil
	// (it believes it synced); a real process would be killed here.
	fs.suppressNextSync = true
	e2 := raft.Entry{Term: 1, Index: 2, Data: []byte("lost")}
	if err := s.Append([]raft.Entry{e2}); err != nil {
		t.Fatalf("Append e2 (suppressed sync): %v", err)
	}

	// Power loss: drop every unsynced byte.
	fs.Crash()

	// Reopen on the SAME faultVFS: recovery sees only durable bytes.
	s2, err := openWith(fs, dir)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer func() { _ = s2.Close() }()

	last, err := s2.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if last != 1 {
		t.Fatalf("after fsync-crash, LastIndex=%d, want 1 (E2 must be ABSENT)", last)
	}
	got, err := s2.Entries(1, 2)
	if err != nil {
		t.Fatalf("Entries(1,2): %v", err)
	}
	if len(got) != 1 || got[0].Index != 1 || string(got[0].Data) != "committed" {
		t.Fatalf("recovered %+v, want exactly [E1 committed]", got)
	}
}

// TestAppendSyncedSurvivesCrash is the non-vacuous POSITIVE control for SC3: a
// fully-synced Append survives a Crash, proving Crash() does not simply wipe
// everything (which would make TestAppendFsyncCrash pass vacuously).
func TestAppendSyncedSurvivesCrash(t *testing.T) {
	fs := newFaultVFS()
	dir := t.TempDir()

	s, err := openWith(fs, dir)
	if err != nil {
		t.Fatalf("openWith: %v", err)
	}
	e := raft.Entry{Term: 2, Index: 1, Data: []byte("durable")}
	if err := s.Append([]raft.Entry{e}); err != nil { // normal Sync completes
		t.Fatalf("Append: %v", err)
	}

	fs.Crash() // the synced write must survive

	s2, err := openWith(fs, dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()

	last, err := s2.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if last != 1 {
		t.Fatalf("synced entry lost across Crash: LastIndex=%d, want 1", last)
	}
	got, err := s2.Entries(1, 2)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if len(got) != 1 || got[0].Term != 2 || string(got[0].Data) != "durable" {
		t.Fatalf("recovered %+v, want [E durable term=2]", got)
	}
}

// TestHardStateRenameCrash proves SC4/STOR-05: a crash BETWEEN the tmp
// Write+Sync and the atomic Rename leaves the PREVIOUS HardState intact — the
// orphan tmp never corrupts the read path (LoadHardState reads only the final
// file). The completed-save positive control shows the new state lands when the
// Rename+SyncDir do run.
func TestHardStateRenameCrash(t *testing.T) {
	fs := newFaultVFS()
	dir := t.TempDir()

	s, err := openWith(fs, dir)
	if err != nil {
		t.Fatalf("openWith: %v", err)
	}
	hsPrev := raft.HardState{CurrentTerm: 3, VotedFor: "n1", Commit: 5}
	if err := s.SaveHardState(hsPrev); err != nil { // fully durable
		t.Fatalf("SaveHardState hsPrev: %v", err)
	}

	// Arm the crash at the NEXT Rename: SaveHardState will Create+Write+Sync the
	// tmp, then die at the rename(2) syscall. The returned error is expected
	// (the process "died") and intentionally ignored.
	fs.crashBeforeNextRename = true
	hsNew := raft.HardState{CurrentTerm: 4, VotedFor: "n2", Commit: 9}
	_ = s.SaveHardState(hsNew) // errors at Rename; the store has "crashed"

	// Reopen: LoadHardState must read the PREVIOUS state; the orphan tmp is gone.
	s2, err := openWith(fs, dir)
	if err != nil {
		t.Fatalf("reopen after rename-crash: %v", err)
	}
	defer func() { _ = s2.Close() }()

	got, err := s2.LoadHardState()
	if err != nil {
		t.Fatalf("LoadHardState after rename-crash: %v", err)
	}
	if got != hsPrev {
		t.Fatalf("after rename-crash LoadHardState=%+v, want previous %+v", got, hsPrev)
	}

	// POSITIVE control: a COMPLETED SaveHardState(hsNew) + Crash yields hsNew.
	if err := s2.SaveHardState(hsNew); err != nil {
		t.Fatalf("SaveHardState hsNew (completed): %v", err)
	}
	fs.Crash()
	s3, err := openWith(fs, dir)
	if err != nil {
		t.Fatalf("reopen after completed save: %v", err)
	}
	defer func() { _ = s3.Close() }()
	got2, err := s3.LoadHardState()
	if err != nil {
		t.Fatalf("LoadHardState after completed save: %v", err)
	}
	if got2 != hsNew {
		t.Fatalf("after completed save LoadHardState=%+v, want %+v", got2, hsNew)
	}
}

// TestRolloverDirFsync proves SC5/Per-3: a segment rollover fsyncs the parent
// directory (SyncDir) BEFORE the Append that triggered it returns, so the new
// segment's directory entry is durable before the entry is acked. It also shows
// the rolled-over entry survives a Crash after its Append+Sync.
func TestRolloverDirFsync(t *testing.T) {
	fs := newFaultVFS()
	dir := t.TempDir()

	s, err := openWith(fs, dir)
	if err != nil {
		t.Fatalf("openWith: %v", err)
	}
	// Force a rollover on every entry after the first: one entry per segment.
	s.mu.Lock()
	s.maxEntriesPerSegment = 1
	s.mu.Unlock()

	// First Append creates segment 1 (one SyncDir on create).
	if err := s.Append([]raft.Entry{{Term: 1, Index: 1, Data: []byte("a")}}); err != nil {
		t.Fatalf("Append 1: %v", err)
	}

	// Record the spy, then do the Append that rolls into segment 2. The rollover
	// SyncDir must have run BY THE TIME this Append returns.
	fs.mu.Lock()
	before := fs.syncDirCount
	fs.mu.Unlock()

	if err := s.Append([]raft.Entry{{Term: 1, Index: 2, Data: []byte("b")}}); err != nil {
		t.Fatalf("Append 2 (rollover): %v", err)
	}

	fs.mu.Lock()
	after := fs.syncDirCount
	fs.mu.Unlock()
	if after <= before {
		t.Fatalf("rollover did not SyncDir before Append returned: syncDirCount %d -> %d", before, after)
	}

	// The rolled-over entry (Append+Sync completed) survives a Crash.
	fs.Crash()
	s2, err := openWith(fs, dir)
	if err != nil {
		t.Fatalf("reopen after rollover crash: %v", err)
	}
	defer func() { _ = s2.Close() }()
	last, err := s2.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if last != 2 {
		t.Fatalf("after rollover crash LastIndex=%d, want 2 (both entries durable)", last)
	}
	got, err := s2.Entries(1, 3)
	if err != nil {
		t.Fatalf("Entries(1,3): %v", err)
	}
	if len(got) != 2 || string(got[0].Data) != "a" || string(got[1].Data) != "b" {
		t.Fatalf("recovered %+v, want [a b] across two segments", got)
	}
}
