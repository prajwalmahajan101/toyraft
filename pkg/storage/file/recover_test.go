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
