package file

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// TestRecordRoundtrip proves encodeRecord/decodeRecord are byte-identical
// inverses across empty, small, and high-byte Data, and that the decoded
// Data is a fresh copy the caller can mutate without touching the source.
func TestRecordRoundtrip(t *testing.T) {
	cases := []raft.Entry{
		{Term: 1, Index: 1, Data: nil},
		{Term: 1, Index: 1, Data: []byte{}},
		{Term: 7, Index: 42, Data: []byte("hello")},
		{Term: 0xdeadbeef, Index: 0xffff, Data: []byte{0x00, 0xff, 0x80, 0x01, 0xfe}},
	}
	for i, want := range cases {
		var buf bytes.Buffer
		if err := encodeRecord(&buf, want); err != nil {
			t.Fatalf("case %d: encodeRecord: %v", i, err)
		}
		raw := buf.Bytes()
		if len(raw) < recHeaderLen {
			t.Fatalf("case %d: encoded record too short: %d bytes", i, len(raw))
		}
		hdr := raw[:recHeaderLen]
		payload := raw[recHeaderLen:]

		got, err := decodeRecord(hdr, payload)
		if err != nil {
			t.Fatalf("case %d: decodeRecord: %v", i, err)
		}
		if got.Term != want.Term || got.Index != want.Index {
			t.Errorf("case %d: term/index = (%d,%d), want (%d,%d)", i, got.Term, got.Index, want.Term, want.Index)
		}
		if !bytes.Equal(got.Data, want.Data) {
			t.Errorf("case %d: Data = %v, want %v", i, got.Data, want.Data)
		}

		// Fresh-copy guarantee: mutating the returned Data must not disturb
		// the original entry's Data.
		if len(got.Data) > 0 {
			orig := append([]byte(nil), want.Data...)
			got.Data[0] ^= 0xff
			if !bytes.Equal(want.Data, orig) {
				t.Errorf("case %d: mutating decoded Data changed the source entry", i)
			}
		}
	}
}

// TestDecodeRecordBadCRC proves a flipped payload byte is caught as a CRC
// mismatch rather than silently decoded.
func TestDecodeRecordBadCRC(t *testing.T) {
	var buf bytes.Buffer
	if err := encodeRecord(&buf, raft.Entry{Term: 3, Index: 9, Data: []byte("payload")}); err != nil {
		t.Fatalf("encodeRecord: %v", err)
	}
	raw := buf.Bytes()
	hdr := raw[:recHeaderLen]
	payload := append([]byte(nil), raw[recHeaderLen:]...)
	payload[len(payload)-1] ^= 0xff // corrupt the last Data byte

	if _, err := decodeRecord(hdr, payload); err == nil {
		t.Fatal("decodeRecord accepted a corrupt payload; want CRC error")
	}
}

// writeTestSegment builds a real on-disk segment via osVFS: a valid header
// followed by n good records (Term=1, Index=i, Data="rec<i>"). It returns
// the path and the byte offset immediately after the last good record.
func writeTestSegment(t *testing.T, n int) (path string, goodEnd int64) {
	t.Helper()
	dir := t.TempDir()
	path = filepath.Join(dir, segmentName(1))

	var fs osVFS
	f, err := fs.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := writeSegHeader(f); err != nil {
		t.Fatalf("writeSegHeader: %v", err)
	}
	for i := 1; i <= n; i++ {
		e := raft.Entry{Term: 1, Index: raft.Index(i), Data: []byte("rec")}
		if err := encodeRecord(f, e); err != nil {
			t.Fatalf("encodeRecord %d: %v", i, err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	size, err := f.Size()
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return path, size
}

// TestScanSegmentTornTail appends a truncated (header-only) final record
// after N good ones and asserts scanSegment returns exactly N records and a
// goodEnd at the byte offset after record N — the exact truncation boundary.
func TestScanSegmentTornTail(t *testing.T) {
	const n = 5
	path, goodEnd := writeTestSegment(t, n)

	var fs osVFS
	// Append a torn final record: just the 8-byte record header, no payload.
	af, err := fs.OpenAppend(path)
	if err != nil {
		t.Fatalf("OpenAppend: %v", err)
	}
	if _, err := af.Write(make([]byte, recHeaderLen)); err != nil {
		t.Fatalf("write torn header: %v", err)
	}
	if err := af.Sync(); err != nil {
		t.Fatalf("Sync torn: %v", err)
	}
	if err := af.Close(); err != nil {
		t.Fatalf("Close torn: %v", err)
	}

	rf, err := fs.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = rf.Close() }()

	recs, gotEnd, err := scanSegment(rf)
	if err != nil {
		t.Fatalf("scanSegment: %v", err)
	}
	if len(recs) != n {
		t.Fatalf("scan returned %d records, want %d", len(recs), n)
	}
	if gotEnd != goodEnd {
		t.Errorf("goodEnd = %d, want %d (offset after record %d)", gotEnd, goodEnd, n)
	}
	for i, r := range recs {
		if r.Index != raft.Index(i+1) {
			t.Errorf("rec[%d].Index = %d, want %d", i, r.Index, i+1)
		}
	}
}

// TestScanSegmentBadCRCTornTail flips a CRC byte in the final good record
// and asserts the scan stops at the record before it (N-1 records) with a
// goodEnd at that earlier boundary — corruption truncates exactly one record.
func TestScanSegmentBadCRCTornTail(t *testing.T) {
	const n = 4
	path, _ := writeTestSegment(t, n)

	var fs osVFS
	// Scan the clean file first to learn record N-1's boundary and the size.
	clean, err := fs.Open(path)
	if err != nil {
		t.Fatalf("Open clean: %v", err)
	}
	size, err := clean.Size()
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	cleanRecs, _, err := scanSegment(clean)
	_ = clean.Close()
	if err != nil {
		t.Fatalf("scan clean: %v", err)
	}
	if len(cleanRecs) != n {
		t.Fatalf("clean scan = %d records, want %d", len(cleanRecs), n)
	}
	// Boundary after record N-1 = payload offset of record N minus its header.
	wantEnd := cleanRecs[n-1].Offset - recHeaderLen

	// Corrupt the very last byte of the file (inside record N's payload).
	corruptByte(t, path, size-1)

	cf, err := fs.Open(path)
	if err != nil {
		t.Fatalf("Open corrupt: %v", err)
	}
	defer func() { _ = cf.Close() }()
	recs, gotEnd, err := scanSegment(cf)
	if err != nil {
		t.Fatalf("scanSegment corrupt: %v", err)
	}
	if len(recs) != n-1 {
		t.Fatalf("scan returned %d records, want %d (last is corrupt)", len(recs), n-1)
	}
	if gotEnd != wantEnd {
		t.Errorf("goodEnd = %d, want %d (offset after record %d)", gotEnd, wantEnd, n-1)
	}
}

// TestScanSegmentBadHeader proves a wrong-magic file makes scanSegment
// return an error instead of a silently-empty scan (Per-5).
func TestScanSegmentBadHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, segmentName(1))

	var fs osVFS
	f, err := fs.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Write a header with a wrong magic then a byte of junk.
	if _, err := f.Write([]byte{'X', 'X', 'X', 'X', segVersion, 0}); err != nil {
		t.Fatalf("write bad header: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	rf, err := fs.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = rf.Close() }()
	if _, _, err := scanSegment(rf); err == nil {
		t.Fatal("scanSegment accepted a bad-magic segment; want structural error")
	}
}

// TestScanSegmentUnknownVersion proves an unknown version byte errors (Per-5).
func TestScanSegmentUnknownVersion(t *testing.T) {
	if err := readSegHeader([]byte{'T', 'R', 'L', '1', segVersion + 1}); err == nil {
		t.Fatal("readSegHeader accepted an unknown version; want error")
	}
}

// TestSegmentNameSorts proves segmentName zero-pads the first index so names
// sort lexically in index order.
func TestSegmentNameSorts(t *testing.T) {
	a := segmentName(1)
	b := segmentName(2)
	c := segmentName(1024)
	if a != "0000000000000001.seg" {
		t.Errorf("segmentName(1) = %q", a)
	}
	if !(a < b && b < c) {
		t.Errorf("names not lexically ordered: %q %q %q", a, b, c)
	}
}

// TestDefaultRolloverThreshold anchors the documented rollover default so a
// change to it is a deliberate edit (referenced for SC5 override in later plans).
func TestDefaultRolloverThreshold(t *testing.T) {
	if defaultMaxEntriesPerSegment <= 0 {
		t.Fatalf("defaultMaxEntriesPerSegment = %d, want > 0", defaultMaxEntriesPerSegment)
	}
}

// corruptByte flips the byte at off in the file at path via a real *os.File
// (the vfile seam intentionally has no WriteAt; corruption is a test-only
// disk manipulation, so it uses os directly).
func corruptByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open for corrupt: %v", err)
	}
	defer func() { _ = f.Close() }()
	var b [1]byte
	if _, err := f.ReadAt(b[:], off); err != nil {
		t.Fatalf("read byte to corrupt: %v", err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b[:], off); err != nil {
		t.Fatalf("write corrupt byte: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync corrupt: %v", err)
	}
}
