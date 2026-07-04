package file

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	"github.com/prajwalmahajan101/toyraft/pkg/storage"
)

// Storage is the durable, append-only, fsynced implementation of
// storage.Storage (LLD §3; STOR-03/STOR-04/STOR-06; ADR-0013). It is the
// on-disk counterpart to pkg/storage/memory and a behavioral drop-in: it
// passes the identical pkg/storage/storagetest conformance suite while
// adding crash-recoverable durability under the frozen 10-method contract.
//
// Concurrency: a single sync.Mutex serialises every method (ADR-0004's
// single-mutex discipline, mirrored from the memory impl). There is no
// RWMutex split — the read path touches disk (ReaderAt) so the simpler
// single-mutex model is preferred over lock churn.
//
// Durability model: the on-disk log is a sequence of segment files
// (segment.go framing). An in-RAM index (idx) maps every logical entry to
// its {segment, payload offset, length} so reads seek directly and recovery
// need not re-scan. Append fsyncs the active segment before returning
// (STOR-04); a segment rollover fsyncs the parent directory before the next
// Append returns (SC5/Per-3); TruncateSuffix fsyncs before returning.
//
// The zero value is NOT usable — construction goes through New(dir), which
// runs crash recovery (torn-tail truncation) and may fail.
type Storage struct {
	mu  sync.Mutex
	fs  vfs    // durability seam (osVFS in prod, faultVFS in 08-05 tests)
	dir string // directory holding the *.seg segment files

	// idx is the in-RAM index: one entry per logical log entry, in index
	// order. idx[i] describes the entry with logical index i+firstIndex
	// (firstIndex == 1 in v1: no compaction). It lets Entries()/Term() seek
	// straight to a record without re-scanning segments.
	idx []idxEntry

	// active is the segment currently open for append (the last segment on
	// disk). activeFile is its open handle; activeCount is how many records
	// it holds (for the rollover threshold). All three are zero/nil until
	// the first Append creates segment segmentName(1).
	active      string
	activeFile  vfile
	activeCount int

	// maxEntriesPerSegment is the rollover threshold. Defaults to
	// defaultMaxEntriesPerSegment; tests shrink it to force rollover (SC5).
	maxEntriesPerSegment int
}

// idxEntry locates one logical log entry on disk (rebuilt on recovery and
// extended on every Append). Offset/length address the record's PAYLOAD
// (Term‖Index‖Data) within its segment, so Entries() reads via ReaderAt.
type idxEntry struct {
	index   raft.Index
	term    raft.Term
	segment string // base name of the segment file holding this entry
	offset  int64  // byte offset of the payload start within the segment
	length  int    // payload length in bytes (payloadFixedLen + len(Data))
}

// Compile-time interface assertion (repo convention; catches drift if
// storage.Storage grows). The HardState methods (SaveHardState/LoadHardState)
// are stubbed here and replaced with the real fsync+rename impl in 08-04.
var _ storage.Storage = (*Storage)(nil)

// segSuffix is the segment file extension; recovery filters ReadDir on it.
const segSuffix = ".seg"

// New opens (or creates) an on-disk log rooted at dir, running crash
// recovery — it truncates any torn tail left by a crash mid-Append and
// rebuilds the in-RAM index from the surviving records. Production callers
// use this; the fault tests use openWith directly.
func New(dir string) (*Storage, error) {
	return openWith(osVFS{}, dir)
}

// openWith is the construction seam shared by New (osVFS) and the 08-05
// crash-injection tests (faultVFS). It ensures dir exists, then runs
// recovery. The vfs argument is the sole injection point — no os.* call
// happens outside it, so a fault fake can model "lose all unsynced writes".
func openWith(fs vfs, dir string) (*Storage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("file storage: create dir %q: %w", dir, err)
	}
	s := &Storage{
		fs:                   fs,
		dir:                  dir,
		maxEntriesPerSegment: defaultMaxEntriesPerSegment,
	}
	if err := s.recover(); err != nil {
		return nil, err
	}
	return s, nil
}

// recover scans every segment in dir (ascending), truncates a torn tail on
// the last-written segment, rebuilds the in-RAM index from the surviving
// records, and reopens the last segment for append. An empty dir yields an
// empty log (LastIndex 0); the first Append then creates segmentName(1).
//
// Torn-tail handling: scanSegment stops cleanly at the first short-read or
// bad-CRC record and reports goodEnd (the offset after the last good
// record). If goodEnd is below the file size a crash tore the tail mid-write
// — we Truncate the segment to goodEnd, Sync it, and SyncDir(dir) so the
// truncation is itself durable before recovery returns (STOR-06/SC2).
func (s *Storage) recover() error {
	names, err := s.fs.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("file storage: readdir %q: %w", s.dir, err)
	}
	segs := make([]string, 0, len(names))
	for _, n := range names {
		if strings.HasSuffix(n, segSuffix) {
			segs = append(segs, n)
		}
	}
	// ReadDir already returns names sorted; segmentName is lexically ordered
	// by first index, so segs is in log order.

	for _, name := range segs {
		recs, goodEnd, serr := s.scanOne(name)
		if serr != nil {
			return serr
		}
		for _, r := range recs {
			s.idx = append(s.idx, idxEntry{
				index:   r.Index,
				term:    r.Term,
				segment: name,
				offset:  r.Offset,
				length:  payloadFixedLen + r.DataLen,
			})
		}
		// The last segment becomes the active append target. Its record
		// count seeds the rollover counter; goodEnd is the truncated tail.
		s.active = name
		s.activeCount = len(recs)
		_ = goodEnd // goodEnd already applied by scanOne's truncate
	}

	// Reopen the last segment for append so the next Append writes into it.
	if s.active != "" {
		f, oerr := s.fs.OpenAppend(s.path(s.active))
		if oerr != nil {
			return fmt.Errorf("file storage: reopen active segment %q: %w", s.active, oerr)
		}
		s.activeFile = f
	}
	return nil
}

// scanOne opens a single segment read/append, scans it, and — if the scan
// found a torn tail (goodEnd below the file size) — truncates the segment to
// goodEnd and fsyncs both the file and the parent directory so the repair is
// durable. It returns the surviving records and goodEnd.
func (s *Storage) scanOne(name string) (recs []scanRec, goodEnd int64, err error) {
	f, err := s.fs.OpenAppend(s.path(name))
	if err != nil {
		return nil, 0, fmt.Errorf("file storage: open segment %q: %w", name, err)
	}
	defer func() { err = errors.Join(err, f.Close()) }()

	size, err := f.Size()
	if err != nil {
		return nil, 0, fmt.Errorf("file storage: stat segment %q: %w", name, err)
	}
	recs, goodEnd, err = scanSegment(f)
	if err != nil {
		return nil, 0, fmt.Errorf("file storage: scan segment %q: %w", name, err)
	}
	if goodEnd < size {
		// A crash tore the tail mid-write: truncate to the last good record
		// and make the repair durable before recovery proceeds (STOR-06).
		if terr := f.Truncate(goodEnd); terr != nil {
			return nil, 0, fmt.Errorf("file storage: truncate torn tail of %q: %w", name, terr)
		}
		if serr := f.Sync(); serr != nil {
			return nil, 0, fmt.Errorf("file storage: sync truncated %q: %w", name, serr)
		}
		if derr := s.fs.SyncDir(s.dir); derr != nil {
			return nil, 0, fmt.Errorf("file storage: syncdir after truncate: %w", derr)
		}
	}
	return recs, goodEnd, nil
}

// path joins the storage dir with a segment base name.
func (s *Storage) path(name string) string { return filepath.Join(s.dir, name) }

// Close syncs and closes the active segment handle. Errors from Sync and
// Close are joined so neither is dropped (errcheck is blocking on the write
// path). Safe to call when no segment is open (fresh empty log).
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeFile == nil {
		return nil
	}
	err := errors.Join(s.activeFile.Sync(), s.activeFile.Close())
	s.activeFile = nil
	return err
}

// Snapshot returns storage.ErrSnapshotUnsupported in v1 (LLD §5 Global
// Invariant 5). v2 will populate without changing the signature.
func (s *Storage) Snapshot() ([]byte, raft.Index, error) {
	return nil, 0, storage.ErrSnapshotUnsupported
}

// Restore returns storage.ErrSnapshotUnsupported in v1 (LLD §5 Global
// Invariant 5). v2 will populate without changing the signature.
func (s *Storage) Restore(data []byte) error {
	return storage.ErrSnapshotUnsupported
}

// --- Temporary stubs replaced in later plans/tasks ---
//
// These keep pkg/storage/file compiling as a standalone package (the
// interface assertion above needs every method present) before the real
// implementations land. Append/TruncateSuffix and the four read methods are
// implemented in this plan's Tasks 2 and 3; SaveHardState/LoadHardState land
// in 08-04. Each is replaced in place — do not add fields for them here.

// SaveHardState is a temporary no-op stub; the real fsync+rename impl lands
// in 08-04 (hardstate.go).
func (s *Storage) SaveHardState(hs raft.HardState) error { return nil }

// LoadHardState is a temporary no-op stub; the real impl lands in 08-04.
func (s *Storage) LoadHardState() (raft.HardState, error) { return raft.HardState{}, nil }

// errNonContiguous is wrapped by Append when entries do not start at
// LastIndex()+1 or are not strictly increasing by 1 — matching the memory
// impl's contract (08-RESEARCH §1). Same-package reference (Append) keeps
// the `unused` linter quiet.
var errNonContiguous = errors.New("file storage: non-contiguous append")

// lastIndexLocked returns the largest logical index in the log, or 0 if
// empty. Caller holds s.mu.
func (s *Storage) lastIndexLocked() raft.Index {
	if len(s.idx) == 0 {
		return 0
	}
	return s.idx[len(s.idx)-1].index
}

// Append persists entries contiguously and fsyncs the active segment before
// returning (STOR-04/REPL-09/P0-4). Entries MUST start at LastIndex()+1 and
// increase by 1; otherwise the call returns an error wrapping errNonContiguous
// and leaves the on-disk state unchanged (atomic per call — the whole batch
// is validated BEFORE any record is written).
//
// Rollover: when the active segment would exceed maxEntriesPerSegment, a new
// segment (segmentName of the next index) is created, its header written, and
// — CRITICAL (SC5/Per-3) — the parent directory is fsynced so the new
// segment's directory entry is durable BEFORE this Append returns. Only after
// the active-segment Sync succeeds is the in-RAM index extended, so memory
// never claims a durability the disk lacks (if Sync fails the index is left
// untouched).
func (s *Storage) Append(entries []raft.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(entries) == 0 {
		return nil
	}

	// Validate the ENTIRE batch before writing anything (atomic per call).
	expected := s.lastIndexLocked() + 1
	if entries[0].Index != expected {
		return fmt.Errorf("file storage: append at index %d, got %d: %w", expected, entries[0].Index, errNonContiguous)
	}
	for i := 0; i+1 < len(entries); i++ {
		if entries[i+1].Index != entries[i].Index+1 {
			return fmt.Errorf("file storage: append at index %d, got %d: %w", entries[i].Index+1, entries[i+1].Index, errNonContiguous)
		}
	}

	// Write every record, rolling to a new segment when the counter would
	// exceed the threshold. Stage the new index entries and only commit them
	// to s.idx after the durability Sync below.
	staged := make([]idxEntry, 0, len(entries))
	for _, e := range entries {
		if s.activeFile == nil || s.activeCount >= s.maxEntriesPerSegment {
			if err := s.rolloverLocked(e.Index); err != nil {
				return err
			}
		}
		off, err := s.writeRecordLocked(e)
		if err != nil {
			return err
		}
		staged = append(staged, idxEntry{
			index:   e.Index,
			term:    e.Term,
			segment: s.active,
			offset:  off,
			length:  payloadFixedLen + len(e.Data),
		})
		s.activeCount++
	}

	// fsync the active segment BEFORE returning (durability precedes success).
	if err := s.activeFile.Sync(); err != nil {
		return fmt.Errorf("file storage: sync on append: %w", err)
	}
	s.idx = append(s.idx, staged...)
	return nil
}

// rolloverLocked closes the current active segment (if any), creates a new
// segment named for firstIndex, writes its header, and fsyncs the parent
// directory so the new segment's directory entry is durable BEFORE the caller
// (Append) returns (SC5/Per-3). Caller holds s.mu.
func (s *Storage) rolloverLocked(firstIndex raft.Index) error {
	if s.activeFile != nil {
		if err := errors.Join(s.activeFile.Sync(), s.activeFile.Close()); err != nil {
			return fmt.Errorf("file storage: close segment on rollover: %w", err)
		}
		s.activeFile = nil
	}
	name := segmentName(firstIndex)
	f, err := s.fs.Create(s.path(name))
	if err != nil {
		return fmt.Errorf("file storage: create segment %q: %w", name, err)
	}
	if err := writeSegHeader(f); err != nil {
		return fmt.Errorf("file storage: write segment header %q: %w", name, errors.Join(err, f.Close()))
	}
	// Per-3: the new segment's directory entry must be durable before the
	// Append that triggered this rollover returns.
	if err := s.fs.SyncDir(s.dir); err != nil {
		return fmt.Errorf("file storage: syncdir after create %q: %w", name, errors.Join(err, f.Close()))
	}
	s.active = name
	s.activeFile = f
	s.activeCount = 0
	return nil
}

// writeRecordLocked encodes e into the active segment and returns the byte
// offset of the record's PAYLOAD (used by the in-RAM index for later reads).
// Caller holds s.mu and guarantees s.activeFile is non-nil.
func (s *Storage) writeRecordLocked(e raft.Entry) (int64, error) {
	size, err := s.activeFile.Size()
	if err != nil {
		return 0, fmt.Errorf("file storage: size before write: %w", err)
	}
	// The payload starts recHeaderLen bytes after the record start (the
	// current end of file), matching scanSegment's payloadOff arithmetic.
	payloadOff := size + recHeaderLen
	if err := encodeRecord(s.activeFile, e); err != nil {
		return 0, fmt.Errorf("file storage: write record at index %d: %w", e.Index, err)
	}
	return payloadOff, nil
}

// TruncateSuffix discards every entry with index >= from and fsyncs before
// returning (STOR-04). It is a no-op (nil) when from > LastIndex(), and an
// error when from < 1. On disk it truncates the segment holding `from` at
// that entry's record start, Removes any wholly-later segments, then Syncs
// the truncated segment and SyncDir(dir) before returning.
func (s *Storage) TruncateSuffix(from raft.Index) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	last := s.lastIndexLocked()
	if from > last {
		return nil
	}
	if from < 1 {
		return fmt.Errorf("file storage: truncate at invalid index %d", from)
	}

	// Locate the in-RAM index position of `from` (idx is index-ordered from
	// firstIndex==1, so position == from-1 in v1).
	pos := int(from - 1)
	target := s.idx[pos]

	// Every segment strictly after target.segment is wholly truncated away.
	// Collect their names (unique, in order) so they can be Removed.
	laterSegs := make([]string, 0)
	seen := map[string]bool{target.segment: true}
	for _, ie := range s.idx[pos:] {
		if ie.segment != target.segment && !seen[ie.segment] {
			seen[ie.segment] = true
			laterSegs = append(laterSegs, ie.segment)
		}
	}

	// Truncate the target segment at the record start of `from` (the payload
	// offset minus the record header).
	cut := target.offset - recHeaderLen
	tf, err := s.fs.OpenAppend(s.path(target.segment))
	if err != nil {
		return fmt.Errorf("file storage: open segment %q for truncate: %w", target.segment, err)
	}
	if err := tf.Truncate(cut); err != nil {
		return fmt.Errorf("file storage: truncate %q to %d: %w", target.segment, cut, errors.Join(err, tf.Close()))
	}
	if err := tf.Sync(); err != nil {
		return fmt.Errorf("file storage: sync truncated %q: %w", target.segment, errors.Join(err, tf.Close()))
	}

	// Remove wholly-later segments. Close the current active handle first if
	// it points at one of them (it will be reset to the truncated tail).
	if s.activeFile != nil {
		if err := s.activeFile.Close(); err != nil {
			return fmt.Errorf("file storage: close active on truncate: %w", errors.Join(err, tf.Close()))
		}
		s.activeFile = nil
	}
	for _, name := range laterSegs {
		if err := s.fs.Remove(s.path(name)); err != nil {
			return fmt.Errorf("file storage: remove segment %q on truncate: %w", name, errors.Join(err, tf.Close()))
		}
	}
	// Make the truncation + removals durable (STOR-04).
	if err := s.fs.SyncDir(s.dir); err != nil {
		return fmt.Errorf("file storage: syncdir after truncate: %w", errors.Join(err, tf.Close()))
	}

	// The truncated segment becomes the active append target again. Recount
	// the entries it retains from the surviving in-RAM index.
	retained := 0
	for _, ie := range s.idx[:pos] {
		if ie.segment == target.segment {
			retained++
		}
	}
	s.idx = s.idx[:pos]
	s.active = target.segment
	s.activeFile = tf
	s.activeCount = retained
	return nil
}

// LastIndex returns the largest logical index in the log, or 0 if empty
// (LLD §3). Matches the memory impl exactly.
func (s *Storage) LastIndex() (raft.Index, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastIndexLocked(), nil
}

// FirstIndex returns 1 always — v1 has no compaction (LLD §3), matching the
// memory impl.
func (s *Storage) FirstIndex() (raft.Index, error) {
	return 1, nil
}

// Term returns the term of the entry at index, or 0 if index == 0 (the
// implicit pre-log sentinel). Returns an error wrapping io.ErrUnexpectedEOF
// if index > LastIndex() — the exact memory contract (08-RESEARCH §1). The
// term is served from the in-RAM index (no disk read needed).
func (s *Storage) Term(index raft.Index) (raft.Term, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index == 0 {
		return 0, nil
	}
	last := s.lastIndexLocked()
	if index > last {
		return 0, fmt.Errorf("file storage: term at index %d > LastIndex %d: %w", index, last, io.ErrUnexpectedEOF)
	}
	return s.idx[index-1].term, nil
}

// Entries returns the half-open range [lo, hi) (LLD §3), reading each record's
// payload from disk via ReaderAt so the returned entries are freshly
// allocated with fresh Data — the caller may mutate freely (conformance
// EntriesCallerCanMutate; 08-RESEARCH Open Question 2). Error contract mirrors
// memory exactly: lo < 1 || lo > hi is invalid; hi > LastIndex()+1 wraps
// io.ErrUnexpectedEOF; lo == hi returns an empty non-nil slice.
func (s *Storage) Entries(lo, hi raft.Index) ([]raft.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	last := s.lastIndexLocked()
	if lo < 1 || lo > hi {
		return nil, fmt.Errorf("file storage: invalid range [%d,%d)", lo, hi)
	}
	if hi > last+1 {
		return nil, fmt.Errorf("file storage: hi=%d > LastIndex+1=%d: %w", hi, last+1, io.ErrUnexpectedEOF)
	}
	if lo == hi {
		return []raft.Entry{}, nil
	}

	out := make([]raft.Entry, 0, hi-lo)
	for i := lo; i < hi; i++ {
		e, err := s.readEntryLocked(s.idx[i-1])
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

// readEntryLocked reads and decodes the record described by ie from disk. The
// decoded Entry.Data is a fresh copy (decodeRecord deep-copies), so the caller
// may mutate it. Caller holds s.mu.
func (s *Storage) readEntryLocked(ie idxEntry) (raft.Entry, error) {
	f, err := s.fs.Open(s.path(ie.segment))
	if err != nil {
		return raft.Entry{}, fmt.Errorf("file storage: open segment %q for read: %w", ie.segment, err)
	}
	defer func() { _ = f.Close() }() // read-only handle: Close error is not a durability failure

	// Read the actual on-disk record header (immediately before the payload)
	// so decodeRecord's CRC check verifies the bytes on disk, not a header we
	// synthesised — a silent bit-flip in the payload is thus caught on read.
	var hdr [recHeaderLen]byte
	if _, err := f.ReadAt(hdr[:], ie.offset-recHeaderLen); err != nil {
		return raft.Entry{}, fmt.Errorf("file storage: read header for entry %d from %q: %w", ie.index, ie.segment, err)
	}
	payload := make([]byte, ie.length)
	if _, err := f.ReadAt(payload, ie.offset); err != nil {
		return raft.Entry{}, fmt.Errorf("file storage: read entry %d from %q: %w", ie.index, ie.segment, err)
	}
	e, err := decodeRecord(hdr[:], payload)
	if err != nil {
		return raft.Entry{}, fmt.Errorf("file storage: decode entry %d from %q: %w", ie.index, ie.segment, err)
	}
	return e, nil
}
