package file

import (
	"errors"
	"fmt"
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

// Append is a temporary stub replaced by Task 2 of this plan.
func (s *Storage) Append(entries []raft.Entry) error { return nil }

// TruncateSuffix is a temporary stub replaced by Task 2 of this plan.
func (s *Storage) TruncateSuffix(from raft.Index) error { return nil }

// Entries is a temporary stub replaced by Task 3 of this plan.
func (s *Storage) Entries(lo, hi raft.Index) ([]raft.Entry, error) { return nil, nil }

// Term is a temporary stub replaced by Task 3 of this plan.
func (s *Storage) Term(index raft.Index) (raft.Term, error) { return 0, nil }

// FirstIndex is a temporary stub replaced by Task 3 of this plan.
func (s *Storage) FirstIndex() (raft.Index, error) { return 1, nil }

// LastIndex is a temporary stub replaced by Task 3 of this plan.
func (s *Storage) LastIndex() (raft.Index, error) { return 0, nil }
