package file

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// faultVFS is an IN-PROCESS page-cache fake modelling "lose every write since
// the last fsync" WITHOUT a real kill -9 (that is Phase 11). It is the proof
// engine behind SC3/SC4/SC5 and retires risk T-6.
//
// Page-cache model (08-RESEARCH §3 "faultVFS"):
//   - Each file carries two byte buffers: durable (survives Crash) and unsynced
//     (a dirty page — lost on Crash). Write appends to unsynced; the writing
//     process sees its own dirty pages via ReaderAt (durable ++ unsynced), so a
//     read-back before Sync succeeds pre-crash. Sync() promotes unsynced onto
//     durable and clears unsynced.
//   - Directory metadata (which base names exist) is itself split into durable
//     vs pending: Create/Rename/Remove mutate a PENDING view; SyncDir promotes
//     the pending metadata to durable AND bumps syncDirCount (the SC5 spy).
//   - Crash() drops every file's unsynced bytes, discards files whose creation
//     was never made durable by a SyncDir, and drops orphaned pre-rename tmp
//     state. After Crash() a fresh openWith(sameFaultVFS, dir) sees ONLY
//     durably-synced bytes + durably-created names.
//
// Everything here is referenced by recover_test.go so the `unused` linter (a
// blocking correctness lint) stays quiet — the Phase-7 bite (commit 719d4cf,
// 08-RESEARCH Pitfall 5). faultVFS is injected via openWith(fs, dir).
type faultVFS struct {
	mu sync.Mutex

	// files maps a full path to its page-cache state. A file present here but
	// whose name is not yet in durableNames is "created but not SyncDir'd" and
	// is dropped on Crash.
	files map[string]*faultFileState

	// durableNames is the set of file paths whose directory entry is durable
	// (has survived a SyncDir). pendingNames is the not-yet-fsynced metadata
	// delta (creates/renames/removes) applied on SyncDir and discarded on Crash.
	durableNames map[string]bool
	pendingNames map[string]bool // true=present, false=removed (tombstone)

	// syncDirCount is the SC5 spy: number of SyncDir calls observed. Tests read
	// it before/after a rollover-triggering Append to prove the parent dir was
	// fsynced before the entry was acked.
	syncDirCount int

	// suppressNextSync, when set, makes the NEXT faultFile.Sync() a no-op
	// (unsynced bytes are NOT promoted). This models "the process was killed
	// after Write returned but before the fsync landed" for the SC3 test.
	suppressNextSync bool

	// crashBeforeNextRename, when set, makes the NEXT Rename crash the store
	// (drop unsynced + un-durable state) and return an error INSTEAD of
	// performing the swap — modelling a process death at the rename(2) syscall,
	// after the tmp file was written+fsynced but before the atomic swap landed
	// (the SC4 window). One-shot.
	crashBeforeNextRename bool
}

// faultFileState is the durable/unsynced page-cache state for one path, shared
// between every open handle to that path (writes through one handle are visible
// to a reopened handle, as with a real OS page cache).
type faultFileState struct {
	durable  []byte // survives Crash
	unsynced []byte // dirty pages; dropped on Crash
	// durablyCreated records whether this file's existence has been made
	// durable via SyncDir. A file Create'd but never SyncDir'd is dropped on
	// Crash (the pre-rename tmp / orphan case).
	durablyCreated bool
}

func newFaultVFS() *faultVFS {
	return &faultVFS{
		files:        make(map[string]*faultFileState),
		durableNames: make(map[string]bool),
		pendingNames: make(map[string]bool),
	}
}

// Compile-time assertion that faultVFS satisfies vfs.
var _ vfs = (*faultVFS)(nil)

// exists reports whether path is currently visible (durable name not tombstoned
// by a pending remove, or a pending create). Caller holds fv.mu.
func (fv *faultVFS) exists(path string) bool {
	if present, ok := fv.pendingNames[path]; ok {
		return present
	}
	return fv.durableNames[path]
}

func (fv *faultVFS) Open(name string) (vfile, error) {
	fv.mu.Lock()
	defer fv.mu.Unlock()
	if !fv.exists(name) {
		return nil, os.ErrNotExist
	}
	st := fv.files[name]
	if st == nil {
		return nil, os.ErrNotExist
	}
	return &faultFile{fv: fv, st: st}, nil
}

func (fv *faultVFS) Create(name string) (vfile, error) {
	fv.mu.Lock()
	defer fv.mu.Unlock()
	// O_TRUNC: a fresh empty page-cache state, replacing any prior contents.
	st := &faultFileState{}
	fv.files[name] = st
	fv.pendingNames[name] = true // creation is pending until SyncDir
	return &faultFile{fv: fv, st: st}, nil
}

func (fv *faultVFS) OpenAppend(name string) (vfile, error) {
	fv.mu.Lock()
	defer fv.mu.Unlock()
	if !fv.exists(name) {
		return nil, os.ErrNotExist
	}
	st := fv.files[name]
	if st == nil {
		return nil, os.ErrNotExist
	}
	return &faultFile{fv: fv, st: st}, nil
}

// Rename atomically remaps oldpath onto newpath in the PENDING metadata view
// (the bytes at oldpath move to newpath; durability of the swap needs a
// following SyncDir). Models rename(2): the new name points at the fully-written
// tmp payload, the old name disappears.
func (fv *faultVFS) Rename(oldpath, newpath string) error {
	fv.mu.Lock()
	defer fv.mu.Unlock()
	if fv.crashBeforeNextRename {
		fv.crashBeforeNextRename = false
		fv.crashLocked()
		return os.ErrClosed // the rename(2) never completed (process died)
	}
	if !fv.exists(oldpath) {
		return os.ErrNotExist
	}
	fv.files[newpath] = fv.files[oldpath]
	delete(fv.files, oldpath)
	fv.pendingNames[oldpath] = false // tombstone the old name
	fv.pendingNames[newpath] = true  // new name pending until SyncDir
	return nil
}

// SyncDir promotes the pending metadata delta to durable and bumps the spy
// counter (SC5). After this returns every currently-visible file is durably
// created and every pending remove is applied.
func (fv *faultVFS) SyncDir(dir string) error {
	fv.mu.Lock()
	defer fv.mu.Unlock()
	fv.syncDirCount++
	for name, present := range fv.pendingNames {
		if present {
			fv.durableNames[name] = true
			if st := fv.files[name]; st != nil {
				st.durablyCreated = true
			}
		} else {
			delete(fv.durableNames, name)
		}
	}
	fv.pendingNames = make(map[string]bool)
	return nil
}

func (fv *faultVFS) Remove(name string) error {
	fv.mu.Lock()
	defer fv.mu.Unlock()
	if !fv.exists(name) {
		return os.ErrNotExist
	}
	delete(fv.files, name)
	fv.pendingNames[name] = false // removal pending until SyncDir
	return nil
}

// ReadDir returns the sorted base names currently visible in dir, mirroring
// osVFS.ReadDir (base names, lexically sorted) so recovery discovers segments
// identically on the fake.
func (fv *faultVFS) ReadDir(dir string) ([]string, error) {
	fv.mu.Lock()
	defer fv.mu.Unlock()
	seen := make(map[string]bool)
	// Durable names, minus pending tombstones, plus pending creates.
	for name := range fv.durableNames {
		seen[name] = true
	}
	for name, present := range fv.pendingNames {
		if present {
			seen[name] = true
		} else {
			delete(seen, name)
		}
	}
	names := make([]string, 0, len(seen))
	for path := range seen {
		if filepath.Dir(path) == dir {
			names = append(names, filepath.Base(path))
		}
	}
	sort.Strings(names)
	return names, nil
}

// Crash models power loss: every file's unsynced (dirty-page) bytes are lost,
// every not-yet-SyncDir'd creation is dropped (orphan tmp / un-durable segment),
// and the pending metadata delta is discarded. Only durably-synced bytes under
// durably-created names survive — exactly what a fresh openWith would find on
// reboot. suppressNextSync is cleared (a crash resets the harness knob).
func (fv *faultVFS) Crash() {
	fv.mu.Lock()
	defer fv.mu.Unlock()
	fv.crashLocked()
}

// crashLocked is the body of Crash; callable from Rename which already holds
// fv.mu (the SC4 crash-at-rename path).
func (fv *faultVFS) crashLocked() {
	fv.suppressNextSync = false
	// Drop the un-fsynced metadata delta.
	fv.pendingNames = make(map[string]bool)
	// For each file: drop unsynced bytes, and drop the file entirely if its
	// creation was never made durable (no SyncDir promoted it).
	for path, st := range fv.files {
		st.unsynced = nil
		if !st.durablyCreated || !fv.durableNames[path] {
			delete(fv.files, path)
		}
	}
}

// faultFile is one open handle onto a faultFileState. Multiple handles to the
// same path share the underlying state (OS page-cache semantics).
type faultFile struct {
	fv *faultVFS
	st *faultFileState
}

// Compile-time assertion that faultFile satisfies vfile.
var _ vfile = (*faultFile)(nil)

// Write appends p to the file's unsynced (dirty-page) buffer. The bytes are
// visible to this and other handles' ReaderAt immediately, but are NOT durable
// until Sync — a Crash before Sync loses them (SC3).
func (f *faultFile) Write(p []byte) (int, error) {
	f.fv.mu.Lock()
	defer f.fv.mu.Unlock()
	f.st.unsynced = append(f.st.unsynced, p...)
	return len(p), nil
}

// ReadAt reads over the logical view durable ++ unsynced — a process sees its
// own dirty pages before a crash.
func (f *faultFile) ReadAt(p []byte, off int64) (int, error) {
	f.fv.mu.Lock()
	defer f.fv.mu.Unlock()
	view := make([]byte, 0, len(f.st.durable)+len(f.st.unsynced))
	view = append(view, f.st.durable...)
	view = append(view, f.st.unsynced...)
	if off < 0 || off > int64(len(view)) {
		return 0, io.EOF
	}
	n := copy(p, view[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Sync promotes unsynced bytes to durable and clears unsynced, unless the
// harness armed suppressNextSync (modelling a kill after Write returned but
// before the fsync landed — the SC3 crash window). The knob is one-shot.
func (f *faultFile) Sync() error {
	f.fv.mu.Lock()
	defer f.fv.mu.Unlock()
	if f.fv.suppressNextSync {
		f.fv.suppressNextSync = false
		return nil // the fsync "did not land" before the crash
	}
	f.st.durable = append(f.st.durable, f.st.unsynced...)
	f.st.unsynced = nil
	return nil
}

// Truncate trims the logical durable++unsynced view to size, matching how the
// recovery path cuts a torn tail. It keeps the durable/unsynced split
// consistent: durable is trimmed first, then any remaining cut lands on
// unsynced.
func (f *faultFile) Truncate(size int64) error {
	f.fv.mu.Lock()
	defer f.fv.mu.Unlock()
	total := int64(len(f.st.durable) + len(f.st.unsynced))
	if size >= total {
		return nil
	}
	if size <= int64(len(f.st.durable)) {
		f.st.durable = f.st.durable[:size]
		f.st.unsynced = nil
		return nil
	}
	// size falls inside the unsynced region: keep all durable, trim unsynced.
	f.st.unsynced = f.st.unsynced[:size-int64(len(f.st.durable))]
	return nil
}

func (f *faultFile) Close() error {
	return nil
}

// Size reports the logical length durable + unsynced (what the writer sees).
func (f *faultFile) Size() (int64, error) {
	f.fv.mu.Lock()
	defer f.fv.mu.Unlock()
	return int64(len(f.st.durable) + len(f.st.unsynced)), nil
}
