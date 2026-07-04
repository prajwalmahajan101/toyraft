package file

import (
	"io"
	"os"
	"sort"
)

// vfs is the unexported durability surface the file storage impl uses for
// every filesystem operation (LLD §3; 08-RESEARCH §3 "Recommended seam").
// Isolating it behind an interface lets an in-process fault fake model
// "lose all writes since the last fsync" for the SC3/SC4/T-6 crash-injection
// tests, while production uses osVFS backed by the stdlib os package.
type vfs interface {
	Open(name string) (vfile, error)       // read/recover an existing file
	Create(name string) (vfile, error)     // new segment / tmp hardstate (O_TRUNC)
	OpenAppend(name string) (vfile, error) // reopen the active segment for append
	Rename(oldpath, newpath string) error  // atomic HardState swap (rename(2))
	SyncDir(dir string) error              // parent-dir fsync (Per-3)
	Remove(name string) error              // drop truncated / orphaned files
	ReadDir(dir string) ([]string, error)  // discover segments on recover
}

// vfile is a single open file within a vfs. Writes are not durable until
// Sync returns. Reads are positional via ReaderAt so recovery and Entries
// can seek without disturbing an append cursor.
type vfile interface {
	io.Writer
	io.ReaderAt
	Sync() error
	Truncate(size int64) error // torn-tail truncation
	Close() error
	Size() (int64, error)
}

// osVFS is the real, os-backed vfs used in production and by the
// torn-tail recovery test (which needs a genuine on-disk file). It holds
// no state; the zero value osVFS{} is ready to use.
type osVFS struct{}

// Compile-time assertion that osVFS satisfies vfs.
var _ vfs = osVFS{}

func (osVFS) Open(name string) (vfile, error) {
	f, err := os.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	return &osFile{f: f}, nil
}

func (osVFS) Create(name string) (vfile, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &osFile{f: f}, nil
}

func (osVFS) OpenAppend(name string) (vfile, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &osFile{f: f}, nil
}

func (osVFS) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

// SyncDir fsyncs the directory itself so a create or rename inside it is
// durable (Per-3). On Linux ext4/xfs this persists the directory entry;
// the read-side handle is Close()d best-effort (a read handle Close error
// is not a durability failure). See 08-RESEARCH "Directory fsync".
func (osVFS) SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

func (osVFS) Remove(name string) error {
	return os.Remove(name)
}

// ReadDir returns the sorted base names (NOT full paths) of the directory
// entries, so segment discovery is deterministic and lexically ordered.
func (osVFS) ReadDir(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}

// osFile adapts *os.File to vfile.
type osFile struct {
	f *os.File
}

// Compile-time assertion that osFile satisfies vfile.
var _ vfile = (*osFile)(nil)

func (o *osFile) Write(p []byte) (int, error)             { return o.f.Write(p) }
func (o *osFile) ReadAt(p []byte, off int64) (int, error) { return o.f.ReadAt(p, off) }
func (o *osFile) Sync() error                             { return o.f.Sync() }
func (o *osFile) Truncate(size int64) error               { return o.f.Truncate(size) }
func (o *osFile) Close() error                            { return o.f.Close() }

// Size reports the current on-disk size via Stat.
func (o *osFile) Size() (int64, error) {
	st, err := o.f.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}
