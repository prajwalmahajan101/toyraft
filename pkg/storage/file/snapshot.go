package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// Snapshot persistence (ADR-0024). A durable StateMachine checkpoint is a
// single small file written with the same atomic-rename recipe as HardState
// (hardstate.go / ADR-0013): write a fresh temp file, fsync it, rename(2) it
// over the final path, then fsync the parent directory so the rename is
// durable. A crash at any point leaves LoadSnapshot reading either the complete
// OLD snapshot or the complete NEW one — never a torn write.
//
// On-disk layout (little-endian, CRC32-Castagnoli prefixed):
//
//	[uint32 crc][uint64 Index][uint64 Term][uint32 dataLen][Data bytes]
//	crc = CRC32-Castagnoli over EVERYTHING after the crc field.
//
// This is the WORKING durable-snapshot path; the frozen Snapshot()/Restore()
// methods (file.go) stay ErrSnapshotUnsupported for STOR-01 forward-compat.

const (
	// snapshotFile is the final durable snapshot path within the storage dir.
	snapshotFile = "snapshot"
	// snapshotTmpFile is the fixed-name scratch file the new snapshot is
	// written+fsynced into before the atomic rename. A single writer under
	// s.mu owns it; an orphan left by a crash before Rename is ignored on the
	// read path and overwritten (O_TRUNC) on the next Save.
	snapshotTmpFile = "snapshot.tmp"
	// snapCRCLen is the leading CRC field width; the CRC covers all bytes after it.
	snapCRCLen = 4
	// snapFixedLen is the fixed portion after the CRC: Index(u64) + Term(u64) +
	// dataLen(u32), before the variable Data bytes.
	snapFixedLen = 8 + 8 + 4
)

// errCorruptSnapshot wraps a snapshot file that is present but structurally
// broken (short buffer or CRC mismatch). Surfaced by decodeSnapshot with %w.
var errCorruptSnapshot = errors.New("file storage: corrupt snapshot")

// encodeSnapshot serialises snap to the CRC-prefixed on-disk layout. The CRC is
// computed over every byte after the 4-byte CRC field.
func encodeSnapshot(snap raft.Snapshot) []byte {
	buf := make([]byte, snapCRCLen+snapFixedLen+len(snap.Data))
	body := buf[snapCRCLen:]
	binary.LittleEndian.PutUint64(body[0:8], uint64(snap.Index))
	binary.LittleEndian.PutUint64(body[8:16], uint64(snap.Term))
	binary.LittleEndian.PutUint32(body[16:20], uint32(len(snap.Data)))
	copy(body[20:], snap.Data)
	binary.LittleEndian.PutUint32(buf[0:snapCRCLen], crc32.Checksum(body, castagnoli))
	return buf
}

// decodeSnapshot validates the CRC and reconstructs the Snapshot. A buffer too
// short to hold the header, a declared dataLen that overruns the buffer, or a
// CRC mismatch each yield an error wrapping errCorruptSnapshot with %w.
func decodeSnapshot(b []byte) (raft.Snapshot, error) {
	if len(b) < snapCRCLen+snapFixedLen {
		return raft.Snapshot{}, fmt.Errorf("snapshot too short: have %d bytes, want >= %d: %w", len(b), snapCRCLen+snapFixedLen, errCorruptSnapshot)
	}
	body := b[snapCRCLen:]
	wantCRC := binary.LittleEndian.Uint32(b[0:snapCRCLen])
	if crc32.Checksum(body, castagnoli) != wantCRC {
		return raft.Snapshot{}, fmt.Errorf("snapshot crc mismatch: %w", errCorruptSnapshot)
	}
	index := binary.LittleEndian.Uint64(body[0:8])
	term := binary.LittleEndian.Uint64(body[8:16])
	dataLen := binary.LittleEndian.Uint32(body[16:20])
	if int(dataLen) != len(body)-snapFixedLen {
		return raft.Snapshot{}, fmt.Errorf("snapshot dataLen %d overruns buffer (%d trailing bytes): %w", dataLen, len(body)-snapFixedLen, errCorruptSnapshot)
	}
	data := make([]byte, dataLen)
	copy(data, body[20:])
	return raft.Snapshot{Index: raft.Index(index), Term: raft.Term(term), Data: data}, nil
}

// SaveSnapshot durably and atomically persists snap (ADR-0024). It writes the
// encoded snapshot into <dir>/snapshot.tmp, fsyncs that file, renames it over
// <dir>/snapshot (atomic on POSIX), then fsyncs the parent directory so the
// rename's directory entry is itself durable BEFORE this call returns. Every fs
// error is wrapped with %w and never ignored; all access goes through the vfs
// seam so the fault tests can model a crash between Write and Rename.
func (s *Storage) SaveSnapshot(snap raft.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tmp := filepath.Join(s.dir, snapshotTmpFile)
	final := filepath.Join(s.dir, snapshotFile)

	f, err := s.fs.Create(tmp)
	if err != nil {
		return fmt.Errorf("file storage: create snapshot tmp: %w", err)
	}
	if _, err := f.Write(encodeSnapshot(snap)); err != nil {
		return fmt.Errorf("file storage: write snapshot tmp: %w", errors.Join(err, f.Close()))
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("file storage: sync snapshot tmp: %w", errors.Join(err, f.Close()))
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("file storage: close snapshot tmp: %w", err)
	}

	if err := s.fs.Rename(tmp, final); err != nil {
		return fmt.Errorf("file storage: rename snapshot into place: %w", err)
	}
	if err := s.fs.SyncDir(s.dir); err != nil {
		return fmt.Errorf("file storage: syncdir after snapshot rename: %w", err)
	}
	return nil
}

// LoadSnapshot reads the durable snapshot. A MISSING file returns
// (raft.Snapshot{}, nil) — a fresh store has never saved one. A
// present-but-corrupt file returns an error wrapping the parse failure with %w.
// Any orphaned snapshot.tmp (a crash before Rename) is ignored: Load reads only
// the final path.
func (s *Storage) LoadSnapshot() (raft.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	final := filepath.Join(s.dir, snapshotFile)
	f, err := s.fs.Open(final)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return raft.Snapshot{}, nil // fresh store: never saved
		}
		return raft.Snapshot{}, fmt.Errorf("file storage: open snapshot: %w", err)
	}
	defer func() { _ = f.Close() }()

	b, err := readAllAt(f)
	if err != nil {
		return raft.Snapshot{}, fmt.Errorf("file storage: read snapshot: %w", err)
	}
	snap, err := decodeSnapshot(b)
	if err != nil {
		return raft.Snapshot{}, fmt.Errorf("file storage: decode snapshot: %w", err)
	}
	return snap, nil
}
