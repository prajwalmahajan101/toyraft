package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// HardState persistence (LLD §3/§4; STOR-05; ADR-0013).
//
// The durable Raft HardState (CurrentTerm, VotedFor, Commit) is a single small
// file written with the classic atomic-rename recipe: write a fresh temp file,
// fsync it, rename(2) it over the final path, then fsync the parent directory
// so the rename itself is durable. rename(2) is atomic on POSIX filesystems, so
// a crash at ANY point leaves LoadHardState reading either the complete OLD
// state or the complete NEW state — never a torn half-write (08-05 proves this
// via faultVFS.Crash between Write and Rename).
//
// On-disk layout (little-endian, CRC32-Castagnoli prefixed):
//
//	[uint32 crc][uint64 CurrentTerm][uint64 Commit][uint32 votedForLen][VotedFor bytes]
//	crc = CRC32-Castagnoli over EVERYTHING after the crc field.
//
// CRITICAL (08-RESEARCH §1 / conformance HardStateRoundtrip): Commit IS
// persisted here. The "commitIndex is in-memory only" rule is a node-level
// policy, not a storage carve-out — HardState.Commit round-trips faithfully
// (the conformance suite sets Commit:5 and asserts struct equality).

const (
	// hardStateFile is the final durable HardState path within the storage dir.
	hardStateFile = "hardstate"
	// hardStateTmpFile is the fixed-name scratch file the new HardState is
	// written+fsynced into before the atomic rename onto hardStateFile. Fixed
	// (no time.Now(); check-no-time-now gate) — a single writer under s.mu owns
	// it, and any orphan left by a crash before Rename is ignored on the read
	// path and overwritten (O_TRUNC) on the next Save.
	hardStateTmpFile = "hardstate.tmp"
	// hsCRCLen is the leading CRC field width; the CRC covers all bytes after it.
	hsCRCLen = 4
	// hsFixedLen is the fixed portion after the CRC: CurrentTerm(u64) +
	// Commit(u64) + votedForLen(u32), before the variable VotedFor bytes.
	hsFixedLen = 8 + 8 + 4
)

// errCorruptHardState wraps a HardState file that is present but structurally
// broken (short buffer or CRC mismatch). Surfaced by decodeHardState with %w so
// callers can errors.Is it; distinct from a missing file (which is not an error).
var errCorruptHardState = errors.New("file storage: corrupt hardstate")

// encodeHardState serialises hs to the CRC-prefixed on-disk layout. VotedFor
// (a raft.NodeID string) is length-prefixed so an empty vote round-trips to the
// empty string. The CRC is computed over every byte after the 4-byte CRC field.
func encodeHardState(hs raft.HardState) []byte {
	votedFor := []byte(hs.VotedFor)
	buf := make([]byte, hsCRCLen+hsFixedLen+len(votedFor))

	body := buf[hsCRCLen:]
	binary.LittleEndian.PutUint64(body[0:8], uint64(hs.CurrentTerm))
	binary.LittleEndian.PutUint64(body[8:16], uint64(hs.Commit))
	binary.LittleEndian.PutUint32(body[16:20], uint32(len(votedFor)))
	copy(body[20:], votedFor)

	binary.LittleEndian.PutUint32(buf[0:hsCRCLen], crc32.Checksum(body, castagnoli))
	return buf
}

// decodeHardState validates the CRC and reconstructs all three HardState fields
// (including Commit). A buffer too short to hold the header, a declared
// VotedFor length that overruns the buffer, or a CRC mismatch each yield an
// error wrapping errCorruptHardState with %w.
func decodeHardState(b []byte) (raft.HardState, error) {
	if len(b) < hsCRCLen+hsFixedLen {
		return raft.HardState{}, fmt.Errorf("hardstate too short: have %d bytes, want >= %d: %w", len(b), hsCRCLen+hsFixedLen, errCorruptHardState)
	}
	body := b[hsCRCLen:]
	wantCRC := binary.LittleEndian.Uint32(b[0:hsCRCLen])
	if crc32.Checksum(body, castagnoli) != wantCRC {
		return raft.HardState{}, fmt.Errorf("hardstate crc mismatch: %w", errCorruptHardState)
	}
	currentTerm := binary.LittleEndian.Uint64(body[0:8])
	commit := binary.LittleEndian.Uint64(body[8:16])
	votedForLen := binary.LittleEndian.Uint32(body[16:20])
	if int(votedForLen) != len(body)-hsFixedLen {
		return raft.HardState{}, fmt.Errorf("hardstate votedFor length %d overruns buffer (%d trailing bytes): %w", votedForLen, len(body)-hsFixedLen, errCorruptHardState)
	}
	return raft.HardState{
		CurrentTerm: raft.Term(currentTerm),
		VotedFor:    raft.NodeID(body[20:]),
		Commit:      raft.Index(commit),
	}, nil
}

// SaveHardState durably and atomically persists hs (STOR-05/SC4; LLD §4). It
// writes the encoded state into <dir>/hardstate.tmp, fsyncs that file, renames
// it over <dir>/hardstate (atomic on POSIX), then fsyncs the parent directory
// so the rename's directory entry is itself durable BEFORE this call returns.
// A node MUST NOT emit an RPC depending on (Term, VotedFor) until this returns
// (REPL-09) — hence the fsyncs are not optional.
//
// Every fs error is wrapped with %w and never ignored (errcheck is blocking on
// the write path). All filesystem access goes through the vfs seam so 08-05 can
// inject faultVFS and model a crash between the Write and the Rename.
func (s *Storage) SaveHardState(hs raft.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tmp := filepath.Join(s.dir, hardStateTmpFile)
	final := filepath.Join(s.dir, hardStateFile)

	// Write + fsync the temp file so its bytes are durable BEFORE the rename.
	f, err := s.fs.Create(tmp)
	if err != nil {
		return fmt.Errorf("file storage: create hardstate tmp: %w", err)
	}
	if _, err := f.Write(encodeHardState(hs)); err != nil {
		return fmt.Errorf("file storage: write hardstate tmp: %w", errors.Join(err, f.Close()))
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("file storage: sync hardstate tmp: %w", errors.Join(err, f.Close()))
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("file storage: close hardstate tmp: %w", err)
	}

	// Atomic swap, then make the rename durable (Per-3/SC4).
	if err := s.fs.Rename(tmp, final); err != nil {
		return fmt.Errorf("file storage: rename hardstate into place: %w", err)
	}
	if err := s.fs.SyncDir(s.dir); err != nil {
		return fmt.Errorf("file storage: syncdir after hardstate rename: %w", err)
	}
	return nil
}

// LoadHardState reads the durable HardState. A MISSING file returns
// (raft.HardState{}, nil) — a fresh store has never saved and its zero state is
// the correct answer (conformance HardStateFreshIsZero). A present-but-corrupt
// file (short buffer or CRC mismatch) returns an error wrapping the parse
// failure with %w. Any orphaned hardstate.tmp (a crash before Rename) is
// ignored: Load reads only the final path, so a torn temp never corrupts the
// load (SC4).
func (s *Storage) LoadHardState() (raft.HardState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	final := filepath.Join(s.dir, hardStateFile)
	f, err := s.fs.Open(final)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return raft.HardState{}, nil // fresh store: never saved
		}
		return raft.HardState{}, fmt.Errorf("file storage: open hardstate: %w", err)
	}
	defer func() { _ = f.Close() }() // read-only handle: Close error is not a durability failure

	b, err := readAllAt(f)
	if err != nil {
		return raft.HardState{}, fmt.Errorf("file storage: read hardstate: %w", err)
	}
	hs, err := decodeHardState(b)
	if err != nil {
		return raft.HardState{}, fmt.Errorf("file storage: decode hardstate: %w", err)
	}
	return hs, nil
}

// readAllAt reads the entire contents of a positional (ReaderAt) vfile. The
// HardState file is tiny, so a single Size + ReadAt is sufficient; io.EOF from
// the exact-length read is expected and not surfaced.
func readAllAt(f vfile) ([]byte, error) {
	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	b := make([]byte, size)
	if _, err := f.ReadAt(b, 0); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return b, nil
}
