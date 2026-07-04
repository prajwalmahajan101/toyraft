package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// On-disk log format (LLD §3; STOR-03, STOR-06; ADR-0013).
//
// A segment file is a small versioned header followed by length+CRC-framed
// records, one record per raft.Entry:
//
//	[segMagic(4)][segVersion(1)]                       // segment header
//	{ [uint32 len][uint32 crc][payload(len bytes)] }*  // records
//	payload = Term(uint64 LE) ‖ Index(uint64 LE) ‖ Data(len-16 bytes)
//	crc     = CRC32-Castagnoli over payload ONLY (not over len)
//
// All integers are little-endian. len bounds the payload read so recovery
// never over-reads; a short read of the record header or payload, or a CRC
// mismatch, is the torn-tail boundary (not an error) — see scanSegment.

// castagnoli is the CRC32-Castagnoli table used for every record checksum.
// Castagnoli (iSCSI/LevelDB polynomial) has better error-detection than the
// IEEE default (08-RESEARCH "Don't Hand-Roll").
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

const (
	// recHeaderLen is the fixed record-header size: uint32 len + uint32 crc.
	recHeaderLen = 8
	// payloadFixedLen is the fixed prefix of every payload: Term + Index.
	payloadFixedLen = 16
	// segVersion is the current on-disk segment format version. Per-5: the
	// read path validates this and errors on an unknown version so future
	// format changes are detectable.
	segVersion byte = 1
	// segHeaderLen is the segment-header size: segMagic(4) + segVersion(1).
	segHeaderLen = 5
	// defaultMaxEntriesPerSegment is the rollover threshold: the active
	// segment rolls to a new one after this many entries. Exposed as a
	// per-Storage field (see maxEntriesPerSegment) so tests can shrink it
	// to force rollover cheaply for SC5.
	defaultMaxEntriesPerSegment = 1024
)

// segMagic is the stable 4-byte magic prefixing every segment ("TRL1" =
// ToyRaft Log). A file whose magic differs is not one of our segments and
// makes scanSegment return a structural error (Per-5).
var segMagic = [4]byte{'T', 'R', 'L', '1'}

var (
	// errShortRecord signals a truncated record header/payload during a scan.
	// It is an INTERNAL sentinel used to detect the torn tail; scanSegment
	// translates it into a clean stop (goodEnd), never surfacing it.
	errShortRecord = errors.New("file storage: short record (torn tail)")
	// errBadCRC signals a record whose payload CRC does not match. Like
	// errShortRecord it marks the torn tail and is not surfaced by scanSegment.
	errBadCRC = errors.New("file storage: record CRC mismatch")
	// errShortPayload signals a record header claiming a payload smaller than
	// the fixed Term+Index prefix — a structurally impossible record.
	errShortPayload = errors.New("file storage: record payload shorter than fixed header")
)

// encodeRecord writes one framed record for e to w: an 8-byte header
// (len ‖ crc) followed by the payload (Term ‖ Index ‖ Data). The CRC is
// computed over the payload only.
func encodeRecord(w io.Writer, e raft.Entry) error {
	payload := make([]byte, payloadFixedLen+len(e.Data))
	binary.LittleEndian.PutUint64(payload[0:8], uint64(e.Term))
	binary.LittleEndian.PutUint64(payload[8:16], uint64(e.Index))
	copy(payload[payloadFixedLen:], e.Data)

	var hdr [recHeaderLen]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.Checksum(payload, castagnoli))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	return nil
}

// decodeRecord validates a record header + payload and returns the Entry.
// hdr must be the 8-byte record header; payload must be exactly len(hdr)
// bytes long. The returned Entry.Data is a fresh copy (never aliasing the
// caller's payload buffer) so callers may mutate it freely.
func decodeRecord(hdr, payload []byte) (raft.Entry, error) {
	if len(hdr) < recHeaderLen {
		return raft.Entry{}, errShortRecord
	}
	declaredLen := binary.LittleEndian.Uint32(hdr[0:4])
	wantCRC := binary.LittleEndian.Uint32(hdr[4:8])
	if uint32(len(payload)) != declaredLen {
		return raft.Entry{}, errShortRecord
	}
	if len(payload) < payloadFixedLen {
		return raft.Entry{}, errShortPayload
	}
	if crc32.Checksum(payload, castagnoli) != wantCRC {
		return raft.Entry{}, errBadCRC
	}
	term := binary.LittleEndian.Uint64(payload[0:8])
	index := binary.LittleEndian.Uint64(payload[8:16])
	data := make([]byte, len(payload)-payloadFixedLen)
	copy(data, payload[payloadFixedLen:])
	return raft.Entry{Term: raft.Term(term), Index: raft.Index(index), Data: data}, nil
}

// writeSegHeader writes the segment header (magic ‖ current version) to w.
// It is the first thing written to a freshly created segment.
func writeSegHeader(w io.Writer) error {
	var hdr [segHeaderLen]byte
	copy(hdr[0:4], segMagic[:])
	hdr[4] = segVersion
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	return nil
}

// readSegHeader reads and validates a segment header from hdr. A wrong magic
// or an unknown version is a structural error (Per-5): unreadable/foreign
// segments must not be silently treated as empty.
func readSegHeader(hdr []byte) error {
	if len(hdr) < segHeaderLen {
		return fmt.Errorf("file storage: truncated segment header: have %d bytes, want %d", len(hdr), segHeaderLen)
	}
	if [4]byte(hdr[0:4]) != segMagic {
		return fmt.Errorf("file storage: bad segment magic %q", hdr[0:4])
	}
	if v := hdr[4]; v != segVersion {
		return fmt.Errorf("file storage: unknown segment version %d (this build understands %d)", v, segVersion)
	}
	return nil
}

// scanRec is one record located by a segment scan: enough to serve a later
// Entries() read (via ReaderAt at Offset) and to rebuild the in-RAM index.
type scanRec struct {
	Term    raft.Term
	Index   raft.Index
	DataLen int   // len of the entry Data (payload len minus the fixed 16-byte prefix)
	Offset  int64 // byte offset of this record's PAYLOAD start within the segment
}

// scanSegment reads f from the start, validates the segment header, then
// walks every record. It returns the records up to (and including) the last
// CRC-valid one and goodEnd — the byte offset immediately after that last
// good record, which is where a recovery Truncate cuts the torn tail.
//
// A short read of a record header/payload or a CRC mismatch is the torn-tail
// boundary and stops the scan WITHOUT an error (STOR-06/SC2). A structurally
// broken segment header (bad magic / unknown version) IS returned as an
// error. When the segment holds no records, goodEnd == segHeaderLen.
func scanSegment(f vfile) (recs []scanRec, goodEnd int64, err error) {
	size, err := f.Size()
	if err != nil {
		return nil, 0, err
	}

	shdr := make([]byte, segHeaderLen)
	if _, err := f.ReadAt(shdr, 0); err != nil {
		// A segment must at least carry a full header; a short read here is
		// structural corruption, not a torn record tail.
		return nil, 0, fmt.Errorf("file storage: read segment header: %w", err)
	}
	if err := readSegHeader(shdr); err != nil {
		return nil, 0, err
	}

	off := int64(segHeaderLen)
	goodEnd = off
	for off < size {
		// Read the 8-byte record header. A short read is a torn tail.
		var hdr [recHeaderLen]byte
		if _, rerr := f.ReadAt(hdr[:], off); rerr != nil {
			break // torn tail: header incomplete
		}
		declaredLen := binary.LittleEndian.Uint32(hdr[0:4])
		payloadOff := off + recHeaderLen
		// A record claiming more bytes than remain in the file is a torn tail.
		if payloadOff+int64(declaredLen) > size {
			break
		}
		payload := make([]byte, declaredLen)
		if _, rerr := f.ReadAt(payload, payloadOff); rerr != nil {
			break // torn tail: payload incomplete
		}
		e, derr := decodeRecord(hdr[:], payload)
		if derr != nil {
			// errShortPayload / errBadCRC / errShortRecord all mark the torn
			// tail: stop at the last good record, do not surface the error.
			break
		}
		recs = append(recs, scanRec{
			Term:    e.Term,
			Index:   e.Index,
			DataLen: len(e.Data),
			Offset:  payloadOff,
		})
		off = payloadOff + int64(declaredLen)
		goodEnd = off
	}
	return recs, goodEnd, nil
}

// segmentName returns the segment file name holding entries starting at
// firstIndex: a zero-padded first index with a ".seg" suffix, so segment
// names sort lexically in index order (e.g. "0000000000000001.seg").
func segmentName(firstIndex raft.Index) string {
	return fmt.Sprintf("%016d.seg", uint64(firstIndex))
}
