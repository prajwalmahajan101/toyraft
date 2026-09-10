package file

import (
	"errors"
	"testing"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// TestSnapshotEncodeDecodeRoundtrip proves the on-disk codec round-trips every
// field, including an empty blob.
func TestSnapshotEncodeDecodeRoundtrip(t *testing.T) {
	t.Parallel()
	for _, snap := range []raft.Snapshot{
		{Index: 0, Term: 0, Data: nil},
		{Index: 42, Term: 7, Data: []byte("state-machine-blob")},
	} {
		got, err := decodeSnapshot(encodeSnapshot(snap))
		if err != nil {
			t.Fatalf("decodeSnapshot(%+v): %v", snap, err)
		}
		if got.Index != snap.Index || got.Term != snap.Term || string(got.Data) != string(snap.Data) {
			t.Errorf("roundtrip = %+v, want %+v", got, snap)
		}
	}
}

// TestSnapshotDecodeCorrupt proves a torn/short buffer and a CRC mismatch each
// surface an error wrapping errCorruptSnapshot rather than silently returning
// garbage.
func TestSnapshotDecodeCorrupt(t *testing.T) {
	t.Parallel()

	if _, err := decodeSnapshot([]byte{0x00, 0x01}); !errors.Is(err, errCorruptSnapshot) {
		t.Errorf("short buffer: err = %v, want errCorruptSnapshot", err)
	}

	b := encodeSnapshot(raft.Snapshot{Index: 5, Term: 2, Data: []byte("x")})
	b[snapCRCLen+1] ^= 0xFF // flip a body byte; CRC no longer matches
	if _, err := decodeSnapshot(b); !errors.Is(err, errCorruptSnapshot) {
		t.Errorf("crc mismatch: err = %v, want errCorruptSnapshot", err)
	}
}
