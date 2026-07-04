package http

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// goldenCase pairs a golden JSON frame (byte-for-byte from docs/WIRE.md §2.1-2.4)
// with the raft.Message it MUST decode to and re-marshal from.
type goldenCase struct {
	file string
	want raft.Message
}

func goldenCases() []goldenCase {
	return []goldenCase{
		{
			// WIRE §2.1 RequestVote
			file: "rv.json",
			want: raft.Message{
				Type:         raft.MsgRequestVote,
				Term:         7,
				From:         "node-1",
				To:           "node-2",
				LastLogIndex: 42,
				LastLogTerm:  6,
			},
		},
		{
			// WIRE §2.2 RequestVoteResponse
			file: "rvresp.json",
			want: raft.Message{
				Type:        raft.MsgRequestVoteResponse,
				Term:        7,
				From:        "node-2",
				To:          "node-1",
				VoteGranted: true,
			},
		},
		{
			// WIRE §2.3 AppendEntries (two entries)
			file: "ae.json",
			want: raft.Message{
				Type:         raft.MsgAppendEntries,
				Term:         7,
				From:         "node-1",
				To:           "node-2",
				PrevLogIndex: 41,
				PrevLogTerm:  6,
				Entries: []raft.Entry{
					{Term: 7, Index: 42, Data: []byte("PUT foo bar")},
					{Term: 7, Index: 43, Data: []byte("DEL foo")},
				},
				LeaderCommit: 40,
			},
		},
		{
			// WIRE §2.4 AppendEntriesResponse (success)
			file: "aeresp.json",
			want: raft.Message{
				Type:       raft.MsgAppendEntriesResp,
				Term:       7,
				From:       "node-2",
				To:         "node-1",
				Success:    true,
				MatchIndex: 43,
			},
		},
	}
}

func readGolden(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "golden", name))
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	return b
}

// canonical compacts arbitrary JSON to its whitespace-free canonical form so a
// value comparison is a byte-for-byte VALUE match independent of the pretty
// formatting in the WIRE.md examples.
func canonical(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := json.Compact(&buf, b); err != nil {
		t.Fatalf("canonicalize: %v", err)
	}
	return buf.Bytes()
}

// TestWireConformance is the SC7 golden round-trip: each of the four WIRE.md
// §2 frames decodes to the expected raft.Message, and the SAME Message
// re-marshals to a byte-for-byte (canonicalized) match of the golden frame.
func TestWireConformance(t *testing.T) {
	for _, tc := range goldenCases() {
		tc := tc
		t.Run(tc.file, func(t *testing.T) {
			raw := readGolden(t, tc.file)

			// Direction 1: golden bytes -> decodeMessage -> expected Message.
			got, err := decodeMessage(raw)
			if err != nil {
				t.Fatalf("decodeMessage(%s): %v", tc.file, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("decoded Message mismatch for %s\n got: %#v\nwant: %#v", tc.file, got, tc.want)
			}

			// Direction 2: expected Message -> fromMessage -> Marshal ->
			// canonical bytes == canonical golden bytes (byte-for-byte value match).
			out, err := json.Marshal(fromMessage(tc.want))
			if err != nil {
				t.Fatalf("marshal %s: %v", tc.file, err)
			}
			gotC := canonical(t, out)
			wantC := canonical(t, raw)
			if !bytes.Equal(gotC, wantC) {
				t.Fatalf("re-marshal mismatch for %s\n got: %s\nwant: %s", tc.file, gotC, wantC)
			}
		})
	}
}

// TestWireConformanceBase64Fidelity asserts the AppendEntries golden's base64
// entry data decodes to the exact plaintext bytes (standard base64, WIRE §2).
func TestWireConformanceBase64Fidelity(t *testing.T) {
	m, err := decodeMessage(readGolden(t, "ae.json"))
	if err != nil {
		t.Fatalf("decode ae.json: %v", err)
	}
	if len(m.Entries) != 2 {
		t.Fatalf("want 2 entries, got %d", len(m.Entries))
	}
	if got := string(m.Entries[0].Data); got != "PUT foo bar" {
		t.Errorf("entries[0].Data = %q, want %q (base64 UFVUIGZvbyBiYXI=)", got, "PUT foo bar")
	}
	if got := string(m.Entries[1].Data); got != "DEL foo" {
		t.Errorf("entries[1].Data = %q, want %q (base64 REVMIGZvbw==)", got, "DEL foo")
	}

	// And the reverse: marshalling those bytes yields the golden base64 strings.
	out, err := json.Marshal(fromMessage(m))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, b64 := range []string{"UFVUIGZvbyBiYXI=", "REVMIGZvbw=="} {
		if !bytes.Contains(out, []byte(b64)) {
			t.Errorf("re-marshalled AE frame missing base64 %q\ngot: %s", b64, out)
		}
	}
}
