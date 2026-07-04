package http

import (
	"os"
	"path/filepath"
	"testing"
)

// seedCorpus returns the FuzzMessageParse seed inputs: the four golden frames
// plus the RESEARCH §SC5 edge cases. Every entry MUST leave decodeMessage
// panic-free; the split into valid/invalid is asserted by TestDecodeSeedCorpus.
func seedCorpus(t testing.TB) (valid, invalid [][]byte) {
	t.Helper()
	read := func(name string) []byte {
		b, err := os.ReadFile(filepath.Join("testdata", "golden", name))
		if err != nil {
			t.Fatalf("read golden %s: %v", name, err)
		}
		return b
	}

	valid = [][]byte{
		read("rv.json"),
		read("rvresp.json"),
		read("ae.json"),
		read("aeresp.json"),
		// Unknown extra field MUST be ignored, not rejected (WIRE §6.1).
		[]byte(`{"type":0,"term":7,"from":"a","to":"b","snapshot_meta":{"x":1},"learner":true}`),
	}

	invalid = [][]byte{
		[]byte(``),             // empty body
		[]byte(`{`),            // truncated JSON
		[]byte(`{"type":255}`), // MsgTick — internal-only (WIRE §2.5)
		[]byte(`{"type":99}`),  // unknown MessageType (WIRE §6.2)
		[]byte(`{"type":2,"term":99999999999999999999}`),           // huge number overflows uint64
		[]byte(`{"type":2,"entries":[{"data":"not base64 !!!"}]}`), // non-base64 data
	}
	return valid, invalid
}

// TestDecodeSeedCorpus is the normal unit sub-check (WIRE §6.1 + §2.5/§6.2):
// valid seeds decode without error (incl. the unknown-extra-field frame),
// invalid seeds return a non-nil error — and NONE panic.
func TestDecodeSeedCorpus(t *testing.T) {
	valid, invalid := seedCorpus(t)

	for i, b := range valid {
		if _, err := decodeMessage(b); err != nil {
			t.Errorf("valid seed %d decoded with error: %v\ninput: %s", i, err, b)
		}
	}
	// The empty-object frame {} decodes to type=0 (RequestVote) with zero
	// fields — valid per WIRE (missing fields are zero values).
	if _, err := decodeMessage([]byte(`{}`)); err != nil {
		t.Errorf("empty-object {} should decode (type defaults to 0): %v", err)
	}
	for i, b := range invalid {
		if _, err := decodeMessage(b); err == nil {
			t.Errorf("invalid seed %d decoded WITHOUT error (want rejection)\ninput: %s", i, b)
		}
	}
}

// FuzzMessageParse feeds arbitrary bytes through the PURE decoder (SC5). The
// core invariant is: decodeMessage NEVER panics, regardless of input. It also
// asserts the decoder returns cleanly (either a Message or an error, never
// both a non-zero Message alongside an error). No network, no HTTP handler.
func FuzzMessageParse(f *testing.F) {
	valid, invalid := seedCorpus(f)
	for _, b := range valid {
		f.Add(b)
	}
	for _, b := range invalid {
		f.Add(b)
	}
	f.Add([]byte(`{}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		// The load-bearing invariant: no panic on arbitrary bytes.
		msg, err := decodeMessage(data)
		if err != nil {
			// On error the Message MUST be the zero value (decoder discipline).
			if msg.Type != 0 || msg.Term != 0 || msg.From != "" || msg.To != "" {
				t.Fatalf("decodeMessage returned err AND a non-zero Message: %#v (err: %v)", msg, err)
			}
		}
	})
}
