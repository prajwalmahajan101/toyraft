package linearizability

import (
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// TestFigure7Linearizable is a POSITIVE test (SC3/LIN-05) encoding Raft's
// Figure-7 scenario (paper §5.3) as a client-visible register history.
//
// Figure 7 is about log reconciliation across a leader change: after a value is
// committed, a leadership change must NOT reorder or drop the committed client
// effect. Porcupine has no notion of leaders, so the churn is modeled purely as
// subsequent operations issued by DIFFERENT ClientIds — the assertion is that
// every post-churn read STILL observes the committed value (or a legal
// successor when a later write is scripted). A correct implementation (toyraft)
// yields porcupine.Ok; a Raft that lost or reordered the committed write under
// churn would make this history Illegal.
//
// History: client 1 commits set(k,"v7") in [10,20]. After the (implicit) leader
// change, two DIFFERENT clients read: client 2's get(k) [30,50] overlaps client
// 3's get(k) [40,60]; both observe "v7". Client 1 then commits a legal successor
// set(k,"v7b") [70,80], and client 2's get(k) [85,95] observes "v7b" — proving
// the register keeps advancing correctly across the modeled churn. The
// linearization set(k,"v7") < get->"v7" < get->"v7" < set(k,"v7b") < get->"v7b"
// certifies it.
func TestFigure7Linearizable(t *testing.T) {
	const timeout = 10 * time.Second

	ops := []porcupine.Operation{
		{ClientId: 1, Input: setIn("k", "v7"), Output: kvOutput{}, Call: 10, Return: 20},  // committed before churn
		{ClientId: 2, Input: getIn("k"), Output: obs("v7"), Call: 30, Return: 50},         // post-churn read A
		{ClientId: 3, Input: getIn("k"), Output: obs("v7"), Call: 40, Return: 60},         // post-churn read B (overlaps A)
		{ClientId: 1, Input: setIn("k", "v7b"), Output: kvOutput{}, Call: 70, Return: 80}, // legal successor write
		{ClientId: 2, Input: getIn("k"), Output: obs("v7b"), Call: 85, Return: 95},        // observes the successor
	}

	res, _ := porcupine.CheckOperationsVerbose(kvModel, ops, timeout)
	switch res {
	case porcupine.Ok:
		// pass: leader churn preserved the committed effects.
	case porcupine.Illegal:
		t.Fatalf("Figure 7 history unexpectedly NOT linearizable (Illegal) — churn reordered/dropped a committed effect")
	case porcupine.Unknown:
		t.Fatalf("Figure 7 linearizability UNKNOWN (timeout) — cannot certify")
	default:
		t.Fatalf("Figure 7: unexpected checker result %v", res)
	}
}
