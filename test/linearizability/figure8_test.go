package linearizability

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// TestFigure8Linearizable is a POSITIVE test (SC3/LIN-05) encoding Raft's
// Figure-8 commitment-safety scenario (paper §5.4.2).
//
// Figure 8 is the classic pitfall: WITHOUT the current-term-commit rule, an
// entry that appears committed by replica count can still be overwritten after
// a leader change — a lost committed write. toyraft IMPLEMENTS that rule
// (docs/adr/0010-current-term-commit-rule.md: a leader only commits an entry by
// replica count once it has replicated an entry from its OWN current term), so
// the committed write STAYS visible across the modeled leader change. The
// correct history therefore returns porcupine.Ok — the entry is NOT lost.
//
// History: client 1 commits set(k,"c8") in [10,20]. Across the modeled leader
// change (later ops from other clients), every subsequent get(k) still returns
// "c8": client 2's get(k) [30,50] overlaps client 3's get(k) [40,60], both
// observing "c8". The linearization set(k,"c8") < get->"c8" < get->"c8"
// certifies that toyraft's current-term rule kept V visible.
func TestFigure8Linearizable(t *testing.T) {
	const timeout = 10 * time.Second

	ops := []porcupine.Operation{
		{ClientId: 1, Input: setIn("k", "c8"), Output: kvOutput{}, Call: 10, Return: 20}, // committed under current-term rule
		{ClientId: 2, Input: getIn("k"), Output: obs("c8"), Call: 30, Return: 50},        // post-churn read A — still c8
		{ClientId: 3, Input: getIn("k"), Output: obs("c8"), Call: 40, Return: 60},        // post-churn read B (overlaps A) — still c8
	}

	res, _ := porcupine.CheckOperationsVerbose(kvModel, ops, timeout)
	switch res {
	case porcupine.Ok:
		// pass: the committed write survived the modeled leader change.
	case porcupine.Illegal:
		t.Fatalf("Figure 8 history unexpectedly NOT linearizable (Illegal) — committed write was lost")
	case porcupine.Unknown:
		t.Fatalf("Figure 8 linearizability UNKNOWN (timeout) — cannot certify")
	default:
		t.Fatalf("Figure 8: unexpected checker result %v", res)
	}
}

// TestFigure8ViolationProducesArtifact proves the violation path end-to-end
// (SC4/LIN-03) WITHOUT introducing any real bug into pkg/raft.
//
// It constructs a DELIBERATELY non-linearizable Figure-8 history: set(k,"c8") is
// acknowledged (returns by t=20 — a committed write), then a later, strictly
// subsequent get(k) on [30,40] returns the PRE-write absent value. No
// linearization allows a committed write to vanish for a later read, so the
// checker MUST return porcupine.Illegal. The test then feeds the resulting
// LinearizationInfo to porcupine.VisualizePath, which writes the HTML
// visualization under test/artifacts/linearizability/<seed>.html, and asserts
// the file exists.
//
// The test PASSES BECAUSE the history is Illegal (mirroring porcupine's own
// visualization_test.go, which asserts Illegal on crafted bad histories) — this
// is the sanctioned way to exercise the artifact path without breaking the
// build (RESEARCH SC4 fault-injection note). It uses CheckOperationsVerbose
// (LOCKED) because the bool CheckOperations yields no LinearizationInfo to
// visualize. test/artifacts/ is already gitignored (.gitignore line 16), so the
// produced HTML is never committed — this asserts only that the file was
// written on THIS run.
func TestFigure8ViolationProducesArtifact(t *testing.T) {
	const timeout = 10 * time.Second
	const seed = 8 // fixed filename component: <seed>.html

	// A committed write of "c8" (acknowledged by t=20) that a strictly-later read
	// [30,40] claims never happened (observes the pre-write absent value) — a lost
	// committed write, which no linearization permits.
	ops := []porcupine.Operation{
		{ClientId: 1, Input: setIn("k", "c8"), Output: kvOutput{}, Call: 10, Return: 20}, // committed write of c8
		{ClientId: 2, Input: getIn("k"), Output: obs(""), Call: 30, Return: 40},          // later read sees ABSENT -> Illegal
	}

	res, info := porcupine.CheckOperationsVerbose(kvModel, ops, timeout)
	if res != porcupine.Illegal {
		t.Fatalf("crafted Figure-8 violation expected Illegal, got %v", res)
	}

	// go test runs each package test with CWD = the package dir
	// (test/linearizability), so ../../test/artifacts/linearizability resolves to
	// <repo>/test/artifacts/linearizability/.
	path := filepath.Join("..", "..", "test", "artifacts", "linearizability", fmt.Sprintf("%d.html", seed))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll %s: %v", filepath.Dir(path), err)
	}
	if err := porcupine.VisualizePath(kvModel, info, path); err != nil {
		t.Fatalf("VisualizePath: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected HTML artifact at %s: %v", path, err)
	}
}
