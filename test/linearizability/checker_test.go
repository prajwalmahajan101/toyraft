package linearizability

import (
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// TestScenariosLinearizable is the SC2 headline: it runs Porcupine over each of
// the three named concurrent scenarios (steady-state, leader-churn, packet-loss)
// and asserts every one is linearizable (porcupine.Ok).
//
// It uses CheckOperationsVerbose (NOT the bool CheckOperations) — the LOCKED
// decision requires the *Verbose form so 12-03/SC4 can obtain LinearizationInfo
// for the Figure-8 VisualizePath; we use it here too for consistency (the info
// return is intentionally ignored in this positive test).
//
// porcupine.Unknown (the checker timed out) is treated as a TEST FAILURE, never as
// a pass: for a correctness gate, "could not certify" is not "certified" (RESEARCH
// anti-pattern). The histories are sized at tens of ops so Unknown never occurs on
// a green run; a 10s per-scenario budget is generous headroom.
func TestScenariosLinearizable(t *testing.T) {
	const timeout = 10 * time.Second

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			res, _ := porcupine.CheckOperationsVerbose(kvModel, s.ops, timeout)
			switch res {
			case porcupine.Ok:
				// pass: the history is linearizable.
			case porcupine.Illegal:
				t.Fatalf("scenario %q unexpectedly NOT linearizable (Illegal)", s.name)
			case porcupine.Unknown:
				t.Fatalf("scenario %q linearizability UNKNOWN (timeout) — cannot certify; shrink the history", s.name)
			default:
				t.Fatalf("scenario %q: unexpected checker result %v", s.name, res)
			}
		})
	}
}
