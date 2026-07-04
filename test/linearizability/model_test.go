package linearizability

import (
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// checkTimeout is generous relative to the tiny (<=4 op) histories below, so
// Porcupine returns a definite Ok/Illegal and never Unknown.
const checkTimeout = 10 * time.Second

// TestModelAcceptsLinearizableHistory proves the model is not over-strict: a
// genuinely linearizable overlapping history returns porcupine.Ok.
//
// History (single key "x"):
//
//	set(x,"A")  [0,10]
//	get(x)->"A" [5,15]   (overlaps the set; may linearize after it)
//	get(x)->"A" [12,20]  (after the set returns; must observe "A")
//
// A valid linearization is set, get, get — so the model must accept it.
func TestModelAcceptsLinearizableHistory(t *testing.T) {
	ops := []porcupine.Operation{
		{ClientId: 0, Input: kvInput{Op: "set", Key: "x", Value: "A"}, Call: 0, Output: kvOutput{}, Return: 10},
		{ClientId: 1, Input: kvInput{Op: "get", Key: "x"}, Call: 5, Output: kvOutput{Value: "A", Found: true}, Return: 15},
		{ClientId: 2, Input: kvInput{Op: "get", Key: "x"}, Call: 12, Output: kvOutput{Value: "A", Found: true}, Return: 20},
	}

	res, _ := porcupine.CheckOperationsVerbose(kvModel, ops, checkTimeout)
	if res != porcupine.Ok {
		t.Fatalf("expected linearizable history to be Ok, got %v", res)
	}
}

// TestModelRejectsNonLinearizableHistory proves the model is NOT vacuous: a
// crafted history that no interleaving can justify returns porcupine.Illegal.
// A rubber-stamp model that always returned Ok would fail this test.
//
// History (single key "x"), all intervals DISJOINT and ordered:
//
//	set(x,"A")  [0,10]
//	set(x,"B")  [11,20]  (starts strictly after A returns)
//	get(x)->"A" [21,30]  (starts strictly after B returns)
//
// The only total order is set A, set B, get. The get runs entirely after B
// committed, so it must observe "B" — observing "A" is impossible. The model
// must reject this.
func TestModelRejectsNonLinearizableHistory(t *testing.T) {
	ops := []porcupine.Operation{
		{ClientId: 0, Input: kvInput{Op: "set", Key: "x", Value: "A"}, Call: 0, Output: kvOutput{}, Return: 10},
		{ClientId: 1, Input: kvInput{Op: "set", Key: "x", Value: "B"}, Call: 11, Output: kvOutput{}, Return: 20},
		{ClientId: 2, Input: kvInput{Op: "get", Key: "x"}, Call: 21, Output: kvOutput{Value: "A", Found: true}, Return: 30},
	}

	res, _ := porcupine.CheckOperationsVerbose(kvModel, ops, checkTimeout)
	if res != porcupine.Illegal {
		t.Fatalf("expected non-linearizable history to be Illegal, got %v (model may be vacuous)", res)
	}
}
