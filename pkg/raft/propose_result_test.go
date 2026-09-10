package raft

import (
	"bytes"
	"context"
	"testing"
)

// TestProposeReturnsApplyResult proves Friction-3: Propose surfaces the opaque
// value StateMachine.Apply returned for the committed entry, so an embedder can
// recover an Apply-computed value without an out-of-band registry. recordingSM
// returns e.Data from Apply, so the result must equal the proposed bytes.
func TestProposeReturnsApplyResult(t *testing.T) {
	t.Parallel()
	sm := &recordingSM{}
	n, clk := newSingleNode(t, sm, nil)
	advanceUntil(t, clk, testTick, func() bool { return n.Status().Role == Leader })

	type out struct {
		res any
		err error
	}
	done := make(chan out, 1)
	go func() {
		_, _, res, err := n.Propose(context.Background(), []byte("hello"))
		done <- out{res, err}
	}()

	var got out
	for range 400 {
		select {
		case got = <-done:
			goto returned
		default:
			clk.Advance(testTick)
		}
	}
	t.Fatal("Propose did not return")
returned:
	if got.err != nil {
		t.Fatalf("Propose: %v", got.err)
	}
	b, ok := got.res.([]byte)
	if !ok {
		t.Fatalf("Propose result type = %T, want []byte", got.res)
	}
	if !bytes.Equal(b, []byte("hello")) {
		t.Errorf("Propose result = %q, want %q", b, "hello")
	}
}
