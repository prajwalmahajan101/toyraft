package linearizability

import (
	"testing"

	"github.com/prajwalmahajan101/toyraft/internal/raftest"
)

// TestHistoryShape satisfies SC1/LIN-01: every recorded client op carries the
// (invocation_time, response_time, op, args, result) five-tuple. It builds a
// representative []raftest.HistoryEvent, round-trips it through the pinned
// raftest.ToPorcupine seam (the same seam the chaos suites and Phase-12
// scenarios use), and asserts each resulting porcupine.Operation is well-formed:
//
//	invocation_time = Call   (>= 0)
//	response_time   = Return (>= Call, closed interval)
//	op + args       = Input  (non-nil, a kvInput with a valid Op and non-empty Key)
//	result          = Output (non-nil, a kvOutput)
func TestHistoryShape(t *testing.T) {
	events := []raftest.HistoryEvent{
		{
			ClientID: 1,
			Input:    kvInput{Op: "set", Key: "k", Value: "v"},
			Call:     10,
			Output:   kvOutput{},
			Return:   20,
		},
		{
			ClientID: 2,
			Input:    kvInput{Op: "get", Key: "k"},
			Call:     25,
			Output:   kvOutput{Value: "v", Found: true},
			Return:   35,
		},
	}

	ops := raftest.ToPorcupine(events)
	if len(ops) != len(events) {
		t.Fatalf("ToPorcupine returned %d ops, want %d", len(ops), len(events))
	}

	validOps := map[string]bool{"get": true, "set": true, "del": true}

	for i, op := range ops {
		// invocation_time present and non-negative.
		if op.Call < 0 {
			t.Fatalf("op[%d]: Call = %d, want >= 0", i, op.Call)
		}
		// response_time at or after invocation (closed interval).
		if op.Return < op.Call {
			t.Fatalf("op[%d]: Return = %d < Call = %d", i, op.Return, op.Call)
		}
		// op + args present.
		if op.Input == nil {
			t.Fatalf("op[%d]: Input is nil (op+args missing)", i)
		}
		// result present.
		if op.Output == nil {
			t.Fatalf("op[%d]: Output is nil (result missing)", i)
		}
		// op/args are interpretable as a register operation.
		in, ok := op.Input.(kvInput)
		if !ok {
			t.Fatalf("op[%d]: Input is %T, want kvInput", i, op.Input)
		}
		if !validOps[in.Op] {
			t.Fatalf("op[%d]: Op = %q, want one of get/set/del", i, in.Op)
		}
		if in.Key == "" {
			t.Fatalf("op[%d]: Key is empty", i)
		}
		// result is interpretable as a register result.
		if _, ok := op.Output.(kvOutput); !ok {
			t.Fatalf("op[%d]: Output is %T, want kvOutput", i, op.Output)
		}
	}
}
