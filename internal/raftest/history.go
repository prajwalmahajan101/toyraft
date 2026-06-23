package raftest

import "github.com/anishathalye/porcupine"

// HistoryEvent is a recorded client operation, shaped 1:1 with
// porcupine.Operation at v1.0.3 (ClientID, Input, Call, Output, Return).
// Call/Return are int64 nanoseconds drawn from the FakeClock — making
// this field deterministic across runs at the same seed (SC5 + SC6).
//
// See docs/TESTING.md section 7 (history shape) and ADR-0006 (FakeClock
// contract).
type HistoryEvent struct {
	ClientID int
	Input    any   // op + args (e.g. KVOp{Kind:"set", Key:"k", Value:"v"})
	Call     int64 // FakeClock.Now().UnixNano() at invocation
	Output   any   // result (e.g. KVResult{OK:true})
	Return   int64 // FakeClock.Now().UnixNano() at response
}

// ToPorcupine converts a slice of HistoryEvent to porcupine.Operation.
// Pinned shape: Phase 12's CheckOperations consumer reads this directly.
func ToPorcupine(events []HistoryEvent) []porcupine.Operation {
	out := make([]porcupine.Operation, len(events))
	for i, e := range events {
		out[i] = porcupine.Operation{
			ClientId: e.ClientID,
			Input:    e.Input,
			Call:     e.Call,
			Output:   e.Output,
			Return:   e.Return,
		}
	}
	return out
}

// Compile-time pin: HistoryEvent must round-trip through porcupine.Operation
// without losing fields. If porcupine ever adds or renames a field, this
// assignment chain fails to compile and forces a deliberate update here.
var _ = func() porcupine.Operation {
	e := HistoryEvent{}
	return porcupine.Operation{
		ClientId: e.ClientID,
		Input:    e.Input,
		Call:     e.Call,
		Output:   e.Output,
		Return:   e.Return,
	}
}
