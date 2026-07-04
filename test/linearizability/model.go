// Package linearizability defines the Porcupine model and history-shape checks
// that Phase-12 uses to prove ToyRaft's key/value register is linearizable.
//
// The model here is the phase's correctness oracle: every downstream scenario
// (12-02) and the Figure 7/8 artifacts (12-03) feed recorded histories through
// kvModel. If the model were wrong or vacuous it would silently pass a broken
// Raft, so model_test.go proves it accepts a linearizable history AND rejects a
// crafted non-linearizable one.
package linearizability

import (
	"fmt"

	"github.com/anishathalye/porcupine"
)

// kvInput is the op+args half of a recorded operation. It mirrors the client
// surface of pkg/kvsm: Kind is "set"|"del" on the write path plus "get" on the
// leader-only read path.
//
// This type is a semantic mirror of pkg/kvsm — the model does NOT import or call
// kvsm at runtime (Step reproduces kvsm.Apply/Get semantics directly).
type kvInput struct {
	Op    string // "get" | "set" | "del"
	Key   string
	Value string // the value for "set"; empty for "get"/"del"
}

// kvOutput is the result half of a recorded operation.
type kvOutput struct {
	Value string // observed value for a "get"; "" for "set"/"del"/absent
	Found bool   // true iff the key is present (reserved for a future present-but-empty state)
}

// kvModel is the KV-register model whose Step reproduces pkg/kvsm semantics
// EXACTLY, mirroring kvsm.Apply/Get (kvsm.go:47-77):
//
//   - "set": an unconditional write; state becomes the set value (kvsm stores op.Value).
//   - "del": deletes the key; state collapses to "" (kvsm does delete(map,key)).
//   - "get": legal iff the observed value equals the current state (kvsm.Get read-back).
//
// Init returns "" (empty). Because kvsm's Get returns (nil,false) for BOTH a
// never-set key and a deleted key, del and never-set collapse to the SAME Init
// state here.
//
// FORBID-EMPTY-VALUE CONSTRAINT (locked del/empty resolution, RESEARCH Pitfall 5):
// Scenarios MUST NOT `set` an empty value; state is a plain string where "" means
// absent/deleted. Never-set, del, and set("") would otherwise be indistinguishable
// under a plain-string state. If a future scenario needs present-but-empty, promote
// state to `struct{V string; Present bool}` and carry the kvOutput.Found bit.
//
// Partition groups operations by key so an N-key history becomes N independent
// per-key checks (RESEARCH Pattern 1 + perf). DescribeOperation/DescribeState
// render readable labels for VisualizePath in the Figure-8 artifact (12-03/SC4).
var kvModel = porcupine.Model{
	// Partition splits the flat operation slice into one sub-history per key,
	// so Porcupine runs N tiny independent per-key linearizations instead of
	// one exponential cross-key check.
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := make(map[string][]porcupine.Operation)
		var order []string
		for _, op := range history {
			key := op.Input.(kvInput).Key
			if _, seen := byKey[key]; !seen {
				order = append(order, key)
			}
			byKey[key] = append(byKey[key], op)
		}
		parts := make([][]porcupine.Operation, 0, len(order))
		for _, key := range order {
			parts = append(parts, byKey[key])
		}
		return parts
	},

	// Init is the empty/absent register state. "" == absent (kvsm collapses
	// del and never-set to (nil,false)).
	Init: func() interface{} {
		return ""
	},

	// Step advances the per-key register state, returning whether the operation
	// is legal from the current state.
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(string)
		in := input.(kvInput)
		out := output.(kvOutput)
		switch in.Op {
		case "set":
			// Unconditional write: always legal, state becomes the value.
			return true, in.Value
		case "del":
			// Delete: always legal, state collapses to absent ("").
			return true, ""
		case "get":
			// Read: legal iff the observed value equals the current state.
			return out.Value == st, st
		default:
			return false, st
		}
	},

	// Equal compares the comparable string states. (Porcupine would fall back
	// to == here, but making it explicit documents the state type.)
	Equal: func(a, b interface{}) bool {
		return a.(string) == b.(string)
	},

	// DescribeOperation renders readable labels for VisualizePath (12-03/SC4).
	DescribeOperation: func(input, output interface{}) string {
		in := input.(kvInput)
		switch in.Op {
		case "set":
			return fmt.Sprintf("set(%q, %q)", in.Key, in.Value)
		case "del":
			return fmt.Sprintf("del(%q)", in.Key)
		case "get":
			return fmt.Sprintf("get(%q) -> %q", in.Key, output.(kvOutput).Value)
		default:
			return fmt.Sprintf("%s(%q)", in.Op, in.Key)
		}
	},

	// DescribeState renders the per-key register value for the visualization.
	DescribeState: func(state interface{}) string {
		return fmt.Sprintf("%q", state.(string))
	},
}
