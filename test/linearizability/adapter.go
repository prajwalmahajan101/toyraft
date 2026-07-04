package linearizability

import (
	"encoding/json"
	"fmt"

	"github.com/anishathalye/porcupine"

	"github.com/prajwalmahajan101/toyraft/internal/raftest"
)

// FromHistory normalizes a recorded []raftest.HistoryEvent into
// []porcupine.Operation whose Input/Output are the typed kvInput/kvOutput the
// 12-01 model expects.
//
// The chaos recorders emit ad-hoc shapes that would panic the model's
// type-asserts if fed to Porcupine directly (RESEARCH Pitfall 2):
//
//   - Input: the inproc matrix records string(entry.Data) — the kvsm.Op JSON
//     envelope {op,key,value} (value base64). FromHistory decodes that into a
//     typed kvInput{Op,Key,Value:string(value)}. An already-typed kvInput passes
//     through unchanged.
//   - Output: the inproc matrix records a 1-based commit-index int; a process-kill
//     dump may record an ad-hoc map[string]any like {"value":v} / {"not_found":true}.
//     FromHistory maps these to a kvOutput. An already-typed kvOutput passes through.
//
// An Output that cannot be interpreted as a register result returns an error
// rather than a silent type-assert — so a bad dump fails loudly here instead of
// panicking inside the model's Step.
//
// The three scripted scenarios in scenarios.go are authored already-typed as
// kvInput/kvOutput, so they do NOT flow through FromHistory. This adapter exists
// for the optional real-dump smoke path and to prove the recorded shapes are
// interpretable.
func FromHistory(events []raftest.HistoryEvent) ([]porcupine.Operation, error) {
	// Reuse ToPorcupine for the timestamp/client copy, then rewrite Input/Output.
	ops := raftest.ToPorcupine(events)
	for i := range ops {
		in, err := normalizeInput(ops[i].Input)
		if err != nil {
			return nil, fmt.Errorf("op[%d] input: %w", i, err)
		}
		out, err := normalizeOutput(ops[i].Output)
		if err != nil {
			return nil, fmt.Errorf("op[%d] output: %w", i, err)
		}
		ops[i].Input = in
		ops[i].Output = out
	}
	return ops, nil
}

// opEnvelope mirrors pkg/kvsm.Op: the JSON {op,key,value} the recorder stores as
// string(entry.Data). Value uses encoding/json's default []byte (base64) coding.
type opEnvelope struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

// normalizeInput converts a recorded Input into a typed kvInput.
func normalizeInput(raw any) (kvInput, error) {
	switch v := raw.(type) {
	case kvInput:
		return v, nil
	case string:
		return decodeOpEnvelope([]byte(v))
	case []byte:
		return decodeOpEnvelope(v)
	default:
		return kvInput{}, fmt.Errorf("uninterpretable Input of type %T", raw)
	}
}

func decodeOpEnvelope(data []byte) (kvInput, error) {
	var env opEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return kvInput{}, fmt.Errorf("decode kvsm.Op envelope %q: %w", string(data), err)
	}
	if env.Op == "" || env.Key == "" {
		return kvInput{}, fmt.Errorf("envelope missing op/key: %q", string(data))
	}
	return kvInput{Op: env.Op, Key: env.Key, Value: string(env.Value)}, nil
}

// normalizeOutput converts a recorded Output into a typed kvOutput. A commit-index
// int (inproc) carries no register value, so it maps to the zero kvOutput; a
// {"value":...}/{"not_found":true} map (process-kill) maps to the observed value.
func normalizeOutput(raw any) (kvOutput, error) {
	switch v := raw.(type) {
	case kvOutput:
		return v, nil
	case int:
		// A commit-index acknowledgement carries no observed register value.
		return kvOutput{}, nil
	case int64:
		return kvOutput{}, nil
	case map[string]any:
		if nf, ok := v["not_found"].(bool); ok && nf {
			return kvOutput{Found: false}, nil
		}
		if val, ok := v["value"]; ok {
			s, ok := val.(string)
			if !ok {
				return kvOutput{}, fmt.Errorf("Output map \"value\" is %T, want string", val)
			}
			return kvOutput{Value: s, Found: true}, nil
		}
		return kvOutput{}, fmt.Errorf("uninterpretable Output map %v", v)
	default:
		return kvOutput{}, fmt.Errorf("uninterpretable Output of type %T", raw)
	}
}
