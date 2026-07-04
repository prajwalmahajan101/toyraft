package kvsm

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// Op is the JSON envelope a client Proposes into the log. The daemon marshals
// it into raft.Entry.Data; Apply unmarshals it on every committed entry.
//
// Kind is "set" or "del". Value uses encoding/json's default []byte encoding
// (standard base64), matching the WIRE frame contract.
type Op struct {
	Kind  string `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

// KV is the reference key/value StateMachine: a mutex-guarded
// map[string][]byte. It satisfies raft.StateMachine (Apply/Snapshot/Restore)
// and additionally exposes Get for the leader-only-read path.
type KV struct {
	mu sync.RWMutex
	m  map[string][]byte
}

// New returns an empty KV ready for Apply and Get.
func New() *KV {
	return &KV{m: make(map[string][]byte)}
}

// Apply decodes the Op envelope in entry.Data and mutates the map
// deterministically. It is called from the single apply goroutine, in index
// order, per the raft.StateMachine contract; it takes the write lock so a
// concurrent Get on the client-API goroutine observes a consistent map.
//
//   - "set": stores Value and returns (Value, nil).
//   - "del": deletes Key and returns (nil, nil).
//   - unknown Kind: returns a non-nil error.
//
// A JSON decode failure returns the error to the proposing Propose caller
// (delivered per transport.go's Apply error contract) and leaves the map
// unchanged. No wall-clock is read — Apply is fully deterministic.
func (k *KV) Apply(entry raft.Entry) (any, error) {
	var op Op
	if err := json.Unmarshal(entry.Data, &op); err != nil {
		return nil, fmt.Errorf("kvsm: decode op: %w", err)
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	switch op.Kind {
	case "set":
		k.m[op.Key] = op.Value
		return op.Value, nil
	case "del":
		delete(k.m, op.Key)
		return nil, nil
	default:
		return nil, fmt.Errorf("kvsm: unknown op %q", op.Kind)
	}
}

// Get returns the value stored at key and whether it is present. It is a
// mutex-guarded read serving the WIRE §5.2 leader-only-read path and is NOT
// part of raft.StateMachine — the client GET handler calls it directly on the
// leader because Propose discards the Apply result.
func (k *KV) Get(key string) ([]byte, bool) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	v, ok := k.m[key]
	return v, ok
}

// Snapshot is a v1 stub; snapshotting is unsupported (STOR-01 forward-compat).
func (k *KV) Snapshot() ([]byte, raft.Index, error) {
	return nil, 0, raft.ErrSnapshotUnsupported
}

// Restore is a v1 stub; snapshotting is unsupported (STOR-01 forward-compat).
func (k *KV) Restore([]byte) error {
	return raft.ErrSnapshotUnsupported
}

// Compile-time assertion that *KV satisfies the frozen StateMachine interface.
var _ raft.StateMachine = (*KV)(nil)
