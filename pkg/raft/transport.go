package raft

import "context"

// Transport ships Raft Messages between peers. Implementations may be lossy,
// reorder, or duplicate — Raft is resilient to all three. They MUST NOT
// mutate a Message after it is handed to Send.
//
// LLD §3. The driver (07-03) wires Transport.Register before the tick loop
// starts and routes outbound Ready messages through Send.
type Transport interface {
	// Send is best-effort. Implementations SHOULD apply a bounded timeout;
	// errors are logged but not surfaced to the Raft core (the heartbeat
	// mechanism is the retry strategy).
	//
	// Invariants:
	//   - Safe to call from any goroutine.
	//   - MUST NOT block indefinitely; bound by an implementation-defined
	//     timeout (e.g. 1s for HTTP).
	//
	// Error contract:
	//   - Wraps the underlying network error with %w.
	//   - Returning an error does NOT cause Raft to retry; the next
	//     heartbeat is the retry signal.
	Send(ctx context.Context, msg Message) error

	// Register installs the inbound callback. The transport invokes step
	// for every received Message. MUST be called before Start.
	//
	// Invariants:
	//   - Called exactly once per Transport instance, by Node.Start, before
	//     any internal goroutine runs.
	//   - step is safe to call from any goroutine.
	Register(step func(ctx context.Context, msg Message) error)

	// Close releases listeners and connections. Idempotent.
	//
	// Error contract:
	//   - Returns the first non-nil error from shutting down resources;
	//     subsequent calls return nil.
	Close() error
}

// StateMachine is the consumer-owned replicated state. Apply is called
// exactly once per committed entry, in index order, from a single goroutine.
//
// v1: Snapshot and Restore are stubs; implementors return
// ErrSnapshotUnsupported. v2 will define the snapshot contract WITHOUT
// breaking this interface (STOR-01 forward-compat). LLD §3.
type StateMachine interface {
	// Apply executes a committed Entry and returns an opaque result that is
	// delivered back to the proposing client if the proposal was local.
	//
	// Invariants:
	//   - Called exactly once per committed Entry, in strictly increasing
	//     Index order (API-05).
	//   - Called from a single goroutine; implementations need not be
	//     internally synchronized for Apply.
	//   - MUST be deterministic: identical Entries from identical state
	//     MUST yield identical results across replicas.
	//   - MUST NOT block indefinitely; long work belongs in a background
	//     goroutine the StateMachine owns.
	//
	// Error contract:
	//   - Non-nil err is delivered to the proposing client's Propose call.
	//   - An err here does NOT roll back the commit; the entry is committed
	//     by definition. Implementations SHOULD treat Apply errors as fatal
	//     (panic) unless the error encodes an application-level "rejected"
	//     outcome.
	Apply(entry Entry) (result any, err error)

	// Snapshot serialises the state up to lastIndex.
	//
	// v1: implementors MUST return (nil, 0, ErrSnapshotUnsupported).
	// v2: will define snapshot semantics; this signature is forward-compatible.
	Snapshot() (data []byte, lastIndex Index, err error)

	// Restore replaces the state from a snapshot produced by Snapshot.
	//
	// v1: implementors MUST return ErrSnapshotUnsupported.
	Restore(data []byte) error
}
