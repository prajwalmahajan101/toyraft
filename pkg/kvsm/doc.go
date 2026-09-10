// Package kvsm is the reference KV StateMachine for the demo: a mutex-guarded
// map[string][]byte. Apply mutates it deterministically from committed entries;
// Get serves the WIRE §5.2 leader-only read path (the client GET /kv handler
// reads the map directly on the leader because Propose discards the Apply
// result). Snapshot/Restore serialise and rebuild the map plus the applied
// index as JSON so an in-memory KV survives a restart via a durable checkpoint
// (ADR-0024) rather than a full log replay.
package kvsm
