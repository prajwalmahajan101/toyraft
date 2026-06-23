package raft

import "errors"

// ErrSnapshotUnsupported is the v1 sentinel returned by every storage
// snapshot stub and by StateMachine.Snapshot/Restore. v2 will populate
// snapshot semantics without changing this sentinel (STOR-01 forward-compat;
// LLD §5 Global Invariant 5).
//
// The value is declared in pkg/raft and re-exported by pkg/storage as
// storage.ErrSnapshotUnsupported. This direction avoids the import cycle
// that would arise if pkg/storage owned the sentinel (pkg/storage already
// imports pkg/raft for raft.Entry / raft.HardState / raft.Index / raft.Term).
//
// errors.Is and errors.Unwrap work transparently across the re-export
// because both packages reference the same *errorString value.
//
// See LLD §4 and ADR-0005.
var ErrSnapshotUnsupported = errors.New("raft: snapshot not supported in v1")

// ErrInvalidConfig is returned by Config.Validate when a required field is
// missing or violates a configured invariant (P1-5 — HeartbeatInterval*3 >
// ElectionTimeoutMin, or self not in Peers, or empty ID, etc.). Wrap with
// fmt.Errorf using %w when reporting which field failed.
var ErrInvalidConfig = errors.New("raft: invalid config")

// ErrStopped is returned by node.Step when the node has not yet completed
// its start-up sequence (LoadHardState has not run). This makes a stray
// MsgTick before start a deterministic no-op error rather than a panic on
// uninitialised state (Pitfall 6 — pre-start state machine guard).
var ErrStopped = errors.New("raft: node not started")
