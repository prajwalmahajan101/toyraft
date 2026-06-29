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

// ErrStopped is the single lifecycle sentinel for a node that is not
// accepting work. It covers TWO cases (LLD §3 names exactly one sentinel,
// so both share it and errors.Is(err, ErrStopped) stays the lone classifier):
//
//   - Pre-start guard: node.Step before the start-up sequence completes
//     (LoadHardState has not run). Makes a stray MsgTick before start a
//     deterministic no-op error rather than a panic on uninitialised state
//     (Pitfall 6 — pre-start state machine guard).
//   - Post-Stop guard (Phase 7): after the public Node's Stop returns,
//     Propose, Step, and Status all return ErrStopped (LLD §3 Node contract).
//
// The message is broadened to read correctly in both cases.
var ErrStopped = errors.New("raft: node stopped or not started")

// ErrNotLeader is returned by Propose (and by Transport handlers) when this
// node is not the current leader. LeaderHint carries the best-known current
// leader NodeID (best-effort; may be empty if unknown). API-04 / LLD §3-§4.
//
// Invariants:
//   - LeaderHint, if non-empty, MUST refer to a NodeID in Config.Peers.
//   - Wire projection: docs/WIRE.md X-Raft-Leader-Hint header carries
//     LeaderHint verbatim; the JSON error envelope carries it as
//     "leader_hint".
//
// A POINTER receiver implements error, so the error value is &ErrNotLeader{...}.
type ErrNotLeader struct {
	LeaderHint NodeID
}

func (e *ErrNotLeader) Error() string {
	if e.LeaderHint == "" {
		return "raft: not leader"
	}
	return "raft: not leader (leader hint: " + string(e.LeaderHint) + ")"
}

// ErrProposalDropped is returned by Propose when leadership is lost before
// the proposed entry commits. Safe to retry (LLD §3 Propose contract).
var ErrProposalDropped = errors.New("raft: proposal dropped (leadership lost before commit)")
