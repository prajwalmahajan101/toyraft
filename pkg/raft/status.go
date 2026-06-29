package raft

// Status is a point-in-time snapshot of a node's observable state.
// Returned by Node.Status() for /debug and operator tooling.
//
// Invariants:
//   - The returned MatchIndex map is a copy; callers may mutate freely.
//   - Status is consistent within itself (no partial reads across fields)
//     but may be stale by the time it is observed.
//
// LLD §3.
type Status struct {
	Role        Role
	Term        Term
	CommitIndex Index
	ApplyIndex  Index
	LeaderHint  NodeID           // best-known current leader, or empty
	MatchIndex  map[NodeID]Index // leader-only; nil on followers
}
