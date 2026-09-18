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
	CommitIndex Index // highest log index known committed (quorum-replicated)
	ApplyIndex  Index // highest index passed to StateMachine.Apply (Apply returned)
	// last index present in the local log. Monotonic; the observable invariant
	// an INFO/lag view relies on is LastLogIndex >= CommitIndex >= ApplyIndex.
	LastLogIndex Index
	LeaderHint   NodeID // best-known current leader, or empty
	// leader-only (nil on followers). Keyed over ALL peers INCLUDING this node
	// (self is keyed at LastLogIndex). A per-*follower* ack/lag count must
	// therefore exclude NodeID(), else it is off by one — WAIT n would pass on
	// n-1 real followers (dogfood FRICTION-06 / DOC-02).
	MatchIndex map[NodeID]Index
}
