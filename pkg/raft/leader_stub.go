package raft

// becomeLeaderLocked is the Phase 5 skeleton for the Leader transition.
// Caller MUST hold n.mu.
//
// Responsibilities landed here (per Raft Figure 2 leader-state init):
//   - role -> Leader
//   - leaderHint = self (so subsequent Status() / client redirects see us)
//   - nextIndex[peer] = log.LastIndex()+1 for every peer (optimistic start)
//   - matchIndex[peer] = 0 for every peer (nothing known-replicated yet)
//
// Phase-6 stand-in: heartbeat fan-out (REPL-01) lives in tickLeaderLocked,
// which remains a no-op in Phase 5. Phase 5 tests assert "candidate
// reached quorum and flipped to Leader" via role inspection; the absence
// of heartbeats means followers will time out again and re-elect within
// the same term-stream — that's acceptable for Phase 5 because the
// chaos invariant SC6 forbids TWO leaders per term, not transient
// re-elections across terms.
//
// nextIndex/matchIndex are populated for ALL peers including self. Self-
// entries are harmless (the leader's own log is the authoritative one)
// and keep the map shape uniform for Phase 6's iteration.
func (n *node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderHint = n.id
	nextIdx := n.log.LastIndex() + 1
	for _, peer := range n.peers {
		n.nextIndex[peer] = nextIdx
		n.matchIndex[peer] = 0
	}
	// No queueMsgLocked here — Phase 6 fills tickLeaderLocked with the
	// heartbeat fan-out (REPL-01).
}
