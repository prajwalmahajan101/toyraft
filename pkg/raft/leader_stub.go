package raft

// becomeLeaderLocked is the Leader-state transition. Caller MUST hold n.mu.
//
// Responsibilities landed here (per Raft Figure 2 leader-state init):
//   - role -> Leader
//   - leaderHint = self (so subsequent Status() / client redirects see us)
//   - nextIndex[peer] = log.LastIndex()+1 for every peer (optimistic start)
//   - matchIndex[peer] = 0 for every peer (nothing known-replicated yet)
//   - heartbeat counters reset (heartbeatElapsed=0, heartbeatTimeout=1)
//   - immediate heartbeat fan-out to every peer (REPL-01)
//
// The immediate fan-out asserts leader authority on the SAME step that wins
// the election rather than waiting a tick. This stops the Phase-5
// re-election storm at its source: without it, freshly-elected leaders sat
// silent for one full tick interval, long enough for a follower to time out
// and start a competing election within the term-stream. Fanning out now
// resets every follower's election timer (Pitfall 3) before it can fire.
//
// nextIndex/matchIndex are populated for ALL peers including self. Self-
// entries are harmless (the leader's own log is the authoritative one)
// and keep the map shape uniform for the commit-rule iteration.
func (n *node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderHint = n.id
	nextIdx := n.log.LastIndex() + 1
	for _, peer := range n.peers {
		n.nextIndex[peer] = nextIdx
		n.matchIndex[peer] = 0
	}
	// heartbeatTimeout==1: tickInterval()==HeartbeatInterval, so one
	// heartbeat per tick — the driver's tick cadence drives the cadence.
	n.heartbeatElapsed = 0
	n.heartbeatTimeout = 1
	// Immediate first heartbeat round (REPL-01) — see doc above.
	for _, peer := range n.peers {
		if peer == n.id {
			continue
		}
		n.sendAppendEntriesLocked(peer)
	}
}
