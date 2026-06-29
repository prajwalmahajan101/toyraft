package raft

import "slices"

// maybeAdvanceCommitLocked advances commitIndex to the largest index that
// is (a) replicated on a quorum AND (b) from the CURRENT term — the
// Raft §5.4.2 commit restriction (the Figure-8 fix). Caller MUST hold
// n.mu. (RESEARCH Pattern 3; REPL-06 + REPL-08.)
//
// REPL-08 / C-8 — sorted value snapshot, never map iteration. We build a
// slice of matchIndex VALUES and slices.Sort it; we NEVER range the map
// and "count how many >= N", because Go randomises map iteration order
// and a count-in-map-order quorum is non-deterministic and chaos-flaky
// (RESEARCH anti-pattern "map-iteration quorum"). The largest index
// replicated on a majority is the (n.quorum())-th from the top, i.e.
// matches[len(matches)-n.quorum()] in ascending order. matchIndex is
// keyed over ALL peers including self (becomeLeaderLocked), so
// len(matches) == len(peers) and that index is always in range.
//
// Pitfall 6 — self counts as log.LastIndex(). The leader has every entry
// in its own log, but the stored matchIndex[self] may lag (it is only
// bumped on proposeLocked). We substitute n.log.LastIndex() for the self
// entry in the snapshot so a 3-node cluster (self + 1 follower) can reach
// quorum. We substitute IN THE SNAPSHOT only — we do not mutate the map.
//
// REPL-06 Figure-8 GUARD — current-term rule. We advance commitIndex to
// quorumIndex ONLY when it is strictly above the current commitIndex AND
// log[quorumIndex].term == currentTerm. A prior-term entry is NEVER
// committed by replica count (RESEARCH anti-pattern "committing
// prior-term entries by replica count" — the canonical Figure-8 data
// loss); it commits indirectly once a current-term entry above it reaches
// quorum and the Log Matching property carries the earlier entries with
// it. commitIndex rides in HardState.Commit, so a bump queues HardState.
//
// quorum() is the single source of truth for majority size (Pitfall 8;
// it already accounts for self-in-peers).
func (n *node) maybeAdvanceCommitLocked() {
	matches := make([]Index, 0, len(n.matchIndex))
	for peer, mi := range n.matchIndex {
		if peer == n.id {
			mi = n.log.LastIndex() // leader has everything (Pitfall 6)
		}
		matches = append(matches, mi)
	}
	slices.Sort(matches)

	quorumIndex := matches[len(matches)-n.quorum()]
	if quorumIndex <= n.commitIndex {
		return
	}
	if term, _ := n.log.Term(quorumIndex); term != n.currentTerm {
		// Prior-term entry: never committed by replica count (REPL-06).
		return
	}
	n.commitIndex = quorumIndex
	n.queueHardStateLocked()
}
