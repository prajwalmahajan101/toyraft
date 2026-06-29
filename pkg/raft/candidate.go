package raft

// quorum returns the majority count: peers/2 + 1. peers INCLUDES self
// (Pitfall 8), so for a 3-node cluster this is 2 (self-vote + 1 grant),
// for a 5-node cluster this is 3 (self-vote + 2 grants).
func (n *node) quorum() int {
	return len(n.peers)/2 + 1
}

// becomeCandidateLocked promotes the node to Candidate per Raft §5.2.
// Caller MUST hold n.mu.
//
// Order of operations (ELEC-03 / ELEC-04 / ELEC-05):
//  1. role -> Candidate
//  2. currentTerm++ (new election term)
//  3. votedFor = self (self-vote)
//  4. leaderHint cleared (no leader yet in this term)
//  5. votesReceived seeded with {self: true} (Pitfall 8 — self counts)
//  6. electionElapsed reset; electionTimeout redrawn from the per-node RNG
//  7. queueHardStateLocked — persist (term, votedFor=self) BEFORE fan-out
//     (SC5 persist-first ordering — same constraint as 05-02's vote grant)
//  8. fan out MsgRequestVote to every peer except self
//
// Called by tickFollowerLocked (election timeout, 05-02) and
// tickCandidateLocked (split-vote retry — Pitfall 4 below).
func (n *node) becomeCandidateLocked() {
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderHint = ""
	n.votesReceived = map[NodeID]bool{n.id: true}
	n.resetElectionTimeoutLocked()
	n.queueHardStateLocked()

	for _, peer := range n.peers {
		if peer == n.id {
			continue
		}
		n.queueMsgLocked(Message{
			Type:         MsgRequestVote,
			Term:         n.currentTerm,
			From:         n.id,
			To:           peer,
			LastLogIndex: n.log.LastIndex(),
			LastLogTerm:  n.log.LastTerm(),
		})
	}

	// Single-node fast path (ELEC-04): the self-vote may ALREADY be a quorum.
	// For an N=1 cluster quorum()==1 and votesReceived=={self}, so the
	// candidate wins its own election immediately — there are no peers to
	// solicit a vote from, and without this check the node would re-elect on
	// every timeout and never lead. For N>=3 a lone self-vote is never a
	// majority, so this is a no-op and multi-node behaviour is unchanged.
	if len(n.votesReceived) >= n.quorum() {
		n.becomeLeaderLocked()
	}
}

// tickCandidateLocked implements Pitfall 4 — a candidate that does not
// reach quorum within its randomised election timeout MUST restart the
// election (Term++, fresh self-vote, fresh timeout, fresh fan-out)
// rather than sit idle forever. Caller MUST hold n.mu.
func (n *node) tickCandidateLocked() {
	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		n.becomeCandidateLocked()
	}
}

// handleRequestVoteResponseLocked counts unique grants and, on quorum,
// transitions to Leader (ELEC-04). Caller MUST hold n.mu.
//
// Stale-response guards (the load-bearing safety):
//   - n.role != Candidate: a step-down already happened (maybeStepDownLocked
//     ran in stepLocked for the higher-term case, or we are already Leader).
//   - m.Term != n.currentTerm: response is for a prior election round
//     (Pitfall 4 retry already incremented the term).
//   - !m.VoteGranted: explicit denial — count nothing.
//
// votesReceived is a set keyed by voter NodeID so duplicate grants from
// the same peer (re-delivery under chaos / Phase 4 hub) cannot
// double-count toward quorum.
func (n *node) handleRequestVoteResponseLocked(m Message) {
	if n.role != Candidate || m.Term != n.currentTerm {
		return
	}
	if !m.VoteGranted {
		return
	}
	n.votesReceived[m.From] = true
	if len(n.votesReceived) >= n.quorum() {
		n.becomeLeaderLocked()
	}
}

// resetElectionTimeoutLocked is owned by 05-02 (pkg/raft/follower.go);
// becomeCandidateLocked above calls it to redraw the per-term randomised
// election timeout. Kept here as a doc anchor only.

// wireElectionTriggerLocked installs becomeCandidateLocked as the
// follower's election-timeout trigger (the W2-parallel hook 05-02
// declared on *node). Called once from newNode after the start
// sequence; 05-02 ships the hook as nil-safe so its own tests pass
// without depending on becomeCandidateLocked existing. After both
// plans merge, the hook is always wired and the tickFollowerLocked
// path collapses to the canonical Raft §5.2 promotion.
func (n *node) wireElectionTriggerLocked() {
	n.onElectionTrigger = n.becomeCandidateLocked
}
