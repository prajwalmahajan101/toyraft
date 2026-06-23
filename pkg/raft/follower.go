package raft

// resetElectionTimeoutLocked draws a fresh randomised election timeout
// in the tick domain and zeroes the running elapsed counter. Caller
// holds n.mu.
//
// Range: [minTicks, maxTicks) where the bounds are
// cfg.ElectionTimeoutMin / cfg.tickInterval() and
// cfg.ElectionTimeoutMax / cfg.tickInterval(). Config.Validate already
// guarantees Min < Max strictly, so maxTicks-minTicks >= 1 in normal
// configs; the defensive maxTicks = minTicks+1 covers a pathological
// case where the duration-to-tick truncation collapses the range
// (e.g. tickInterval == ElectionTimeoutMax).
//
// Per-node RNG (P1-4): n.rng is a *math/rand/v2.Rand constructed by
// newNodeRNG in newNode. Two nodes with the same Config.Seed but
// different IDs draw divergent timeouts (ROADMAP SC2).
func (n *node) resetElectionTimeoutLocked() {
	tick := n.cfg.tickInterval()
	minTicks := int(n.cfg.ElectionTimeoutMin / tick)
	maxTicks := int(n.cfg.ElectionTimeoutMax / tick)
	if maxTicks <= minTicks {
		maxTicks = minTicks + 1
	}
	n.electionTimeout = minTicks + n.rng.IntN(maxTicks-minTicks)
	n.electionElapsed = 0
}

// tickFollowerLocked advances the follower's election-timeout counter
// by one tick. When electionElapsed >= electionTimeout the follower
// promotes to candidate (ELEC-01). Caller holds n.mu.
//
// Trigger dispatch: the n.onElectionTrigger hook is the W2-parallel
// extension point (05-02 ships nil-safe; 05-03 wires it to
// becomeCandidateLocked via wireElectionTriggerLocked at newNode time).
// Tests in 05-02 install a recorder hook directly to assert "the
// trigger fired on the Nth tick" without depending on
// becomeCandidateLocked existing.
func (n *node) tickFollowerLocked() {
	n.electionElapsed++
	n.lastHeartbeat++
	if n.electionElapsed >= n.electionTimeout {
		if n.onElectionTrigger != nil {
			n.onElectionTrigger()
		}
	}
}

// handleAppendEntriesLocked implements the Phase-5 half of Raft
// Figure-2 AppendEntries receiver behaviour: term handling + the
// "reset election timer FIRST" rule (Pitfall 3). The log-consistency
// + replication body is Phase-6 work; this method ships a Phase-6
// stand-in Success=true response so heartbeat invariants in tests do
// not flake.
//
// Pitfall 3 (CRITICAL): when m.Term == n.currentTerm the election
// timer MUST reset before any consistency check — even a rejected
// AppendEntries from the legitimate leader keeps the follower from
// timing out. stepLocked has already routed m.Term > n.currentTerm
// through maybeStepDownLocked, so by the time we get here either
// m.Term < n.currentTerm (stale) or m.Term == n.currentTerm (valid
// leader at the current term).
func (n *node) handleAppendEntriesLocked(m Message) {
	if m.Term < n.currentTerm {
		n.queueMsgLocked(Message{
			Type:    MsgAppendEntriesResp,
			Term:    n.currentTerm,
			From:    n.id,
			To:      m.From,
			Success: false,
		})
		return
	}
	// m.Term == n.currentTerm — Raft Figure 2 receiver step 1.
	// Reset BEFORE any further inspection. lastHeartbeat is a tick
	// counter (C-6) tracking ticks-since-last-heartbeat.
	n.electionElapsed = 0
	n.lastHeartbeat = 0
	n.leaderHint = m.From
	// Phase-6 stand-in: consistency check + log replication body
	// lands in pkg/raft/append_entries.go. For now reply success.
	n.queueMsgLocked(Message{
		Type:       MsgAppendEntriesResp,
		Term:       n.currentTerm,
		From:       n.id,
		To:         m.From,
		Success:    true,
		MatchIndex: n.log.LastIndex(),
	})
}

// isCandidateUpToDate implements Raft §5.4.1's log-completeness
// predicate. Pure function — 05-03's TestFigure7 will table-drive it
// without standing up a *node. Exported within the package so
// election_test.go can reach it.
//
// "Up-to-date" rule: if the logs' last terms differ, the higher term
// wins; if equal, the longer log wins (>= ensures a tie still grants).
func isCandidateUpToDate(candLastTerm Term, candLastIndex Index, voterLastTerm Term, voterLastIndex Index) bool {
	if candLastTerm != voterLastTerm {
		return candLastTerm > voterLastTerm
	}
	return candLastIndex >= voterLastIndex
}

// canGrantVote composes the three vote-granting conditions into a
// single predicate (Pitfall 9 — forgetting the votedFor clause is the
// classic split-vote bug). Caller holds n.mu.
//
//   - m.Term >= n.currentTerm (stepLocked has already promoted us to
//     m.Term via maybeStepDownLocked if it was strictly greater; this
//     check covers the stale-RequestVote case).
//   - votedFor is empty OR already points at this candidate (Raft §5.2
//     "first-come-first-served within a term").
//   - candidate's log is at least as up-to-date as ours (§5.4.1).
func (n *node) canGrantVote(m Message) bool {
	if m.Term < n.currentTerm {
		return false
	}
	if n.votedFor != "" && n.votedFor != m.From {
		return false
	}
	return isCandidateUpToDate(m.LastLogTerm, m.LastLogIndex, n.log.LastTerm(), n.log.LastIndex())
}

// handleRequestVoteLocked implements ELEC-05 (votedFor exclusivity) +
// ELEC-06 (log up-to-date) + the persist-first ordering (SC5).
// Caller holds n.mu.
//
// Ordering (SC5): when granting a vote we MUST queue the updated
// HardState BEFORE the response Message so 05-04's Ready drain can
// fsync (Term, VotedFor) before any RPC leaves the process. This
// method enforces queue order locally; 05-04 lands the driver-side
// ordering invariant and the raftdebug OrderingStorage assertion.
//
// Timer reset on grant: Raft §5.2 — a follower that has granted a
// vote in this term should not immediately initiate its own election.
// Resetting the election timeout here amortises the candidate's
// in-flight RPCs.
func (n *node) handleRequestVoteLocked(m Message) {
	grant := n.canGrantVote(m)
	if grant {
		n.votedFor = m.From
		n.resetElectionTimeoutLocked()
		n.queueHardStateLocked() // MUST queue BEFORE queueMsgLocked
	}
	n.queueMsgLocked(Message{
		Type:        MsgRequestVoteResponse,
		Term:        n.currentTerm,
		From:        n.id,
		To:          m.From,
		VoteGranted: grant,
	})
}
