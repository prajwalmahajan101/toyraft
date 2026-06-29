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

// The AppendEntries receiver (handleAppendEntriesLocked) lives in
// pkg/raft/append_entries.go as of Phase 6: it grew from the Phase-5
// stand-in (term handling + the "reset election timer FIRST" rule,
// Pitfall 3) into the full Raft §5.3 Figure-2 receiver — log-matching
// consistency check (REPL-03), conflict truncation (REPL-07) with the
// truncate-above-commit safety guard (SC4/P0-3), follower commitIndex
// advance, and the Success/MatchIndex reply.

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
