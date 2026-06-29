package raft

// leader.go holds the leader send-side of the replication subprotocol:
// the heartbeat/append fan-out (REPL-01) driven by tickLeaderLocked, and
// the per-peer AppendEntries builder sendAppendEntriesLocked. The leader
// response handler (handleAppendEntriesRespLocked) and the current-term
// commit rule land in plans 06-03 / commit.go; the client-proposal entry
// point proposeLocked lands in 06-01 Task 3 below.
//
// Concurrency: every method here is a *Locked method — the caller MUST
// hold n.mu (ADR-0004). No new sync.* primitive is introduced.

// tickLeaderLocked is the Leader MsgTick handler. Caller MUST hold n.mu.
//
// It increments the heartbeat-elapsed tick counter and, when it reaches
// heartbeatTimeout, resets it and fans out one AppendEntries per peer
// (empty Entries == heartbeat, REPL-01). There is NO separate heartbeat
// message type — heartbeats are AppendEntries with no entries (frozen
// MessageType 0..3, REPL-01).
//
// heartbeatTimeout is 1 by default (set in becomeLeaderLocked), and
// tickInterval()==HeartbeatInterval, so the heartbeat cadence equals the
// driver's tick cadence. Over one ElectionTimeoutMin window
// (ElectionTimeoutMin/tickInterval ticks == 3 at the 150/50 default) the
// leader emits >= 3 heartbeats per peer, satisfying the SC1 >= 3x ratio.
func (n *node) tickLeaderLocked() {
	n.heartbeatElapsed++
	if n.heartbeatElapsed < n.heartbeatTimeout {
		return
	}
	n.heartbeatElapsed = 0
	for _, peer := range n.peers {
		if peer == n.id {
			continue
		}
		n.sendAppendEntriesLocked(peer)
	}
}

// sendAppendEntriesLocked builds and queues one MsgAppendEntries for peer
// from that peer's nextIndex. Caller MUST hold n.mu.
//
// Per Raft Figure 2 (Leaders) / RESEARCH Pattern 1:
//   - PrevLogIndex = nextIndex[peer]-1; PrevLogTerm = log.Term(PrevLogIndex)
//     (Term(0) returns the (0,nil) pre-log sentinel — log.go).
//   - Entries = a DEEP COPY of log[nextIndex[peer]..LastIndex()]; empty when
//     nextIndex[peer] > LastIndex() (that case is a pure heartbeat).
//   - LeaderCommit = commitIndex.
//
// The entries slice is deep-copied (entriesFrom) so the outbound Message
// never aliases the live in-memory log backing array — required by
// docs/CONCURRENCY.md §4 (Entries are shared by reference in-process; a
// consumer mutating an aliased slice would corrupt the leader's log).
func (n *node) sendAppendEntriesLocked(peer NodeID) {
	prevIndex := n.nextIndex[peer] - 1
	prevTerm, _ := n.log.Term(prevIndex)
	entries := n.log.entriesFrom(n.nextIndex[peer])
	n.queueMsgLocked(Message{
		Type:         MsgAppendEntries,
		Term:         n.currentTerm,
		From:         n.id,
		To:           peer,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	})
}

// proposeLocked appends a client proposal to the leader's log and reports
// the assigned index. Caller MUST hold n.mu. (REPL-02, RESEARCH Pattern 4.)
//
// Returns (0, false) when not the leader — only a leader may originate an
// entry (Raft §5.3). On success it appends Entry{currentTerm, lastIndex+1,
// data}, marks the leader's own match as fully replicated locally
// (matchIndex[self] = idx), and returns (idx, true). The next
// tickLeaderLocked / response cycle replicates the entry to followers.
//
// matchIndex[self] is set so the commit-rule snapshot (06-03) counts the
// leader's own copy without special-casing self.
//
// RATIFIED decision 2 (P0-4 final / REPL-09): the proposed entry is
// mirrored into the durable local Storage subset BEFORE the leader marks
// it locally replicated (matchIndex[self]=idx) and BEFORE the next
// tickLeaderLocked can fan out an AppendEntries that carries it. n.log
// stays the fast in-memory read path; n.storage is the durable mirror.
// If the mirror fails the entry is NOT advertised as replicated — we roll
// the in-memory append back and return (0,false) rather than silently
// shipping durability we never achieved (global rule: no failure-hiding
// fallback). The pkg/raft.Storage vs pkg/storage.Storage duplication is
// deliberately NOT reconciled here (Phase 7 ADR).
func (n *node) proposeLocked(data []byte) (Index, bool) {
	if n.role != Leader {
		return 0, false
	}
	idx := n.log.LastIndex() + 1
	entry := Entry{Term: n.currentTerm, Index: idx, Data: data}
	n.log.Append(entry)
	if err := n.storage.Append([]Entry{entry}); err != nil {
		// Durable mirror failed: undo the in-memory append so n.log and
		// n.storage stay in lockstep, log, and report failure. The caller
		// MUST NOT treat the entry as accepted.
		n.log.TruncateSuffix(idx)
		n.log2.Error("raft: proposeLocked storage mirror failed",
			"index", idx, "term", n.currentTerm, "err", err)
		return 0, false
	}
	n.matchIndex[n.id] = idx // leader's own entry is locally replicated
	return idx, true
}

// handleAppendEntriesRespLocked processes a follower's AppendEntries
// response: it advances per-peer progress on success (then runs the
// commit rule) or slow-probes nextIndex down on failure (REPL-04).
// Caller MUST hold n.mu. (Raft Figure 2 Leaders + RESEARCH Pattern 3.)
//
// Stale-response guard (mirrors handleRequestVoteResponseLocked,
// candidate.go:74): a late response from a prior term, or one arriving
// after we have stepped down, MUST NOT mutate matchIndex/nextIndex —
// otherwise a stale MatchIndex would corrupt the commit snapshot
// (RESEARCH anti-pattern "resetting matchIndex from a stale response").
// stepLocked already routed m.Term > currentTerm through
// maybeStepDownLocked (term-first funnel, ELEC-07 / P0-5), so a
// higher-term response has already demoted us before we get here; we do
// NOT inline a second term-step-down (that re-introduces the P0-5 TOCTOU).
//
// On success: matchIndex[peer]=resp.MatchIndex, nextIndex[peer]=that+1,
// then maybeAdvanceCommitLocked() (commit.go) may bump commitIndex.
//
// On failure (REPL-04 slow probe): decrement nextIndex[peer] (floored at
// 1) so the next tickLeaderLocked re-probes from a lower point. The
// fast-rollback / ConflictTerm back-jump is deliberately NOT implemented
// (deferred, REPL-04).
func (n *node) handleAppendEntriesRespLocked(m Message) {
	if n.role != Leader || m.Term != n.currentTerm {
		return
	}
	if m.Success {
		n.matchIndex[m.From] = m.MatchIndex
		n.nextIndex[m.From] = m.MatchIndex + 1
		n.maybeAdvanceCommitLocked() // commit.go
		return
	}
	// REPL-04 slow probe: decrement and re-probe next tick. Floor at 1.
	if n.nextIndex[m.From] > 1 {
		n.nextIndex[m.From]--
	}
}

// entriesFrom returns a DEEP COPY of the log entries with Index >= lo.
//
// Returns nil when lo > LastIndex() (the heartbeat case — no entries to
// ship). The copy defeats the aliasing hazard called out in
// docs/CONCURRENCY.md §4: the returned slice is owned by the caller and
// safe to hand to an outbound Message even after subsequent log mutation.
func (l *Log) entriesFrom(lo Index) []Entry {
	if lo == 0 || lo > l.LastIndex() {
		return nil
	}
	// lo is 1-based; convert to a 0-based slice offset.
	src := l.entries[lo-1:]
	out := make([]Entry, len(src))
	copy(out, src)
	return out
}
