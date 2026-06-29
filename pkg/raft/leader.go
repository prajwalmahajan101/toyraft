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
