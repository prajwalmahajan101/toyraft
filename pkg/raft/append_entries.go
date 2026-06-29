package raft

import "fmt"

// handleAppendEntriesLocked implements the full Raft §5.3 Figure-2
// AppendEntries receiver. Caller holds n.mu.
//
// stepLocked has already routed m.Term > n.currentTerm through
// maybeStepDownLocked (term-first funnel, ELEC-07 / P0-5). So by the
// time we get here either m.Term < n.currentTerm (stale leader) or
// m.Term == n.currentTerm (legitimate leader at the current term). This
// method MUST NOT inline a term check that duplicates
// maybeStepDownLocked — doing so re-introduces the P0-5 TOCTOU.
//
// Figure-2 receiver steps:
//  1. m.Term < currentTerm -> reject (stale leader).
//  2. m.Term == currentTerm -> reset the election timer FIRST
//     (Pitfall 3): even a rejected AE from the legitimate leader keeps
//     the follower from timing out. Then install leaderHint.
//  3. REPL-03 log-matching consistency check via n.log.Match (handles
//     the (0,0) pre-log sentinel) -> reject on mismatch.
//  4. REPL-07 conflict resolution: find the first genuinely conflicting
//     index (firstConflictIndex), truncate from there, append the
//     suffix. Entries already present and matching are skipped so a
//     duplicated/stale re-delivery is idempotent (Pitfall 3 — never
//     blind-truncate to PrevLogIndex+1).
//  5. Advance commitIndex to min(LeaderCommit, index of last new entry).
//  6. Reply Success=true with MatchIndex = follower LastIndex.
//
// Fast-rollback hints (ConflictTerm/ConflictIndex, REPL-04) are
// deliberately NOT set here — the optimisation is deferred.
func (n *node) handleAppendEntriesLocked(m Message) {
	if m.Term < n.currentTerm {
		// Stale leader (Figure-2 receiver step 1) — reject and do NOT
		// reset the election timer.
		n.replyAppendEntriesLocked(m.From, false, 0)
		return
	}

	// m.Term == n.currentTerm. Reset BEFORE any consistency check
	// (Pitfall 3). lastHeartbeat is a tick counter (C-6).
	n.electionElapsed = 0
	n.lastHeartbeat = 0
	n.leaderHint = m.From

	// REPL-03 consistency check. Match() returns true for the (0,0)
	// sentinel (empty-log base case).
	if !n.log.Match(m.PrevLogIndex, m.PrevLogTerm) {
		n.replyAppendEntriesLocked(m.From, false, 0)
		return
	}

	// REPL-07 conflict resolution. firstConflictIndex returns:
	//   - 0 when every incoming entry is already present and matching
	//     (idempotent re-delivery — no truncate, no append),
	//   - the first index whose term disagrees with our log (genuine
	//     term conflict — truncate then append the suffix),
	//   - LastIndex()+1 for a clean extend past the end (pure append).
	if firstConflict := n.firstConflictIndex(m.Entries); firstConflict != 0 {
		// A pure append (firstConflict == LastIndex()+1) is not a
		// truncation and must not trip the SC4/P0-3 guard.
		if firstConflict <= n.log.LastIndex() {
			assertTruncateAboveCommit(firstConflict, n.commitIndex)
		}
		// Storage mirror added in 06-04 (P0-4 final).
		n.log.TruncateSuffix(firstConflict)
		n.log.Append(entriesFrom(m.Entries, firstConflict)...)
	}

	// Figure-2 receiver step 5: advance commitIndex. The last new entry
	// is PrevLogIndex + len(Entries); commitIndex never overruns it even
	// if the leader's commit is further ahead.
	if m.LeaderCommit > n.commitIndex {
		lastNew := m.PrevLogIndex + Index(len(m.Entries))
		n.commitIndex = min(m.LeaderCommit, lastNew)
		// commitIndex rides in HardState.Commit (types.go).
		n.queueHardStateLocked()
	}

	n.replyAppendEntriesLocked(m.From, true, n.log.LastIndex())
}

// replyAppendEntriesLocked queues an AppendEntries response. Caller
// holds n.mu. The response always carries the responder's currentTerm
// so a stale leader learns it must step down.
func (n *node) replyAppendEntriesLocked(to NodeID, success bool, matchIndex Index) {
	n.queueMsgLocked(Message{
		Type:       MsgAppendEntriesResp,
		Term:       n.currentTerm,
		From:       n.id,
		To:         to,
		Success:    success,
		MatchIndex: matchIndex,
	})
}

// firstConflictIndex walks the incoming (contiguous, Index-ordered)
// entries and returns the first index at which the follower must begin
// truncating/appending, or 0 if every entry is already present and
// matching. Caller holds n.mu (reads n.log).
//
// This is the idempotency-critical routine (RESEARCH Pattern 2): under
// the Hub's Duplicate/Reorder chaos a follower can receive the same or
// a stale AE twice. We MUST NOT blindly truncate to PrevLogIndex+1 and
// re-append — a delayed/duplicated AE carrying already-present entries
// would then truncate a matching (possibly committed) suffix, which is
// exactly the P0-3 failure mode. So we compare entry-by-entry and only
// report a conflict at the first genuine term mismatch.
//
// Returns:
//   - the first e.Index whose local term differs from e.Term (genuine
//     term conflict — truncate from here, append the suffix),
//   - the first e.Index that lies past LastIndex() (a clean extend —
//     nothing to truncate, append from here),
//   - 0 if the loop completes with every entry already present and
//     matching (no-op).
func (n *node) firstConflictIndex(entries []Entry) Index {
	last := n.log.LastIndex()
	for _, e := range entries {
		if e.Index > last {
			// New entry past the end of our log — clean extend point.
			return e.Index
		}
		// e.Index <= last: an overlapping index. Term() handles bounds
		// and the (idx==0) sentinel; e.Index is >= 1 here.
		localTerm, _ := n.log.Term(e.Index)
		if localTerm != e.Term {
			// First genuine term conflict.
			return e.Index
		}
		// Terms match — already present, skip (idempotent re-delivery).
	}
	// Every entry already present and matching.
	return 0
}

// entriesFrom returns the suffix of entries whose Index >= from. Entries
// are contiguous and Index-ordered, so this is the tail slice starting
// at the first qualifying entry. Used by handleAppendEntriesLocked to
// append only the post-conflict suffix after truncation.
func entriesFrom(entries []Entry, from Index) []Entry {
	for i, e := range entries {
		if e.Index >= from {
			return entries[i:]
		}
	}
	return nil
}

// assertTruncateAboveCommit enforces SC4 / P0-3: a follower MUST NEVER
// truncate its log at or below commitIndex, because a committed entry
// would be overwritten — the canonical Raft safety violation. A
// conflict at/below commitIndex means the consistency invariant has
// already been broken upstream (a leader sent entries conflicting with
// a committed prefix), so this is a bug elsewhere, not a recoverable
// condition.
//
// This is production code (not _test.go), so it panics rather than
// t.Fatal: the panic fires under -race -tags raftdebug stress and is
// impossible to silently ship. Callers MUST only invoke this for a real
// truncation (firstConflict != 0 AND firstConflict <= LastIndex()); a
// pure append (firstConflict == LastIndex()+1) is never a truncation
// and must not reach here.
func assertTruncateAboveCommit(firstConflict, commitIndex Index) {
	if firstConflict <= commitIndex {
		panic(fmt.Sprintf("raft/append_entries: truncate at/below commitIndex: "+
			"firstConflict=%d commitIndex=%d (P0-3: a committed entry would be overwritten)",
			firstConflict, commitIndex))
	}
}
