package raft

import (
	"testing"
)

// seedFollowerLog stands up a started follower at the given term with a
// pre-seeded log. Entries are taken verbatim (caller supplies valid
// 1-based contiguous, term-monotonic entries — Log.Append asserts this
// under -tags raftdebug). Returns the *node for white-box driving.
func seedFollowerLog(t *testing.T, term Term, entries ...Entry) *node {
	t.Helper()
	n := newStartedNode(t, []NodeID{"n1", "leader", "follower2"})
	n.currentTerm = term
	n.role = Follower
	n.log.Append(entries...)
	return n
}

// TestLogMatching is SC2 / REPL-03: the follower accepts an AppendEntries
// exactly when Log.Match(prevLogIndex, prevLogTerm) holds and rejects
// otherwise, including the (0,0) empty-log base case. On accept the
// response MatchIndex equals the follower's LastIndex.
func TestLogMatching(t *testing.T) {
	t.Parallel()
	// Seeded follower log: indexes 1,2,3 with terms 1,1,2.
	seed := []Entry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2},
		{Term: 2, Index: 3},
	}
	tests := []struct {
		name         string
		prevLogIndex Index
		prevLogTerm  Term
		wantSuccess  bool
	}{
		{"empty-log sentinel base case", 0, 0, true},
		{"match at index 1", 1, 1, true},
		{"match at index 2", 2, 1, true},
		{"match at tail index 3", 3, 2, true},
		{"term mismatch at index 3", 3, 1, false},
		{"term mismatch at index 1", 1, 2, false},
		{"prevLogIndex past end", 4, 2, false},
		{"prevLogIndex far past end", 99, 9, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n := seedFollowerLog(t, 5, seed...)
			// Heartbeat-shaped AE (no entries) — isolates the
			// consistency check from the truncate/append path.
			if err := n.Step(Message{
				Type:         MsgAppendEntries,
				Term:         5,
				From:         "leader",
				To:           "n1",
				PrevLogIndex: tc.prevLogIndex,
				PrevLogTerm:  tc.prevLogTerm,
			}); err != nil {
				t.Fatalf("Step: %v", err)
			}
			resp := findResponseTo(t, n, "leader")
			if resp.Success != tc.wantSuccess {
				t.Fatalf("Success: got %v, want %v (Match(%d,%d))",
					resp.Success, tc.wantSuccess, tc.prevLogIndex, tc.prevLogTerm)
			}
			if tc.wantSuccess {
				if resp.MatchIndex != n.log.LastIndex() {
					t.Errorf("MatchIndex on accept: got %d, want %d (follower LastIndex)",
						resp.MatchIndex, n.log.LastIndex())
				}
			} else if resp.MatchIndex != 0 {
				t.Errorf("MatchIndex on reject: got %d, want 0", resp.MatchIndex)
			}
		})
	}
}

// TestAppendEntriesTruncatesConflict is REPL-07: a follower with
// [t1,t1,t2] receives an AE whose entry at index 3 diverges in term.
// The follower MUST truncate from index 3 and append the leader's
// suffix so its log converges to the leader's.
func TestAppendEntriesTruncatesConflict(t *testing.T) {
	t.Parallel()
	n := seedFollowerLog(t, 5,
		Entry{Term: 1, Index: 1},
		Entry{Term: 1, Index: 2},
		Entry{Term: 2, Index: 3}, // will be overwritten (term 2 -> term 3)
	)

	// Leader's suffix diverges at index 3 (term 3) and extends to 4.
	leaderEntries := []Entry{
		{Term: 3, Index: 3},
		{Term: 3, Index: 4},
	}
	if err := n.Step(Message{
		Type:         MsgAppendEntries,
		Term:         5,
		From:         "leader",
		To:           "n1",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      leaderEntries,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}

	resp := findResponseTo(t, n, "leader")
	if !resp.Success {
		t.Fatalf("Success: got false, want true (matching prev, conflict resolved)")
	}
	// Final log must be index1(t1), index2(t1), index3(t3), index4(t3).
	want := []Entry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2},
		{Term: 3, Index: 3},
		{Term: 3, Index: 4},
	}
	if n.log.LastIndex() != 4 {
		t.Fatalf("LastIndex after conflict resolve: got %d, want 4", n.log.LastIndex())
	}
	for _, w := range want {
		gotTerm, err := n.log.Term(w.Index)
		if err != nil {
			t.Fatalf("Term(%d): %v", w.Index, err)
		}
		if gotTerm != w.Term {
			t.Errorf("log[%d].Term: got %d, want %d (truncate+append diverged at index 3)",
				w.Index, gotTerm, w.Term)
		}
	}
	if resp.MatchIndex != 4 {
		t.Errorf("MatchIndex: got %d, want 4", resp.MatchIndex)
	}
}

// TestAppendEntriesIdempotent is Pitfall 3: delivering the SAME matching
// AE twice MUST NOT truncate the matching suffix, MUST NOT regress
// commitIndex, and MUST reply Success both times. This is the chaos
// re-delivery / duplicate guarantee.
func TestAppendEntriesIdempotent(t *testing.T) {
	t.Parallel()
	n := seedFollowerLog(t, 5,
		Entry{Term: 1, Index: 1},
		Entry{Term: 1, Index: 2},
	)

	ae := Message{
		Type:         MsgAppendEntries,
		Term:         5,
		From:         "leader",
		To:           "n1",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries: []Entry{
			{Term: 2, Index: 3},
			{Term: 2, Index: 4},
		},
		LeaderCommit: 3,
	}

	// First delivery — appends 3,4 and advances commitIndex to 3.
	if err := n.Step(ae); err != nil {
		t.Fatalf("Step (first): %v", err)
	}
	resp1 := findResponseTo(t, n, "leader")
	if !resp1.Success {
		t.Fatalf("first delivery Success: got false, want true")
	}
	lastAfterFirst := n.log.LastIndex()
	commitAfterFirst := n.commitIndex
	if lastAfterFirst != 4 {
		t.Fatalf("LastIndex after first: got %d, want 4", lastAfterFirst)
	}
	if commitAfterFirst != 3 {
		t.Fatalf("commitIndex after first: got %d, want 3", commitAfterFirst)
	}

	// Capture the actual entries so we can prove the suffix is untouched.
	termsBefore := make([]Term, 4)
	for i := Index(1); i <= 4; i++ {
		termsBefore[i-1], _ = n.log.Term(i)
	}

	// Second delivery of the identical AE — must be a pure no-op on the
	// log (firstConflictIndex returns 0) and must not regress commit.
	n.pendingMsgs = nil // clear so we read the second response cleanly
	if err := n.Step(ae); err != nil {
		t.Fatalf("Step (second): %v", err)
	}
	resp2 := findResponseTo(t, n, "leader")
	if !resp2.Success {
		t.Fatalf("second delivery Success: got false, want true (idempotent re-delivery)")
	}
	if n.log.LastIndex() != lastAfterFirst {
		t.Errorf("LastIndex after redelivery: got %d, want %d (no suffix change)",
			n.log.LastIndex(), lastAfterFirst)
	}
	if n.commitIndex < commitAfterFirst {
		t.Errorf("commitIndex regressed: got %d, want >= %d", n.commitIndex, commitAfterFirst)
	}
	for i := Index(1); i <= 4; i++ {
		got, _ := n.log.Term(i)
		if got != termsBefore[i-1] {
			t.Errorf("log[%d].Term mutated on redelivery: got %d, want %d", i, got, termsBefore[i-1])
		}
	}
}

// TestTruncateAboveCommitGuard is SC4 / P0-3: a follower whose
// commitIndex has advanced to 2 receives a (malformed/impossible) AE
// that would force a conflict truncation at index <= commitIndex. The
// assertTruncateAboveCommit guard MUST panic — overwriting a committed
// entry is the canonical Raft safety violation, so the guard fires as a
// safety-bug canary rather than silently corrupting the log.
func TestTruncateAboveCommitGuard(t *testing.T) {
	t.Parallel()
	n := seedFollowerLog(t, 5,
		Entry{Term: 1, Index: 1},
		Entry{Term: 2, Index: 2},
		Entry{Term: 2, Index: 3},
	)
	// Mark indexes 1..2 committed.
	n.commitIndex = 2

	// An AE that matches at prevLogIndex=1 but whose entry at index 2
	// carries a conflicting term (3 != local 2). firstConflictIndex
	// returns 2; since 2 <= commitIndex the guard must panic.
	ae := Message{
		Type:         MsgAppendEntries,
		Term:         5,
		From:         "leader",
		To:           "n1",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []Entry{
			{Term: 3, Index: 2}, // conflicts with committed index 2
			{Term: 3, Index: 3},
		},
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic from assertTruncateAboveCommit (truncate at/below commitIndex), got none")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("panic value: got %T (%v), want string", r, r)
		}
		if want := "commitIndex"; !contains(msg, want) {
			t.Errorf("panic message %q does not mention %q (P0-3 guard)", msg, want)
		}
	}()

	// Drive the receiver directly under the lock the production path
	// would hold; Step would recover nothing, so we call the locked
	// handler to let the panic propagate to our deferred recover.
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handleAppendEntriesLocked(ae)
}

// contains is a tiny substring check (avoids importing strings just for
// the guard-message assertion).
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
