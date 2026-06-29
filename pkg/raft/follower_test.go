package raft

import (
	"testing"
)

// findResponseTo scans pendingMsgs and returns the first Message whose
// To matches the target. Helper for vote/AE response assertions.
func findResponseTo(t *testing.T, n *node, target NodeID) Message {
	t.Helper()
	for _, pm := range n.pendingMsgs {
		if pm.msg.To == target {
			return pm.msg
		}
	}
	t.Fatalf("no pending response to %q (have %d pending msgs)", target, len(n.pendingMsgs))
	return Message{}
}

// TestFollowerHeartbeatResetsTimer pins Pitfall 3: a valid
// AppendEntries at the current term MUST reset electionElapsed and
// install leaderHint as the FIRST action.
func TestFollowerHeartbeatResetsTimer(t *testing.T) {
	t.Parallel()
	n := newStartedNode(t, []NodeID{"n1", "n2", "n3"})
	n.currentTerm = 4
	n.electionElapsed = 5
	n.leaderHint = ""

	if err := n.Step(Message{Type: MsgAppendEntries, Term: 4, From: "leader-x", To: "n1"}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.electionElapsed != 0 {
		t.Errorf("electionElapsed: got %d, want 0 (Pitfall 3 — reset on heartbeat)", n.electionElapsed)
	}
	if n.lastHeartbeat != 0 {
		t.Errorf("lastHeartbeat: got %d, want 0", n.lastHeartbeat)
	}
	if n.leaderHint != "leader-x" {
		t.Errorf("leaderHint: got %q, want %q", n.leaderHint, "leader-x")
	}
}

// TestFollowerRejectedAppendEntriesStillResetsTimerWhenTermMatches is
// the Pitfall 3 variant: even when the consistency check would fail
// (Phase 6 will exercise that path), the timer reset MUST happen
// first because Raft Figure 2 receiver step 1 runs before step 2. For
// Phase 5 our handler does not yet evaluate prevLogIndex, so this
// test currently asserts that any term-matching AE resets the timer —
// Phase 6 will extend it with explicit consistency-check failure
// arms once handleAppendEntriesLocked grows the log body.
func TestFollowerRejectedAppendEntriesStillResetsTimerWhenTermMatches(t *testing.T) {
	t.Parallel()
	n := newStartedNode(t, []NodeID{"n1", "n2", "n3"})
	n.currentTerm = 7
	n.electionElapsed = 9

	// An AE with bogus PrevLogIndex would (Phase 6) fail consistency.
	// The term-match still triggers the reset per Pitfall 3.
	if err := n.Step(Message{
		Type:         MsgAppendEntries,
		Term:         7,
		From:         "leader-y",
		To:           "n1",
		PrevLogIndex: 999,
		PrevLogTerm:  99,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.electionElapsed != 0 {
		t.Errorf("electionElapsed: got %d, want 0 (Pitfall 3 — reset before consistency check)", n.electionElapsed)
	}
}

// TestFollowerStaleAppendEntriesDoesNotResetTimer is the Pitfall 3
// inverse: a strictly-lower-term AE is a stale leader, MUST be
// rejected (Success=false), and MUST NOT reset the election timer.
func TestFollowerStaleAppendEntriesDoesNotResetTimer(t *testing.T) {
	t.Parallel()
	n := newStartedNode(t, []NodeID{"n1", "n2", "n3"})
	n.currentTerm = 7
	n.electionElapsed = 5

	if err := n.Step(Message{Type: MsgAppendEntries, Term: 3, From: "stale-leader", To: "n1"}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.electionElapsed != 5 {
		t.Errorf("electionElapsed: got %d, want 5 (stale AE must NOT reset timer)", n.electionElapsed)
	}
	resp := findResponseTo(t, n, "stale-leader")
	if resp.Success {
		t.Errorf("response.Success: got true, want false (stale-term rejection)")
	}
	if resp.Term != 7 {
		t.Errorf("response.Term: got %d, want 7 (responder's currentTerm)", resp.Term)
	}
}

// TestFollowerElectionTriggersOnTimeout locks ELEC-01: a follower
// whose electionElapsed crosses electionTimeout fires the trigger
// hook within the [Min, Max) tick range. Uses the onElectionTrigger
// hook to record without invoking the full becomeCandidateLocked
// promotion (which lives in 05-03 and is exercised by 05-03's tests).
func TestFollowerElectionTriggersOnTimeout(t *testing.T) {
	t.Parallel()
	n := newStartedNode(t, []NodeID{"n1", "n2", "n3"})
	fired := 0
	n.onElectionTrigger = func() { fired++ }
	n.electionTimeout = 3
	n.electionElapsed = 0

	for range 3 {
		if err := n.Step(Message{Type: MsgTick}); err != nil {
			t.Fatalf("Step(MsgTick): %v", err)
		}
	}
	if fired != 1 {
		t.Errorf("onElectionTrigger fired %d times after %d ticks, want 1", fired, 3)
	}
}

// TestFollowerVoteDeniedWhenAlreadyVoted pins ELEC-05's cross-candidate
// vote-doubling prevention (Pitfall 9). votedFor exclusivity holds
// even when the rival candidate's log is overwhelmingly up-to-date.
func TestFollowerVoteDeniedWhenAlreadyVoted(t *testing.T) {
	t.Parallel()
	n := newStartedNode(t, []NodeID{"n1", "node-a", "node-b"})
	n.currentTerm = 5
	n.votedFor = "node-a"

	if err := n.Step(Message{
		Type:         MsgRequestVote,
		Term:         5,
		From:         "node-b",
		To:           "n1",
		LastLogTerm:  100,
		LastLogIndex: 100,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	resp := findResponseTo(t, n, "node-b")
	if resp.VoteGranted {
		t.Errorf("VoteGranted: got true, want false (ELEC-05 votedFor exclusivity)")
	}
}

// TestFollowerVoteDeniedWhenLogStale pins ELEC-06 (§5.4.1): a vote is
// denied when the candidate's log is not at least as up-to-date as
// the voter's. Voter's last log term is 5; candidate's is 4 — denied.
func TestFollowerVoteDeniedWhenLogStale(t *testing.T) {
	t.Parallel()
	n := newStartedNode(t, []NodeID{"n1", "node-b", "node-c"})
	n.currentTerm = 5
	n.votedFor = ""
	n.log.Append(Entry{Term: 5, Index: 1})

	if err := n.Step(Message{
		Type:         MsgRequestVote,
		Term:         5,
		From:         "node-b",
		To:           "n1",
		LastLogTerm:  4,
		LastLogIndex: 100,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	resp := findResponseTo(t, n, "node-b")
	if resp.VoteGranted {
		t.Errorf("VoteGranted: got true, want false (ELEC-06 log staleness)")
	}
	if n.votedFor != "" {
		t.Errorf("votedFor: got %q, want empty (denied vote must not persist)", n.votedFor)
	}
}

// TestFollowerVoteGrantedQueuesHardStateBeforeResponse locks the SC5
// queueing order at the state-machine boundary: when granting a vote,
// pendingHS MUST be set (so 05-04's Ready drain persists it) AND the
// response Message MUST be present in pendingMsgs. The driver-side
// persist-before-send enforcement is 05-04's responsibility; here we
// only assert the state machine queued things in a way that lets the
// driver honour the invariant.
func TestFollowerVoteGrantedQueuesHardStateBeforeResponse(t *testing.T) {
	t.Parallel()
	n := newStartedNode(t, []NodeID{"n1", "node-b", "node-c"})
	n.currentTerm = 5
	n.votedFor = ""
	// Empty log on voter; candidate's (LastLogTerm=5, LastLogIndex=1) is
	// trivially up-to-date.

	if err := n.Step(Message{
		Type:         MsgRequestVote,
		Term:         5,
		From:         "node-b",
		To:           "n1",
		LastLogTerm:  5,
		LastLogIndex: 1,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.pendingHS == nil {
		t.Fatalf("pendingHS: got nil, want queued HardState before response")
	}
	if n.pendingHS.VotedFor != "node-b" {
		t.Errorf("pendingHS.VotedFor: got %q, want %q", n.pendingHS.VotedFor, "node-b")
	}
	if n.pendingHS.CurrentTerm != 5 {
		t.Errorf("pendingHS.CurrentTerm: got %d, want 5", n.pendingHS.CurrentTerm)
	}
	resp := findResponseTo(t, n, "node-b")
	if !resp.VoteGranted {
		t.Errorf("VoteGranted: got false, want true (up-to-date candidate, first vote)")
	}
	if n.votedFor != "node-b" {
		t.Errorf("votedFor: got %q, want %q", n.votedFor, "node-b")
	}
}

// TestFollowerVoteGrantedResetsElectionTimer locks the §5.2 wrinkle:
// granting a vote resets the election timeout so the voter does not
// immediately initiate its own election against the candidate it just
// supported.
func TestFollowerVoteGrantedResetsElectionTimer(t *testing.T) {
	t.Parallel()
	n := newStartedNode(t, []NodeID{"n1", "node-b", "node-c"})
	n.currentTerm = 5
	n.votedFor = ""
	n.electionElapsed = 9

	if err := n.Step(Message{
		Type:         MsgRequestVote,
		Term:         5,
		From:         "node-b",
		To:           "n1",
		LastLogTerm:  5,
		LastLogIndex: 1,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.electionElapsed != 0 {
		t.Errorf("electionElapsed: got %d, want 0 (§5.2 — vote grant resets timer)", n.electionElapsed)
	}
}

// TestIsCandidateUpToDate is the pure-function table 05-03's TestFigure7
// will piggyback on. Pin the predicate's table-shape here so 05-03
// can reuse without re-deriving the truth table.
func TestIsCandidateUpToDate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		candTerm, candIdx Term
		voterTerm         Term
		voterIdx          Index
		candIdx2          Index
		want              bool
	}{
		{"cand higher term wins regardless of index", 5, 0, 4, 999, 1, true},
		{"cand lower term loses regardless of index", 3, 0, 4, 0, 999, false},
		{"equal term, cand longer log grants", 5, 0, 5, 5, 10, true},
		{"equal term, equal length grants (tie)", 5, 0, 5, 5, 5, true},
		{"equal term, cand shorter log denies", 5, 0, 5, 10, 5, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isCandidateUpToDate(tc.candTerm, tc.candIdx2, tc.voterTerm, tc.voterIdx)
			if got != tc.want {
				t.Errorf("isCandidateUpToDate(candTerm=%d, candIdx=%d, voterTerm=%d, voterIdx=%d) = %v, want %v",
					tc.candTerm, tc.candIdx2, tc.voterTerm, tc.voterIdx, got, tc.want)
			}
		})
	}
}
