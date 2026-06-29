package raft

import (
	"testing"
	"time"
)

// TestFigure7 — ELEC-09 / SC3. Six rows of Raft paper Figure 7 (p. 9).
// Candidate S1 at (term=6, lastLogIndex=10). Each row is one voter
// (a)..(f) with its own (lastTerm, lastIndex). Rows (c) and (d) are the
// load-bearing NO votes — they prove the election restriction blocks a
// candidate whose log is not at least as up-to-date as the voter's.
//
// The predicate isCandidateUpToDate is owned by 05-02 (follower.go);
// here we table-drive it directly without standing up a *node.
//
// Source: Raft paper Figure 7; Ongaro dissertation §3.6.1.
// Cross-checked: 05-RESEARCH.md §Pattern 5 table.
func TestFigure7(t *testing.T) {
	t.Parallel()
	const candLastTerm Term = 6
	const candLastIndex Index = 10

	rows := []struct {
		name           string
		voterLastTerm  Term
		voterLastIndex Index
		wantGrant      bool
	}{
		{"a", 6, 9, true},   // (6==6) && (10>=9)
		{"b", 4, 4, true},   // (6>4)
		{"c", 6, 11, false}, // (6==6) && (10<11) — load-bearing NO
		{"d", 7, 12, false}, // (6<7) — load-bearing NO
		{"e", 4, 7, true},   // (6>4)
		{"f", 3, 11, true},  // (6>3)
	}

	for _, row := range rows {
		t.Run("row_"+row.name, func(t *testing.T) {
			t.Parallel()
			got := isCandidateUpToDate(candLastTerm, candLastIndex, row.voterLastTerm, row.voterLastIndex)
			if got != row.wantGrant {
				t.Fatalf("Figure 7 row (%s): candLastTerm=%d candLastIndex=%d "+
					"voterLastTerm=%d voterLastIndex=%d: got grant=%v, want %v",
					row.name, candLastTerm, candLastIndex,
					row.voterLastTerm, row.voterLastIndex, got, row.wantGrant)
			}
		})
	}
}

// makeElectionConfig builds a Config with the default 150/300/50ms
// election window (LLD §2; RATIFIED decision 1 — see config.applyDefaults).
// tickInterval == HeartbeatInterval == 50ms, so the election window is
// 3..6 ticks (150/50 .. 300/50). Seed is caller-supplied so tests can
// pin the per-node RNG draw deterministically (P1-4).
func makeElectionConfig(t *testing.T, id NodeID, peers []NodeID, seed int64) *Config {
	t.Helper()
	return &Config{
		ID:                 id,
		Peers:              peers,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		Seed:               seed,
		Storage:            fakeStorage{},
	}
}

func mustNewNode(t *testing.T, cfg *Config) *node {
	t.Helper()
	n, err := newNode(cfg)
	if err != nil {
		t.Fatalf("newNode: %v", err)
	}
	return n
}

// TestElectionTimeout — SC1. A freshly-started follower with the
// default election window (150..300ms) and a 50ms HeartbeatInterval
// (== tickInterval) MUST promote to Candidate within [3, 6) ticks under
// MsgTick events.
//
// 3..6 ticks = ElectionTimeoutMin/tickInterval .. ElectionTimeoutMax/tickInterval.
// Seed=42 deterministically pins the draw inside that window.
func TestElectionTimeout(t *testing.T) {
	t.Parallel()
	cfg := makeElectionConfig(t, "n1", []NodeID{"n1", "n2", "n3"}, 42)
	n := mustNewNode(t, cfg)

	const minTicks = 3
	const maxTicks = 6
	transitioned := -1
	for i := range maxTicks + 2 { // small slack; should fire well before
		if err := n.Step(Message{Type: MsgTick}); err != nil {
			t.Fatalf("Step tick %d: %v", i, err)
		}
		if n.role == Candidate {
			transitioned = i + 1
			break
		}
	}
	if transitioned < minTicks || transitioned >= maxTicks+1 {
		t.Fatalf("election fired at tick %d; want within [%d, %d]", transitioned, minTicks, maxTicks)
	}
	if n.currentTerm != 1 {
		t.Fatalf("currentTerm=%d after election; want 1", n.currentTerm)
	}
	if !n.votesReceived[n.id] {
		t.Fatalf("self-vote missing after becomeCandidate")
	}
}

// TestCandidateSelfVoteAndQuorum — ELEC-04 + Pitfall 8.
// 3-node cluster: self-vote (1) + 1 grant from a peer = 2 = quorum ->
// Leader. The self-vote is the load-bearing detail: if becomeCandidate
// failed to seed votesReceived[self], we'd need 2 grants (impossible
// before the test even sends one) and the node would never reach
// quorum.
func TestCandidateSelfVoteAndQuorum(t *testing.T) {
	t.Parallel()
	cfg := makeElectionConfig(t, "n1", []NodeID{"n1", "n2", "n3"}, 42)
	n := mustNewNode(t, cfg)

	// Force an election via the timeout path so the full flow runs.
	n.mu.Lock()
	n.electionElapsed = n.electionTimeout
	n.mu.Unlock()
	if err := n.Step(Message{Type: MsgTick}); err != nil {
		t.Fatalf("Step tick: %v", err)
	}
	if n.role != Candidate {
		t.Fatalf("after election timeout: role=%v; want Candidate", n.role)
	}
	candTerm := n.currentTerm
	if !n.votesReceived[n.id] {
		t.Fatalf("votesReceived[self] missing — self-vote regression (Pitfall 8)")
	}
	if got, want := n.quorum(), 2; got != want {
		t.Fatalf("quorum: got %d, want %d", got, want)
	}

	// 1 grant from n2 -> votes = {n1, n2} = 2 = quorum -> Leader.
	if err := n.Step(Message{
		Type:        MsgRequestVoteResponse,
		Term:        candTerm,
		From:        "n2",
		To:          n.id,
		VoteGranted: true,
	}); err != nil {
		t.Fatalf("Step grant: %v", err)
	}
	if n.role != Leader {
		t.Fatalf("after 1 grant in 3-node cluster: role=%v; want Leader", n.role)
	}

	// becomeLeaderLocked must initialise nextIndex/matchIndex for every peer.
	wantNext := n.log.LastIndex() + 1
	for _, peer := range n.peers {
		if got := n.nextIndex[peer]; got != wantNext {
			t.Errorf("nextIndex[%s]: got %d, want %d", peer, got, wantNext)
		}
		if got := n.matchIndex[peer]; got != 0 {
			t.Errorf("matchIndex[%s]: got %d, want 0", peer, got)
		}
	}
	if n.leaderHint != n.id {
		t.Errorf("leaderHint: got %q, want %q", n.leaderHint, n.id)
	}
}

// TestCandidateTickRestartsElection — Pitfall 4 / Raft §5.2.
// A candidate that never reaches quorum (no responses arrive) MUST
// re-start the election (Term++, fresh self-vote, fresh fan-out) when
// its randomised election timeout expires again. Without this, a
// split-vote round leaves the cluster stuck.
func TestCandidateTickRestartsElection(t *testing.T) {
	t.Parallel()
	cfg := makeElectionConfig(t, "n1", []NodeID{"n1", "n2", "n3", "n4", "n5"}, 42)
	n := mustNewNode(t, cfg)

	// First election.
	n.mu.Lock()
	n.electionElapsed = n.electionTimeout
	n.mu.Unlock()
	if err := n.Step(Message{Type: MsgTick}); err != nil {
		t.Fatalf("Step tick: %v", err)
	}
	if n.role != Candidate {
		t.Fatalf("first election: role=%v; want Candidate", n.role)
	}
	firstTerm := n.currentTerm

	// No vote responses. Re-arm the timeout and tick once more.
	n.mu.Lock()
	n.electionElapsed = n.electionTimeout
	n.mu.Unlock()
	if err := n.Step(Message{Type: MsgTick}); err != nil {
		t.Fatalf("Step tick (retry): %v", err)
	}
	if n.currentTerm != firstTerm+1 {
		t.Fatalf("after split-vote retry: currentTerm=%d, want %d", n.currentTerm, firstTerm+1)
	}
	if !n.votesReceived[n.id] {
		t.Fatalf("self-vote missing after retry (votesReceived not re-seeded)")
	}
	if len(n.votesReceived) != 1 {
		t.Fatalf("retry should reset votesReceived to {self}; got %v", n.votesReceived)
	}
}

// TestCandidateIgnoresStaleResponses — handleRequestVoteResponseLocked
// stale-response guard. Grants for a prior term MUST NOT promote the
// candidate (the term-incremented retry already invalidated them).
func TestCandidateIgnoresStaleResponses(t *testing.T) {
	t.Parallel()
	cfg := makeElectionConfig(t, "n1", []NodeID{"n1", "n2", "n3"}, 42)
	n := mustNewNode(t, cfg)

	n.mu.Lock()
	n.electionElapsed = n.electionTimeout
	n.mu.Unlock()
	if err := n.Step(Message{Type: MsgTick}); err != nil {
		t.Fatalf("Step tick: %v", err)
	}
	if n.role != Candidate {
		t.Fatalf("setup: want Candidate, got %v", n.role)
	}
	candTerm := n.currentTerm

	// Stale-term grant — must be ignored.
	if err := n.Step(Message{
		Type:        MsgRequestVoteResponse,
		Term:        candTerm - 1,
		From:        "n2",
		To:          n.id,
		VoteGranted: true,
	}); err != nil {
		t.Fatalf("Step stale: %v", err)
	}
	if n.role == Leader {
		t.Fatalf("stale-term grant must be ignored; role=%v, want Candidate", n.role)
	}
	// votesReceived must still be just {self}.
	if _, ok := n.votesReceived["n2"]; ok {
		t.Fatalf("stale grant must not be counted in votesReceived")
	}
}

// TestCandidateFanOutQueuesRequestVotes proves ELEC-03: becomeCandidate
// fan-outs MsgRequestVote to every peer EXCEPT self, carrying the
// candidate's log tip in LastLogIndex/LastLogTerm. The HardState must
// be queued BEFORE the messages (SC5 persist-first ordering) — we
// verify by checking pendingHS is non-nil at fan-out time.
func TestCandidateFanOutQueuesRequestVotes(t *testing.T) {
	t.Parallel()
	cfg := makeElectionConfig(t, "n1", []NodeID{"n1", "n2", "n3", "n4", "n5"}, 42)
	n := mustNewNode(t, cfg)

	n.mu.Lock()
	n.electionElapsed = n.electionTimeout
	n.mu.Unlock()
	if err := n.Step(Message{Type: MsgTick}); err != nil {
		t.Fatalf("Step tick: %v", err)
	}
	if n.role != Candidate {
		t.Fatalf("role=%v; want Candidate", n.role)
	}

	// SC5: HardState (term + self-vote) MUST be queued.
	if n.pendingHS == nil {
		t.Fatalf("pendingHS nil; becomeCandidate must queue HardState before fan-out")
	}
	if n.pendingHS.CurrentTerm != n.currentTerm {
		t.Errorf("pendingHS.CurrentTerm: got %d, want %d", n.pendingHS.CurrentTerm, n.currentTerm)
	}
	if n.pendingHS.VotedFor != n.id {
		t.Errorf("pendingHS.VotedFor: got %q, want %q", n.pendingHS.VotedFor, n.id)
	}

	// Exactly len(peers)-1 outbound RequestVote messages, one per peer
	// except self.
	gotPeers := map[NodeID]int{}
	for _, pm := range n.pendingMsgs {
		if pm.msg.Type != MsgRequestVote {
			continue
		}
		gotPeers[pm.msg.To]++
		if pm.msg.From != n.id {
			t.Errorf("MsgRequestVote.From=%q; want %q", pm.msg.From, n.id)
		}
		if pm.msg.Term != n.currentTerm {
			t.Errorf("MsgRequestVote.Term=%d; want %d", pm.msg.Term, n.currentTerm)
		}
		if pm.msg.LastLogIndex != n.log.LastIndex() {
			t.Errorf("MsgRequestVote.LastLogIndex=%d; want %d", pm.msg.LastLogIndex, n.log.LastIndex())
		}
		if pm.msg.LastLogTerm != n.log.LastTerm() {
			t.Errorf("MsgRequestVote.LastLogTerm=%d; want %d", pm.msg.LastLogTerm, n.log.LastTerm())
		}
	}
	if _, self := gotPeers[n.id]; self {
		t.Errorf("MsgRequestVote sent to self; Pitfall 8 regression")
	}
	if got, want := len(gotPeers), len(n.peers)-1; got != want {
		t.Errorf("fan-out cardinality: got %d, want %d", got, want)
	}
}
