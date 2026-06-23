package raft

import (
	"testing"
)

// TestReadyReturnsAndClears: queue two messages and a HardState via the
// internal helpers, then drain. The first Ready() returns both messages
// and the HardState; the second returns an empty slice and nil. This
// pins the drain-clear semantics (LLD §5 Ready contract).
func TestReadyReturnsAndClears(t *testing.T) {
	t.Parallel()
	n := newStartedNode(t, []NodeID{"n1", "n2", "n3"})

	n.mu.Lock()
	n.queueMsgLocked(Message{Type: MsgRequestVote, Term: 1, From: "n1", To: "n2"})
	n.queueMsgLocked(Message{Type: MsgRequestVote, Term: 1, From: "n1", To: "n3"})
	n.queueHardStateLocked()
	n.mu.Unlock()

	msgs, hs := n.Ready()
	if len(msgs) != 2 {
		t.Fatalf("first Ready: len(msgs)=%d, want 2", len(msgs))
	}
	if hs == nil {
		t.Fatalf("first Ready: hs nil, want non-nil")
	}

	msgs2, hs2 := n.Ready()
	if len(msgs2) != 0 {
		t.Errorf("second Ready: len(msgs)=%d, want 0", len(msgs2))
	}
	if hs2 != nil {
		t.Errorf("second Ready: hs=%+v, want nil", hs2)
	}
}

// TestReadyEpochFilterDropsStaleMessages — SC4 / ELEC-08 / PITFALLS P0-5.
//
// Scenario: a candidate has fanned out RequestVote messages under
// stepDownEpoch=E. Before the driver gets a chance to call Ready(), an
// AppendEntries arrives at a strictly higher term. maybeStepDownLocked
// bumps stepDownEpoch to E+1 and clears candidate state. The follower-
// role AppendEntries handler queues a response under epoch E+1.
//
// Ready() MUST drop the prior-epoch RequestVote messages on the floor
// and return only the new-epoch response. If the filter regresses, the
// step-down is not TOCTOU-free: a stale RequestVote could be shipped
// under the new term and corrupt the §5.2 vote contract.
func TestReadyEpochFilterDropsStaleMessages(t *testing.T) {
	t.Parallel()
	cfg := makeElectionConfig(t, "n1", []NodeID{"n1", "n2", "n3"}, 42)
	n := mustNewNode(t, cfg)

	// Force the candidate fan-out via the election-timeout path.
	n.mu.Lock()
	n.electionElapsed = n.electionTimeout
	startingEpoch := n.stepDownEpoch
	n.mu.Unlock()
	if err := n.Step(Message{Type: MsgTick}); err != nil {
		t.Fatalf("Step tick: %v", err)
	}
	if n.role != Candidate {
		t.Fatalf("role=%v; want Candidate", n.role)
	}
	candTerm := n.currentTerm

	// Sanity: the fan-out queued two RequestVote messages at the
	// candidate's epoch (peers are n1/n2/n3 minus self = 2 peers).
	if got := len(n.pendingMsgs); got != 2 {
		t.Fatalf("pre-step-down: len(pendingMsgs)=%d, want 2", got)
	}
	for i, pm := range n.pendingMsgs {
		if pm.epoch != n.stepDownEpoch {
			t.Errorf("pendingMsgs[%d].epoch=%d, want %d", i, pm.epoch, n.stepDownEpoch)
		}
		if pm.msg.Type != MsgRequestVote {
			t.Errorf("pendingMsgs[%d].Type=%v, want MsgRequestVote", i, pm.msg.Type)
		}
	}

	// Step a higher-term AppendEntries — maybeStepDownLocked will bump
	// the epoch and clear candidate state; the AE handler then queues
	// an AppendEntriesResp under the NEW epoch.
	higherTerm := candTerm + 5
	if err := n.Step(Message{
		Type: MsgAppendEntries,
		Term: higherTerm,
		From: "leader-x",
		To:   n.id,
	}); err != nil {
		t.Fatalf("Step higher-term AE: %v", err)
	}
	if n.role != Follower {
		t.Fatalf("post-step-down: role=%v, want Follower", n.role)
	}
	if n.stepDownEpoch != startingEpoch+1 {
		t.Fatalf("stepDownEpoch=%d, want %d (single bump)", n.stepDownEpoch, startingEpoch+1)
	}

	// Drain. The two stale-epoch RequestVote messages MUST be filtered
	// out; only the new-epoch AppendEntriesResp survives.
	msgs, hs := n.Ready()
	if hs == nil {
		t.Fatalf("Ready: hs nil; maybeStepDownLocked queues a HardState (new term, cleared vote)")
	}
	if hs.CurrentTerm != higherTerm {
		t.Errorf("hs.CurrentTerm=%d, want %d", hs.CurrentTerm, higherTerm)
	}
	if hs.VotedFor != "" {
		t.Errorf("hs.VotedFor=%q, want empty after step-down", hs.VotedFor)
	}
	if len(msgs) != 1 {
		t.Fatalf("Ready: len(msgs)=%d, want 1 (stale RequestVotes must be filtered)", len(msgs))
	}
	if msgs[0].Type != MsgAppendEntriesResp {
		t.Errorf("Ready: msgs[0].Type=%v, want MsgAppendEntriesResp", msgs[0].Type)
	}
	if msgs[0].Term != higherTerm {
		t.Errorf("Ready: msgs[0].Term=%d, want %d", msgs[0].Term, higherTerm)
	}
}

// TestReadyReturnsHardStateWithVoteResponse — SC5 layer-2 atomicity.
// Granting a vote queues the HardState and the response in the same
// stepLocked call; Ready() MUST return both together so the driver can
// fsync before shipping.
func TestReadyReturnsHardStateWithVoteResponse(t *testing.T) {
	t.Parallel()
	cfg := makeElectionConfig(t, "n1", []NodeID{"n1", "n2", "n3"}, 42)
	n := mustNewNode(t, cfg)

	// A valid RequestVote at term 1 from n2 with an up-to-date log.
	if err := n.Step(Message{
		Type:         MsgRequestVote,
		Term:         1,
		From:         "n2",
		To:           n.id,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}); err != nil {
		t.Fatalf("Step RequestVote: %v", err)
	}

	msgs, hs := n.Ready()
	if hs == nil {
		t.Fatalf("Ready: hs nil; vote grant must queue HardState before response (SC5)")
	}
	if hs.CurrentTerm != 1 {
		t.Errorf("hs.CurrentTerm=%d, want 1", hs.CurrentTerm)
	}
	if hs.VotedFor != "n2" {
		t.Errorf("hs.VotedFor=%q, want %q", hs.VotedFor, NodeID("n2"))
	}
	if len(msgs) != 1 {
		t.Fatalf("Ready: len(msgs)=%d, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Type != MsgRequestVoteResponse {
		t.Errorf("msgs[0].Type=%v, want MsgRequestVoteResponse", m.Type)
	}
	if !m.VoteGranted {
		t.Errorf("msgs[0].VoteGranted=false, want true")
	}
	if m.To != "n2" {
		t.Errorf("msgs[0].To=%q, want %q", m.To, NodeID("n2"))
	}
	if m.Term != hs.CurrentTerm {
		t.Errorf("msgs[0].Term=%d, hs.CurrentTerm=%d; must match", m.Term, hs.CurrentTerm)
	}
}

// TestReadyCallableWithoutMu confirms Ready() acquires n.mu internally.
// If Ready() ever required the caller to hold n.mu, this test would
// deadlock under -race when the goroutine inside attempts a re-lock.
// We exercise the "called without holding mu" path explicitly.
func TestReadyCallableWithoutMu(t *testing.T) {
	t.Parallel()
	n := newStartedNode(t, []NodeID{"n1", "n2", "n3"})
	// No n.mu.Lock() here.
	msgs, hs := n.Ready()
	if msgs == nil {
		t.Errorf("Ready: msgs nil, want empty slice")
	}
	if len(msgs) != 0 {
		t.Errorf("Ready: len(msgs)=%d, want 0", len(msgs))
	}
	if hs != nil {
		t.Errorf("Ready: hs=%+v, want nil", hs)
	}
}
