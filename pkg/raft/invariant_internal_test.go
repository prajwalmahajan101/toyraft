package raft

import (
	"testing"
)

// TestStepDownHaltsInFlight — SC4 / ELEC-08 / Pitfall 1.
//
// 07-04 RELOCATION: this test previously lived in invariant_test.go
// (package raft_test) and drove a raftest.Cluster to elect a leader, then
// reached the wrapped node via Cluster.NodeByID(id).Node() to call Step +
// Ready. The public raft.Node interface carries Step(ctx, msg) but NOT
// Ready() — Ready is the internal drain seam, and exposing it would grow the
// LLD golden. So the Step+Ready inspection is relocated HERE, into an
// internal `package raft` test that drives a real Leader *node via
// driveToLeader and calls n.Step / n.Ready directly. No exported surface is
// added (the plan's preferred option for the invariant_test.go Ready() site).
//
// The epoch-token mechanism halts in-flight messages on step-down even when
// the node is wired to a real Storage:
//
// Scenario:
//  1. Drive a node to Leader at term T (driveToLeader).
//  2. Inject MsgAppendEntries at term T+5 directly via n.Step. stepLocked
//     routes m.Term > currentTerm through maybeStepDownLocked which bumps
//     stepDownEpoch, demoting the Leader to Follower at term T+5 before
//     per-role dispatch.
//  3. Read n.Ready() — pendingHS MUST reflect the new term; pending messages
//     from the prior (Leader) epoch are dropped by the Ready() epoch filter.
//  4. Assert no leftover messages tagged with the stale term remain.
func TestStepDownHaltsInFlight(t *testing.T) {
	t.Parallel()
	peers := []NodeID{"n1", "n2", "n3"}
	n := driveToLeader(t, "n1", peers)

	// Capture the leader's term, then inject a higher-term AppendEntries.
	n.mu.Lock()
	leaderTerm := n.currentTerm
	n.mu.Unlock()

	intruderTerm := leaderTerm + 5
	if err := n.Step(Message{
		Type: MsgAppendEntries,
		Term: intruderTerm,
		From: "intruder",
		To:   "n1",
	}); err != nil {
		t.Fatalf("inject AppendEntries: %v", err)
	}

	msgs, hs := n.Ready()
	if hs == nil {
		t.Fatalf("after step-down: pendingHS is nil; want HardState{Term=%d}", intruderTerm)
	}
	if hs.CurrentTerm != intruderTerm {
		t.Fatalf("after step-down: HardState.CurrentTerm=%d; want %d", hs.CurrentTerm, intruderTerm)
	}

	// Every surviving message MUST carry the new term — the epoch-token
	// filter dropped anything queued under the prior (Leader) epoch. The new
	// term also frames the AppendEntries response built by
	// handleAppendEntriesLocked after the step-down.
	for _, m := range msgs {
		if m.Term < intruderTerm {
			t.Fatalf("post-step-down message at stale term %d: %+v (want >= %d)",
				m.Term, m, intruderTerm)
		}
	}
}
