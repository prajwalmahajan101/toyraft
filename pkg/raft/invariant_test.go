package raft_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/raftest"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// TestAtMostOneLeaderPerTerm_Chaos — SC6 / ELEC-10.
//
// 1000 seeds (100 under -short) × 200 ticks × 5-node cluster, with
// the full Hub chaos surface enabled (per-node drop, delay,
// reorder, duplicate). Invariant under test:
// AssertAtMostOneLeaderPerTerm never fires in any (seed, tick) cell.
//
// Package boundary note (deliverable mapping): the ROADMAP lists
// pkg/raft/invariant_test.go as the deliverable, but pkg/raft cannot
// import internal/raftest (cycle: internal/raftest imports pkg/raft).
// We satisfy the deliverable by declaring this file as
// `package raft_test` — an external test package — which is free to
// import internal/raftest. See 05-05-SUMMARY.md for the architectural
// note.
//
// Chaos knobs are sized from RESEARCH §"Code Examples / Example 4":
//   - DropRate("n00", 0.10): one node drops 10% of its outbound
//   - Delay(1ms, 5ms): every survivor is delayed in [1,5)ms
//   - Reorder(true, 8): per-receiver soft bucket of 8
//   - Duplicate(0.05): 5% of survivors deliver twice
func TestAtMostOneLeaderPerTerm_Chaos(t *testing.T) {
	maxSeed := int64(1000)
	if testing.Short() {
		maxSeed = 100
	}

	for seed := int64(1); seed <= maxSeed; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			c := raftest.NewCluster(t, 5, seed)
			c.Hub.DropRate("n00", 0.10)
			c.Hub.Delay(1*time.Millisecond, 5*time.Millisecond)
			c.Hub.Reorder(true, 8)
			c.Hub.Duplicate(0.05)

			for range 200 {
				c.Tick(10 * time.Millisecond)
				c.AssertAtMostOneLeaderPerTerm()
			}
		})
	}
}

// TestStepDownHaltsInFlight — SC4 / ELEC-08 / Pitfall 1 at the
// Cluster integration layer.
//
// Complements 05-04's pkg/raft unit test
// (TestReadyEpochFilterDropsStaleMessages) with end-to-end coverage:
// the epoch-token mechanism halts in-flight messages even when the
// state machine is wired to a real Hub + OrderingStorage.
//
// Scenario:
//  1. 3-node cluster; drive until a Leader emerges at term T.
//  2. Inject MsgAppendEntries at term T+5 directly into the Leader
//     via TestNode.Step (bypassing the Hub — the Hub would route it
//     normally on the next Tick, but we want a deterministic
//     same-call observation of the step-down).
//  3. Read Ready() — pendingHS MUST reflect the new term; pending
//     messages from the prior epoch are dropped by the Ready()
//     epoch filter.
//  4. Assert no leftover messages tagged with the stale term remain
//     in the Ready() output.
func TestStepDownHaltsInFlight(t *testing.T) {
	c := raftest.NewCluster(t, 3, 42)

	// Drive until a leader is elected.
	var leaderID raft.NodeID
	var leaderTerm raft.Term
	for range 100 {
		c.Tick(100 * time.Millisecond)
		if c.HasLeader() {
			leaderID, leaderTerm = c.Leader()
			break
		}
	}
	if leaderID == "" {
		t.Fatalf("no leader elected after 100 ticks")
	}

	leader := c.NodeByID(leaderID)
	if leader == nil {
		t.Fatalf("NodeByID(%q) returned nil", leaderID)
	}

	// Inject a higher-term AppendEntries directly. stepLocked routes
	// m.Term > currentTerm through maybeStepDownLocked which bumps
	// stepDownEpoch, demoting the Leader to Follower at term T+5
	// before per-role dispatch.
	intruderTerm := leaderTerm + 5
	err := leader.Node().Step(raft.Message{
		Type: raft.MsgAppendEntries,
		Term: intruderTerm,
		From: "intruder",
		To:   leaderID,
	})
	if err != nil {
		t.Fatalf("inject AppendEntries: %v", err)
	}

	msgs, hs := leader.Node().Ready()
	if hs == nil {
		t.Fatalf("after step-down: pendingHS is nil; want HardState{Term=%d}", intruderTerm)
	}
	if hs.CurrentTerm != intruderTerm {
		t.Fatalf("after step-down: HardState.CurrentTerm=%d; want %d", hs.CurrentTerm, intruderTerm)
	}

	// Every surviving message MUST carry the new term — the
	// epoch-token filter dropped anything queued under the prior
	// (Leader) epoch. The new term also frames the AppendEntries
	// response built by handleAppendEntriesLocked after the
	// step-down.
	for _, m := range msgs {
		if m.Term < intruderTerm {
			t.Fatalf("post-step-down message at stale term %d: %+v (want >= %d)",
				m.Term, m, intruderTerm)
		}
	}
}
