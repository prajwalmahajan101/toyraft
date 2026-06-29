package raft_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/raftest"
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
	// Default to 100 seeds so CI stays within the 5min per-package
	// timeout on slower runners (macOS hits ~5min at 1000 seeds).
	// Full SC6 coverage (1000 seeds) is gated behind
	// TOYRAFT_CHAOS_FULL=1 for nightly / pre-merge sweeps.
	maxSeed := int64(100)
	if !testing.Short() && os.Getenv("TOYRAFT_CHAOS_FULL") == "1" {
		maxSeed = 1000
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

// TestNoLogDivergence_Chaos — SC7 / REPL-11 (with REPL-10 spirit).
//
// Clones the shape of TestAtMostOneLeaderPerTerm_Chaos above (RESEARCH
// Example 3): the same 100-seed (1000 under TOYRAFT_CHAOS_FULL) × 200-tick
// × 5-node sweep with the full Hub chaos surface (per-node drop, delay,
// reorder, duplicate). The invariants under test are the cluster-level
// safety helpers added in 06-05:
//
//   - AssertLogMatching (REPL-11): if two logs share an entry at the same
//     (index, term), all preceding entries are byte-identical.
//   - AssertNoCommittedEntryLost (REPL-10 spirit): once an index is
//     reported committed, no node ever changes the entry at that index.
//
// Unlike the leader-only election chaos suite, this test injects REAL log
// content: whenever a leader exists (guarded by c.HasLeader), it proposes
// an entry via c.ProposeToLeader so there is something to diverge. Without
// proposals the logs stay empty and the log-matching invariant is
// vacuous. The invariants are checked every tick so the earliest seed/tick
// that breaks safety surfaces immediately (the Fatalf messages carry the
// seed for bisecting).
//
// Gating matches the existing chaos suite exactly: 100 seeds by default
// (CI budget), 1000 only when !testing.Short() && TOYRAFT_CHAOS_FULL=1.
func TestNoLogDivergence_Chaos(t *testing.T) {
	maxSeed := int64(100)
	if !testing.Short() && os.Getenv("TOYRAFT_CHAOS_FULL") == "1" {
		maxSeed = 1000
	}

	for seed := int64(1); seed <= maxSeed; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			c := raftest.NewCluster(t, 5, seed)
			c.Hub.DropRate("n00", 0.10)
			c.Hub.Delay(1*time.Millisecond, 5*time.Millisecond)
			c.Hub.Reorder(true, 8)
			c.Hub.Duplicate(0.05)

			for tick := range 200 {
				c.Tick(10 * time.Millisecond)
				// Inject real log content periodically so the log-matching
				// invariant is non-vacuous. Only propose when a leader
				// exists (a no-leader propose would be a no-op anyway, but
				// the guard keeps intent explicit). Every few ticks keeps
				// the suite fast while still generating churn-worthy logs.
				if tick%5 == 0 && c.HasLeader() {
					c.ProposeToLeader([]byte(fmt.Sprintf("op-%d-%d", seed, tick)))
				}
				c.AssertLogMatching()
				c.AssertNoCommittedEntryLost()
			}
		})
	}
}

// TestStepDownHaltsInFlight — SC4 / ELEC-08 / Pitfall 1 — RELOCATED in 07-04
// to invariant_internal_test.go (package raft). It needs n.Ready(), which is
// the internal drain seam and NOT on the public raft.Node interface; exposing
// it would grow the LLD golden. The relocated internal test drives a real
// Leader *node via driveToLeader and calls n.Step / n.Ready directly,
// preserving the epoch-filter assertion byte-for-byte in intent without any
// new exported surface.
