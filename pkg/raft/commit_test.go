package raft

import (
	"testing"
)

// commit_test.go is in-package (direct *node access) so the commit rule
// and the response handler's slow-probe can be exercised at the unit
// level — independent of the end-to-end raftest.Cluster path. It covers
// REPL-06 (current-term commit rule / Figure-8 fix), SC6 / REPL-08
// (sorted-quorum determinism), P1-2 (fresh leader no-advance), and
// REPL-04 (slow-probe nextIndex rewind, floored at 1).

// newLeaderForCommit builds a node, drives it to Leader over `peers`, and
// resets its log so a test can script an exact matchIndex / log state. It
// returns the *node with role=Leader; the caller sets currentTerm, log,
// and matchIndex directly under n.mu. The seeded log is cleared so the
// election's bookkeeping does not perturb index math.
func newLeaderForCommit(t *testing.T, id NodeID, peers []NodeID) *node {
	t.Helper()
	n, _ := driveToLeader(t, id, peers)
	n.mu.Lock()
	defer n.mu.Unlock()
	// Clear any election-era log + commit; tests script log/match from scratch.
	n.log.TruncateSuffix(0)
	n.commitIndex = 0
	return n
}

// seedLog appends entries [term...] at indices 1..len under n.mu. Each
// element of terms is the Term for that 1-based index. Caller passes
// non-decreasing terms (the log's raftdebug invariant requires it).
func seedLog(n *node, terms []Term) {
	for i, tm := range terms {
		n.log.Append(Entry{Term: tm, Index: Index(i + 1)})
	}
}

// TestCommitCurrentTermRule — REPL-06. A leader whose top log entries are
// current-term advances commitIndex to the largest current-term index
// that reaches quorum.
func TestCommitCurrentTermRule(t *testing.T) {
	t.Parallel()
	peers := []NodeID{"n1", "n2", "n3"}
	n := newLeaderForCommit(t, "n1", peers)

	n.mu.Lock()
	n.currentTerm = 5
	// Three current-term (5) entries at indices 1,2,3.
	seedLog(n, []Term{5, 5, 5})
	// self has everything (substituted to LastIndex=3 in the snapshot);
	// n2 has replicated up to index 2; n3 has nothing.
	n.matchIndex = map[NodeID]Index{"n1": 0, "n2": 2, "n3": 0}
	n.maybeAdvanceCommitLocked()
	got := n.commitIndex
	n.mu.Unlock()

	// Sorted matches with self=3: [0(n3), 2(n2), 3(self)]; quorum=2,
	// quorumIndex = matches[3-2] = matches[1] = 2. Index 2 is term 5
	// (current) -> commit advances to 2.
	if got != 2 {
		t.Fatalf("commitIndex=%d after current-term quorum; want 2 (REPL-06)", got)
	}
}

// TestCommitSkipsPriorTermEntry — REPL-06 / Figure-8 unit guard. A
// prior-term entry replicated on a majority is NOT committed by replica
// count; once a current-term entry above it reaches quorum, commitIndex
// jumps past it (Log Matching carries the earlier entry indirectly).
func TestCommitSkipsPriorTermEntry(t *testing.T) {
	t.Parallel()
	peers := []NodeID{"n1", "n2", "n3"}
	n := newLeaderForCommit(t, "n1", peers)

	n.mu.Lock()
	n.currentTerm = 5
	// Indices 1,2 are PRIOR term (3); no current-term entry exists yet.
	seedLog(n, []Term{3, 3})
	// A majority (self + n2) has index 2 replicated.
	n.matchIndex = map[NodeID]Index{"n1": 0, "n2": 2, "n3": 0}
	n.maybeAdvanceCommitLocked()
	priorCommit := n.commitIndex
	n.mu.Unlock()

	// quorumIndex would be 2, but log[2].term==3 != currentTerm==5, so the
	// Figure-8 guard blocks the advance — commitIndex stays 0.
	if priorCommit != 0 {
		t.Fatalf("commitIndex=%d: prior-term entry committed by replica count (REPL-06 Figure-8 violation)", priorCommit)
	}

	// Now append a CURRENT-term entry at index 3 and replicate it to quorum.
	n.mu.Lock()
	seedLog(n, []Term{}) // no-op; keep symmetry
	n.log.Append(Entry{Term: 5, Index: 3})
	n.matchIndex = map[NodeID]Index{"n1": 0, "n2": 3, "n3": 0}
	n.maybeAdvanceCommitLocked()
	got := n.commitIndex
	n.mu.Unlock()

	// Sorted matches with self=LastIndex=3: [0, 3(n2), 3(self)]; quorum=2,
	// quorumIndex = matches[1] = 3. log[3].term==5==current -> commit jumps
	// to 3, carrying the prior-term entries 1,2 indirectly.
	if got != 3 {
		t.Fatalf("commitIndex=%d after current-term entry reaches quorum; want 3 (indirect commit of prior entries)", got)
	}
}

// TestQuorumSortedNotMapOrder — SC6 / REPL-08 / C-8. The commit rule MUST
// pick the quorum index from a SORTED value snapshot of matchIndex, never
// from map iteration order.
//
// Why a naive map-order scan fails here: with self substituted to
// LastIndex=7, the value multiset is {self:7, n2:7, n4:3, n3:0, n5:0}.
// The correct sorted snapshot is [0,0,3,7,7]; with quorum=3 the quorum
// index is matches[5-3]=matches[2]=3 (the 3rd-highest, i.e. the largest
// index a majority has reached). A buggy "first time the running count of
// values >= N hits quorum" scan in Go's randomised map-iteration order
// could surface the two 7s and self early and wrongly commit 7 — or, on a
// different visiting order, land on a different N — i.e. a per-run
// commitIndex. The sort makes the answer deterministically 3 every time.
// We assert exactly 3 across repeated invocations (Go randomises map
// iteration; -count=5 in the plan verify proves the sort kills the flake).
func TestQuorumSortedNotMapOrder(t *testing.T) {
	t.Parallel()
	peers := []NodeID{"n1", "n2", "n3", "n4", "n5"}

	// Run several times within the test; each fresh leader gets a fresh map
	// whose iteration order Go randomises, yet the result must be stable.
	for run := 0; run < 8; run++ {
		n := newLeaderForCommit(t, "n1", peers)
		n.mu.Lock()
		n.currentTerm = 4
		// 7 current-term entries so index 3 and 7 are both term 4.
		seedLog(n, []Term{4, 4, 4, 4, 4, 4, 4})
		// self substituted to LastIndex=7 in the snapshot. Set matchIndex[self]
		// to a deliberately STALE 0 to prove the substitution kicks in.
		n.matchIndex = map[NodeID]Index{"n1": 0, "n2": 7, "n3": 0, "n4": 3, "n5": 0}
		n.maybeAdvanceCommitLocked()
		got := n.commitIndex
		n.mu.Unlock()

		// matches (self->7): [0,0,3,7,7]; quorum=3; matches[5-3]=matches[2]=3.
		if got != 3 {
			t.Fatalf("run %d: commitIndex=%d; want 3 (sorted-quorum determinism, SC6/REPL-08 — map-order scan would diverge)", run, got)
		}
	}
}

// TestFreshLeaderQuorumNoAdvance — P1-2 reinforcement. All followers at
// matchIndex 0 while only self is ahead: the quorum index is 0, so
// commitIndex does not advance in a 3- or 5-node cluster.
func TestFreshLeaderQuorumNoAdvance(t *testing.T) {
	t.Parallel()
	for _, peers := range [][]NodeID{
		{"n1", "n2", "n3"},
		{"n1", "n2", "n3", "n4", "n5"},
	} {
		n := newLeaderForCommit(t, "n1", peers)
		n.mu.Lock()
		n.currentTerm = 2
		seedLog(n, []Term{2, 2, 2}) // self has 3 entries
		// Every follower is empty; self alone is ahead.
		n.matchIndex = map[NodeID]Index{}
		for _, p := range peers {
			n.matchIndex[p] = 0
		}
		n.maybeAdvanceCommitLocked()
		got := n.commitIndex
		n.mu.Unlock()

		// Sorted matches (self->3): only ONE value (self) is > 0; with
		// quorum >= 2 the quorum index is 0 -> no advance.
		if got != 0 {
			t.Fatalf("%d-node: commitIndex=%d with only self ahead; want 0 (P1-2)", len(peers), got)
		}
	}
}

// TestSlowProbeRewind — REPL-04. A failed AppendEntries response
// decrements nextIndex[peer] by exactly 1; repeated failures floor it at
// 1 (never 0 or negative). First-class unit coverage independent of the
// end-to-end TestFigure8 path.
func TestSlowProbeRewind(t *testing.T) {
	t.Parallel()
	peers := []NodeID{"n1", "n2", "n3"}
	n := newLeaderForCommit(t, "n1", peers)

	n.mu.Lock()
	term := n.currentTerm
	n.nextIndex["n2"] = 5
	n.mu.Unlock()

	fail := func() Index {
		if err := n.Step(Message{
			Type:    MsgAppendEntriesResp,
			Term:    term,
			From:    "n2",
			To:      "n1",
			Success: false,
		}); err != nil {
			t.Fatalf("Step failed AE resp: %v", err)
		}
		n.mu.Lock()
		defer n.mu.Unlock()
		return n.nextIndex["n2"]
	}

	if got := fail(); got != 4 {
		t.Fatalf("nextIndex[n2]=%d after one failure; want 4 (REPL-04 decrement by 1)", got)
	}
	// Drive repeated failures; nextIndex must floor at 1, never below.
	for i := 0; i < 10; i++ {
		got := fail()
		if got < 1 {
			t.Fatalf("nextIndex[n2]=%d after repeated failures; must floor at 1 (REPL-04)", got)
		}
	}
	n.mu.Lock()
	final := n.nextIndex["n2"]
	n.mu.Unlock()
	if final != 1 {
		t.Fatalf("nextIndex[n2]=%d after many failures; want floor 1 (REPL-04)", final)
	}
}

// TestStaleAppendResponseIgnored — RESEARCH anti-pattern guard. A
// response carrying a prior term must NOT mutate matchIndex/nextIndex
// (matchIndex corruption from a stale reply would poison the commit
// snapshot). Mirrors the RequestVote-response stale guard.
func TestStaleAppendResponseIgnored(t *testing.T) {
	t.Parallel()
	peers := []NodeID{"n1", "n2", "n3"}
	n := newLeaderForCommit(t, "n1", peers)

	n.mu.Lock()
	cur := n.currentTerm
	n.matchIndex["n2"] = 4
	n.nextIndex["n2"] = 5
	n.mu.Unlock()

	// A success response from a STALE prior term (cur-1, guaranteed < cur
	// since driveToLeader wins term >= 1). stepLocked does NOT route a
	// lower-or-equal term through step-down, so it reaches the handler,
	// where the term guard must reject it.
	staleTerm := cur - 1
	if err := n.Step(Message{
		Type:       MsgAppendEntriesResp,
		Term:       staleTerm,
		From:       "n2",
		To:         "n1",
		Success:    true,
		MatchIndex: 99, // would corrupt the snapshot if accepted
	}); err != nil {
		t.Fatalf("Step stale AE resp: %v", err)
	}

	n.mu.Lock()
	gotMatch := n.matchIndex["n2"]
	gotNext := n.nextIndex["n2"]
	n.mu.Unlock()
	if gotMatch != 4 || gotNext != 5 {
		t.Fatalf("stale response mutated progress: matchIndex=%d nextIndex=%d; want 4/5 unchanged", gotMatch, gotNext)
	}
}
