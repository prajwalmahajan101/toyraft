package raft_test

// figure8_test.go scripts the canonical Raft Figure 8 scenario (paper
// §5.4.2) end-to-end against the REAL state machine, retiring SC3 /
// REPL-10: no entry committed by an old leader is ever lost after leader
// churn, and — the decisive guarantee — once an index is reported
// committed, no later term ever changes the entry at that index.
//
// PACKAGE BOUNDARY (REQUIRED): this file is `package raft_test` (external)
// because it imports internal/raftest, which imports pkg/raft — an
// in-package `package raft` test would form an import cycle. It therefore
// drives the cluster through the EXPORTED raftest.Cluster + public raft.Node
// surface only (NewCluster, Leader/NodeByID/Tick/AssertLogMatching/
// AssertNoCommittedEntryLost, the raftest seams ProposeIntoForTest/LogOf, and
// the public raft.Node.Status) — never in-package *node access.
//
// 07-04 NOTE: raft.TestNode is DELETED in Phase 7. The old call sites that
// reached the wrapped node via Cluster.NodeByID(id).Node().{Propose,Log,
// CommitIndex,RoleAndTerm} are rewired to the public surface:
//   - Propose(Index,bool) -> Cluster.ProposeIntoForTest(id, op) (index-pinned
//     seam: injects via public Propose on a cluster-ctx goroutine, returns the
//     leader's assigned index from its Storage mirror WITHOUT blocking on
//     apply — faithful to the non-committing isolated-leader stages a/b/c).
//   - Log() -> Cluster.LogOf(id) (reads the durable Storage mirror, ADR-0011).
//   - CommitIndex() -> raft.Node.Status().CommitIndex.
//   - RoleAndTerm() -> raft.Node.Status().{Role,Term}.
// All four Figure-8 safety assertions + AssertNoCommittedEntryLost are
// preserved byte-for-byte in intent.
//
// DETERMINISM STRATEGY: election timeouts are RNG-driven, so neither the
// WHICH node leads a stage nor the ABSOLUTE term it wins at is fixed across
// runs (a clean first election can land at term 1 or higher depending on
// scheduling, especially under -race). The Figure 8 SAFETY property does
// not depend on either: it depends only on the SHAPE of the churn and the
// RELATIVE term ordering termA < termB < termC, discovered from whichever
// node actually wins in the connected island — never asserted against an
// absolute term floor or a fixed node identity. An old-term entry
// replicated to a majority must not commit on replica count; it commits
// only once a current-term entry above it reaches quorum; and a committed
// entry must survive the subsequent churn. We PARTITION the cluster into
// controlled groups, let whichever node wins in the connected group lead
// (discovered by scanning the island for Role=Leader at a term strictly
// above the prior stage's term — NOT via the cluster-wide c.Leader(),
// which can return a stale isolated leader), and track entries by INDEX +
// TERM (the live termA/termB/termC) rather than by node identity. Because
// the setup is keyed off the actual elected leader and relative terms — and
// the tick budgets are generous upper bounds, not assertions — the scripted
// shape is reached on EVERY run at the fixed seed, leaving only the safety
// assertions able to fail.

import (
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/raftest"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// The five node IDs the cluster allocates (n00..n04, zero-padded so lex
// order == index order — see NewCluster).
const (
	n0 = raft.NodeID("n00")
	n1 = raft.NodeID("n01")
	n2 = raft.NodeID("n02")
	n3 = raft.NodeID("n03")
	n4 = raft.NodeID("n04")
)

var allNodes = []raft.NodeID{n0, n1, n2, n3, n4}

// connectOnly heals all partitions, then symmetrically partitions every
// node NOT in `group` away from every other node. The result: `group` is
// a fully-connected island and every other node is fully isolated. This
// lets the test pin exactly which nodes can vote / replicate in a given
// stage. Each call fully resets the partition state first.
func connectOnly(c *raftest.Cluster, group ...raft.NodeID) {
	in := make(map[raft.NodeID]bool, len(group))
	for _, g := range group {
		in[g] = true
	}
	for i := range allNodes {
		for j := i + 1; j < len(allNodes); j++ {
			a, b := allNodes[i], allNodes[j]
			if in[a] && in[b] {
				c.Hub.Heal(a, b)
			} else {
				c.Hub.Partition(a, b)
			}
		}
	}
}

// driveUntilLeaderIn ticks until some node IN `group` reports Role=Leader
// AT A TERM > minTerm, checking invariants every tick. Returns the leader
// ID and its term.
//
// Two subtleties this guards against:
//   - Restricting to `group` is essential: an ISOLATED node from an earlier
//     stage may still report Role=Leader at its stale term (isolation alone
//     does not demote it), so a cluster-wide c.Leader() scan would return
//     the wrong, crashed node.
//   - The minTerm floor is essential when a stale in-group leader exists: a
//     node reconnected from an earlier stage may STILL hold Leader at its
//     old term for a few ticks before it sees a higher-term message and
//     steps down. We must not accept that stale leadership as "the new
//     leader" — we wait for a fresh election to produce a leader at a term
//     strictly above minTerm.
func driveUntilLeaderIn(t *testing.T, c *raftest.Cluster, minTerm raft.Term, maxTicks int, group ...raft.NodeID) (raft.NodeID, raft.Term) {
	t.Helper()
	for range maxTicks {
		c.Tick(10 * time.Millisecond)
		c.AssertLogMatching()
		c.AssertNoCommittedEntryLost()
		for _, node := range group {
			s := c.NodeByID(node).Node().Status()
			if s.Role == raft.Leader && s.Term > minTerm {
				return node, s.Term
			}
		}
	}
	t.Fatalf("Figure8: no leader emerged in %v at term > %d within %d ticks (seed=%d)",
		group, minTerm, maxTicks, c.Seed)
	return "", 0
}

// driveTicks runs n plain ticks with invariant checks, letting in-flight
// replication settle (heartbeats, AE responses, commit advance).
func driveTicks(t *testing.T, c *raftest.Cluster, n int) {
	t.Helper()
	for range n {
		c.Tick(10 * time.Millisecond)
		c.AssertLogMatching()
		c.AssertNoCommittedEntryLost()
	}
}

// proposeInto proposes op directly into the SPECIFIC node `leader` (the
// connected island's elected leader, tracked by the test) and fails if it
// refuses. We target the node explicitly rather than via c.ProposeToLeader
// /c.Leader(): an isolated leader from an EARLIER stage still reports
// Role=Leader at its stale term (isolation does not demote it), so a
// cluster-wide first-leader scan could route the proposal into the wrong,
// crashed node. The Figure 8 script must inject each entry into the leader
// of the currently-connected group.
//
// 07-04: routes through Cluster.ProposeIntoForTest — the index-pinned seam
// over the public raft.Node.Propose. It returns the leader's ASSIGNED INDEX
// at local-append time WITHOUT blocking on apply, so the non-committing
// isolated-leader stages (a/b/c) — which propose old-term entries that never
// commit — do not hang (the underlying Propose goroutine is bound to the
// cluster ctx and reaped at Close). The (!ok -> Fatalf) refusal semantics are
// preserved exactly.
func proposeInto(t *testing.T, c *raftest.Cluster, stage string, leader raft.NodeID, op string) raft.Index {
	t.Helper()
	idx, ok := c.ProposeIntoForTest(leader, []byte(op))
	if !ok {
		t.Fatalf("Figure8 (%s): Propose(%q) into %s refused — not leader (seed=%d)",
			stage, op, leader, c.Seed)
	}
	return idx
}

// logTermAt returns the term of the entry at 1-based index idx on node,
// or 0 if the node's log is shorter than idx. Reads the durable Storage
// mirror via Cluster.LogOf (R-4 LOCKED; ADR-0011).
func logTermAt(c *raftest.Cluster, node raft.NodeID, idx raft.Index) raft.Term {
	entries := c.LogOf(node)
	if int(idx) > len(entries) {
		return 0
	}
	return entries[idx-1].Term
}

// commitIndexOf returns node's reported commitIndex via the public Status.
func commitIndexOf(c *raftest.Cluster, node raft.NodeID) raft.Index {
	return c.NodeByID(node).Node().Status().CommitIndex
}

// TestFigure8 reproduces the canonical 5-node §5.4.2 scenario and proves
// the current-term commit rule (REPL-06) prevents the Figure 8 data-loss
// hole. The decisive assertions:
//
//   - A leader holding an OLD-term entry at index-2 replicated to a
//     majority does NOT report index-2 committed (log[2].term != currentTerm).
//   - index-2 commits only once the leader replicates a CURRENT-term entry
//     above it to a quorum — at which point Log Matching carries the
//     old-term entry to that same quorum and it is safe forever.
//   - The committed index-2 entry then survives subsequent churn;
//     AssertNoCommittedEntryLost holds at every tick throughout.
func TestFigure8(t *testing.T) {
	c := raftest.NewCluster(t, 5, 12345)

	// ---- Stage (a): with the WHOLE cluster connected, a leader emerges and
	// commits index-1 to ALL five nodes (the shared baseline every node
	// carries through the churn). Then shrink the island to {leaderA, keep}
	// so index-2 reaches an OLD-term minority only (the classic Figure 8
	// setup: the term-A entry at index-2 lives on a minority that does not
	// yet commit, because every node already agrees on index-1).
	connectOnly(c, allNodes...)
	leaderA, termA := driveUntilLeaderIn(t, c, 0, 400, allNodes...)
	// termA is the actual first-election term: a clean first election from a
	// freshly-booted cluster validly wins at term 1, and the Figure 8 shape
	// needs only termA >= 1 plus the RELATIVE ordering termA < termB < termC
	// (enforced by stages b/c against this live termA). driveUntilLeaderIn(.,0,.)
	// already guarantees term > 0, so this is a redundant sanity check — NOT a
	// >= 2 floor (that spurious floor caused the seed-12345 -race flake by
	// rejecting the legitimate term-1 first leader).
	if termA < 1 {
		t.Fatalf("Figure8 (a): leaderA=%s term=%d; want >= 1 (seed=%d)", leaderA, termA, c.Seed)
	}

	// index-1: replicate + commit across all five nodes (shared baseline).
	// Propose into leaderA's LIVE term (it may have re-elected at a higher term
	// since the first election); the baseline check below compares against the
	// term index-1 actually landed at on leaderA, not the captured first term.
	proposeInto(t, c, "a/idx1", leaderA, "a-idx1")
	driveTicks(t, c, 15)
	baseTerm := logTermAt(c, leaderA, 1)
	for _, node := range allNodes {
		if logTermAt(c, node, 1) != baseTerm {
			t.Fatalf("Figure8 (a): node %s missing baseline index-1 (term %d); got %d (seed=%d)",
				node, baseTerm, logTermAt(c, node, 1), c.Seed)
		}
	}

	// Pick a follower to keep with leaderA for the next (minority)
	// replication: any node that is not leaderA.
	var keep raft.NodeID
	for _, cand := range allNodes {
		if cand != leaderA {
			keep = cand
			break
		}
	}

	// Shrink to {leaderA, keep}: a 2-node minority of 5. index-2 will be
	// appended to leaderA's log and replicated only to `keep` — never a
	// quorum — so it CANNOT commit during term A.
	connectOnly(c, leaderA, keep)

	// Re-bind termA to leaderA's LIVE term at the instant it appends index-2.
	// Between the first election and this point, RNG-driven timeouts can
	// re-elect leaderA at a higher term (it still reports Role=Leader, just at
	// a newer term), so the term captured at the first election may be stale.
	// The Figure 8 shape only needs index-2 to carry leaderA's OWN term as the
	// "old term" that stages (b)/(c) then beat with strictly-higher termB/termC
	// — so we discover that old term from leaderA itself rather than assuming
	// the first-election value still holds. (This was the residual seed-12345
	// -race flake: index-2 landed at the re-elected term, not the captured one.)
	for range 400 {
		s := c.NodeByID(leaderA).Node().Status()
		if s.Role == raft.Leader && s.Term >= termA {
			termA = s.Term
			break
		}
		c.Tick(10 * time.Millisecond)
		c.AssertLogMatching()
		c.AssertNoCommittedEntryLost()
	}
	if s := c.NodeByID(leaderA).Node().Status(); s.Role != raft.Leader {
		t.Fatalf("Figure8 (a): leaderA=%s lost leadership before index-2 (role=%v term=%d) (seed=%d)",
			leaderA, s.Role, s.Term, c.Seed)
	}

	idx2 := proposeInto(t, c, "a/idx2", leaderA, "a-idx2-oldterm")
	if idx2 != 2 {
		t.Fatalf("Figure8 (a): index-2 entry assigned index %d; want 2 (seed=%d)", idx2, c.Seed)
	}
	driveTicks(t, c, 8)

	// leaderA holds an OLD-term (term-A) entry at index-2 on a minority.
	if got := logTermAt(c, leaderA, 2); got != termA {
		t.Fatalf("Figure8 (a): leaderA log[2].term=%d; want %d (old-term entry) (seed=%d)",
			got, termA, c.Seed)
	}
	// It is NOT committed (minority only).
	if ci := commitIndexOf(c, leaderA); ci >= 2 {
		t.Fatalf("Figure8 (a): leaderA committed index %d >= 2 on a MINORITY — impossible (seed=%d)",
			ci, c.Seed)
	}

	// ---- Stage (b): leaderA + keep "crash" (isolated). The OTHER three
	// nodes {the rest} elect a new leader at a HIGHER term and accept a
	// DIFFERENT entry at index-2 locally. This is the divergent index-2.
	rest := make([]raft.NodeID, 0, 3)
	for _, x := range allNodes {
		if x != leaderA && x != keep {
			rest = append(rest, x)
		}
	}
	connectOnly(c, rest...) // {leaderA, keep} now fully isolated ("crashed")
	leaderB, termB := driveUntilLeaderIn(t, c, termA, 400, rest...)
	if termB <= termA {
		t.Fatalf("Figure8 (b): leaderB=%s term=%d; want > %d (seed=%d)",
			leaderB, termB, termA, c.Seed)
	}
	// Designate the third rest node — the one that is NEITHER leaderB nor
	// the partner we keep with leaderB — as the "clean" node: it stays
	// connected during the election (so leaderB can win a quorum) but the
	// divergent term-B index-2 entry is proposed only AFTER we shrink to
	// {leaderB, bPartner}, so the clean node NEVER receives a term-B
	// index-2. Its index-2 stays empty, exactly like S3 in the paper. That
	// lets leaderA (with its old term-A index-2) win stage (c) by the
	// election restriction.
	var bPartner, clean raft.NodeID
	for _, x := range rest {
		if x == leaderB {
			continue
		}
		if bPartner == "" {
			bPartner = x
		} else {
			clean = x
		}
	}
	// Shrink leaderB's island to {leaderB, bPartner}: a minority. The
	// divergent term-B index-2 reaches only these two; it never commits and
	// `clean` never sees it.
	connectOnly(c, leaderB, bPartner)
	proposeInto(t, c, "b/idx2", leaderB, "b-idx2-diff")
	driveTicks(t, c, 10)
	if got := logTermAt(c, leaderB, 2); got != termB {
		t.Fatalf("Figure8 (b): leaderB log[2].term=%d; want %d (divergent entry) (seed=%d)",
			got, termB, c.Seed)
	}
	if got := logTermAt(c, clean, 2); got != 0 {
		t.Fatalf("Figure8 (b): clean node %s unexpectedly has index-2 (term %d); "+
			"the divergent entry leaked to the stage-(c) third node (seed=%d)",
			clean, got, c.Seed)
	}

	// ---- Stage (c): leaderB + bPartner "crash"; leaderA + keep are
	// restored and joined with `clean` (whose index-2 is empty), forming the
	// majority {leaderA, keep, clean}. leaderA (carrying the OLD term-A
	// index-2 entry) is at least as up-to-date as clean, so it can win an
	// election at a term > B, then replicates its old term-A index-2 entry
	// to this majority. DECISIVE: it must NOT commit index-2 on replica
	// count (log[2].term == termA != currentTerm).
	connectOnly(c, leaderA, keep, clean)
	leaderC, termC := driveUntilLeaderIn(t, c, termB, 600, leaderA, keep, clean)
	if termC <= termB {
		t.Fatalf("Figure8 (c): leaderC=%s term=%d; want > %d (seed=%d)",
			leaderC, termC, termB, c.Seed)
	}
	// Let the new leader replicate its existing log (incl. the old-term
	// index-2 entry, if it holds it) across the majority. No new proposal.
	driveTicks(t, c, 25)

	// The decisive check is meaningful only when the elected leader carries
	// the old-term index-2 entry (leaderA or keep). The election restriction
	// (§5.4.1) guarantees the winner's log is at least as up-to-date as a
	// majority; since `clean` has no index-2, leaderA/keep are eligible.
	if logTermAt(c, leaderC, 2) != termA {
		t.Fatalf("Figure8 (c): leaderC=%s log[2].term=%d; want the OLD term %d — the "+
			"election restriction should have elected a holder of the old-term entry (seed=%d)",
			leaderC, logTermAt(c, leaderC, 2), termA, c.Seed)
	}
	// leaderC holds the old-term index-2 entry, replicated to the majority.
	// It must NOT have committed it by replica count.
	if ci := commitIndexOf(c, leaderC); ci >= 2 {
		t.Fatalf("Figure8 (c): leaderC committed index %d >= 2 on a PRIOR-term "+
			"entry (term %d != currentTerm %d) — REPL-06 Figure-8 violation; the "+
			"committed entry could later be lost (seed=%d)", ci, termA, termC, c.Seed)
	}

	// ---- Stage (d): leaderC replicates a CURRENT-term (termC) entry above
	// index-2 to the majority. NOW index-2 commits indirectly (Log Matching
	// carries the earlier entries to the same quorum). Record it committed.
	proposeInto(t, c, "d/current", leaderC, "c-idx-current")
	driveTicks(t, c, 25)

	committedCI := commitIndexOf(c, leaderC)
	if committedCI < 2 {
		t.Fatalf("Figure8 (d): leaderC commitIndex=%d; want >= 2 (a current-term entry "+
			"above index-2 reached quorum, so index-2 commits indirectly) (seed=%d)",
			committedCI, c.Seed)
	}
	// AssertNoCommittedEntryLost has now recorded indices 1..committedCI as
	// the immutable committed entries.
	c.AssertNoCommittedEntryLost()
	committedIdx2Term := logTermAt(c, leaderC, 2)

	// ---- Stage (e): churn again — fully reconnect the cluster and let any
	// node attempt re-election. With index-2 committed on a majority, the
	// election restriction prevents any node with a conflicting / shorter
	// log from winning and overwriting it. The committed index-2 entry must
	// NEVER change. AssertNoCommittedEntryLost runs every tick below.
	connectOnly(c, allNodes...) // heal everything
	driveTicks(t, c, 40)

	// Final explicit check: index-2 is unchanged on every node that holds a
	// committed entry there. (AssertNoCommittedEntryLost already enforced
	// this continuously; this is the human-readable SC3 assertion.)
	for _, node := range allNodes {
		ci := commitIndexOf(c, node)
		if ci >= 2 {
			if got := logTermAt(c, node, 2); got != committedIdx2Term {
				t.Fatalf("Figure8 (e): node %s reports index-2 committed but its term=%d; "+
					"committed entry was term %d — a committed entry was overwritten after "+
					"churn (SC3/REPL-10 violation) (seed=%d)",
					node, got, committedIdx2Term, c.Seed)
			}
		}
	}
}
