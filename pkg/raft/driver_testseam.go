package raft

import "context"

// driver_testseam.go provides the NARROW synchronous test-driver seam the
// internal/raftest harness uses to drive a real raft.Node deterministically
// (07-04, harness model ii). It runs the EXACT production driver+apply
// sequence (runTicker body + applyOne), but SYNCHRONOUSLY on the caller's
// goroutine instead of via the async FakeClock ticker + apply goroutine.
//
// WHY MODEL (ii): the own-driver model (i) — each node running its async
// runTicker on a shared clock.Fake with an async Hub dispatcher + inbound
// pump — is functionally correct but NOT deterministically settle-able
// within the chaos suite's wall-clock budget: a single FakeClock.Advance
// does not synchronously flush the heartbeat -> delivery -> Step -> commit ->
// apply pipeline across the dispatcher/pump/driver/applier goroutine
// boundaries, so the harness must spin on a wall-clock settle whose cost
// (multiplied by Figure-8's thousands of ticks and the 1000-seed sweep)
// blows the budget. The plan authorised this fallback. Model (ii) restores
// the proven Phase-5 synchronous determinism while still exercising the REAL
// production driver logic (the same Step/Ready/SaveHardState/Send/drain code
// runTicker runs) and the public surface (raftest drains inbound via the
// public Step, and proposals go through the public Propose).
//
// GOLDEN: TickForTest is an EXPORTED method on the UNEXPORTED *nodeImpl type.
// `go doc -all ./pkg/raft` does NOT list methods on unexported types, so this
// adds ZERO entries to docs/lld-go-doc-golden.txt — honouring the plan's
// "do NOT add exported surface (it would grow the LLD golden)" constraint
// while remaining callable cross-package (raftest type-asserts raft.Node to a
// local interface carrying this method).

// TickForTest runs ONE deterministic driver iteration synchronously and
// returns. It mirrors runTicker's body VERBATIM in ordering (Global Invariant
// 1 / SC5: SaveHardState precedes every Send; no lock held across Send), then
// applies every newly-committed entry inline via applyOne (the same applier
// the async runApply goroutine would have run), so a blocked public Propose
// is unblocked in lockstep.
//
// It does NOT consume the FakeClock ticker (the node is NOT Start'd in model
// ii), so there is no async driver/applier goroutine to race — the harness
// owns the schedule. Inbound messages are delivered separately by the harness
// via the public Step before/around each TickForTest call.
func (n *nodeImpl) TickForTest() {
	// 1. MsgTick -> drives the election timer + leader heartbeat cadence.
	if err := n.core.Step(Message{Type: MsgTick}); err != nil {
		n.cfg.Logger.Error("raft: TickForTest tick Step", "err", err)
	}
	n.drainAndApplyForTest()
}

// DrainForTest runs the Ready -> SaveHardState -> Send -> apply tail of the
// driver WITHOUT a MsgTick. The harness uses it for the SECOND pass of a Tick
// (after inbound from the first pass has been delivered): it lets a node react
// to freshly-delivered messages (emit AE responses / a stepped-down node's new
// HardState) and apply any commit that resulted, WITHOUT advancing the
// tick-domain election timer a second time. This keeps electionElapsed
// advancing exactly ONCE per Cluster.Tick (one MsgTick per Tick), so a
// Cluster.Tick(d) corresponds to a single logical tick of election timing —
// the stable cadence the Figure-8 / chaos budgets assume.
func (n *nodeImpl) DrainForTest() {
	n.drainAndApplyForTest()
}

// drainAndApplyForTest is the shared Ready -> SaveHardState (SC5) -> Send ->
// apply tail used by both TickForTest and DrainForTest.
func (n *nodeImpl) drainAndApplyForTest() {
	// Ready() copies pendingMsgs/pendingHS under core.mu, releasing the lock on
	// return — every Send below runs with NO lock held (SC5).
	msgs, hs := n.core.Ready()
	// SC5: persist HardState BEFORE any Send.
	if hs != nil {
		if err := n.cfg.Storage.SaveHardState(*hs); err != nil {
			n.cfg.Logger.Error("raft: SaveHardState", "err", err)
		}
	}
	// Send each outbound message (best-effort; the Transport is lossy).
	for _, m := range msgs {
		if err := n.cfg.Transport.Send(context.Background(), m); err != nil {
			n.cfg.Logger.Debug("raft: Send", "to", m.To, "err", err)
		}
	}
	// Apply newly-committed entries SYNCHRONOUSLY (the work the async
	// drainCommitsToApply + runApply pair does). This advances appliedIdx and
	// signals any blocked Propose waiter in index order.
	n.applyCommittedForTest()
}

// applyCommittedForTest applies every entry in (appliedIdx, commitIndex]
// synchronously, in index order, via the production applyOne. It is the
// synchronous fusion of drainCommitsToApply (read each committed entry from
// Storage) + runApply (apply it), with the bounded channel skipped — there is
// no async producer/consumer split in model (ii), so entries flow straight
// from Storage to applyOne. appliedIdx is advanced by applyOne (its sole
// writer), keeping Status().ApplyIndex honest.
func (n *nodeImpl) applyCommittedForTest() {
	n.core.mu.Lock()
	ci := n.core.commitIndex
	n.core.mu.Unlock()

	for i := Index(n.appliedIdx.Load()) + 1; i <= ci; i++ {
		ents, err := n.cfg.Storage.Entries(i, i+1) // half-open [i, i+1)
		if err != nil || len(ents) == 0 {
			n.cfg.Logger.Error("raft: TickForTest read commit", "index", i, "err", err)
			return
		}
		n.applyOne(ents[0]) // advances appliedIdx (sole writer) + signals waiter
	}
}

// testDriver is the narrow interface raftest type-asserts raft.Node to so it
// can drive the synchronous seam without depending on the unexported
// *nodeImpl type. Declared here (NOT in raftest) only as a compile-time
// assertion that *nodeImpl satisfies it; raftest declares its own identical
// local interface for the assertion.
type testDriver interface {
	TickForTest()
	DrainForTest()
}

// Compile-time assertion: *nodeImpl satisfies the synchronous test-driver
// seam. (Methods on the unexported *nodeImpl stay out of the LLD golden.)
var _ testDriver = (*nodeImpl)(nil)
