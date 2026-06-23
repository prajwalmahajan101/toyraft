// Ready() is the outbound drain that pairs every (Term, VotedFor) shift
// with the Messages emitted under that shift. The caller MUST persist
// the returned *HardState (if non-nil) via Storage.SaveHardState BEFORE
// shipping any of the returned Messages via Transport.Send. This
// ordering is required by:
//
//   - Raft §5.2 (a granted vote MUST be on durable storage before the
//     response leaves the process)
//   - LLD §5 Invariant 1 (fsync ordering for HardState vs RPC)
//   - ROADMAP SC5 (assertion-enforced across three layers)
//   - PITFALLS P0-4 (HardState reordering corrupts the vote contract)
//
// Three enforcement layers:
//
//   - Layer 1 — driver discipline: the only Phase 5 caller is
//     internal/raftest.Cluster.tickOnce (lands in plan 05-05); the Phase
//     7 public driver in pkg/raft/driver.go honours the same contract.
//     Both invoke SaveHardState before any Send.
//   - Layer 2 — raftdebug build-tagged invariant
//     (assertReadyInvariantsLocked in ready_assert_debug.go): under the
//     `raftdebug` build tag every Ready() call panics if pendingHS is
//     present and any RequestVoteResponse{VoteGranted=true} in the same
//     batch disagrees with pendingHS.VotedFor.
//   - Layer 3 — internal/raftest.OrderingStorage: records SaveHardState
//     and RecordSend events in a single monotonic log;
//     AssertHardStatePrecedesVoteGrantedResponse(t) verifies the
//     per-(term, votedFor) precedence at test teardown.

package raft

// Ready snapshots pendingMsgs and pendingHS under n.mu, then clears
// them. Pending messages are filtered by stepDownEpoch — entries queued
// under a prior role (any pm.epoch < n.stepDownEpoch) are dropped on
// the floor. That filter is the TOCTOU-free step-down halt mechanism
// (ELEC-08 / P0-5): a candidate that observed a higher term via
// stepLocked has its in-flight RequestVote fan-out invalidated before
// any of those messages can be shipped under the new term.
//
// Ready acquires n.mu; the caller MUST NOT already hold it. The
// returned slice and pointer are owned by the caller after return; the
// internal buffers are reset to empty.
//
// Returned values:
//
//   - msgs: a freshly-allocated slice (possibly empty) of Messages to
//     ship after the HardState fence. Caller may mutate freely.
//   - hs:   non-nil iff (CurrentTerm, VotedFor, Commit) changed since
//     the last Ready() drain. Caller MUST persist this BEFORE
//     shipping msgs (see package doc above for the three
//     enforcement layers).
//
// Subsequent Ready() calls return (empty slice, nil) until the next
// state-machine event populates the buffers.
func (n *node) Ready() (msgs []Message, hs *HardState) {
	n.mu.Lock()
	defer n.mu.Unlock()
	// raftdebug-tagged invariant; no-op in production builds.
	assertReadyInvariantsLocked(n)

	msgs = make([]Message, 0, len(n.pendingMsgs))
	for _, pm := range n.pendingMsgs {
		if pm.epoch == n.stepDownEpoch {
			msgs = append(msgs, pm.msg)
			continue
		}
		// Stale-epoch message: queued under a prior role. Dropped per
		// ELEC-08 / P0-5 — shipping it would race the new term.
	}
	n.pendingMsgs = n.pendingMsgs[:0]

	hs = n.pendingHS
	n.pendingHS = nil
	return msgs, hs
}
