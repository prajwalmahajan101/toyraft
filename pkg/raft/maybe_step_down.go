package raft

// maybeStepDownLocked is the SINGLE path through which the state
// machine handles a higher-term observation (Raft §5.1). Caller MUST
// hold n.mu.
//
// Invariants enforced:
//   - currentTerm is set to rpcTerm.
//   - votedFor is cleared (a fresh term has no vote yet).
//   - role drops to Follower.
//   - leaderHint is cleared; the new term has not elected anyone yet.
//   - votesReceived is dropped; any in-flight candidate tally is void.
//   - stepDownEpoch is bumped EXACTLY ONCE so any outbound message
//     queued under the prior role is discardable by Ready() (ELEC-08 /
//     P0-5 — the TOCTOU window we are closing).
//   - HardState is queued so the driver persists the new
//     (currentTerm, votedFor=nil) tuple BEFORE shipping any RPC the
//     new role might emit (SC5 — Phase 5's persist-first invariant).
//
// Election timer reset is NOT this method's responsibility. The per-
// role handlers landed in 05-02 (follower) reset the timer when an
// AppendEntries from the new leader arrives; clearing here would race
// against the very next stepLocked call that just delegated to us.
//
// The early-return on rpcTerm <= n.currentTerm makes this method safe
// to call defensively — though the only caller is stepLocked, which
// already gates the call on the inverse condition.
func (n *node) maybeStepDownLocked(rpcTerm Term) {
	if rpcTerm <= n.currentTerm {
		return
	}
	n.currentTerm = rpcTerm
	n.votedFor = ""
	n.role = Follower
	n.leaderHint = ""
	n.votesReceived = nil
	n.stepDownEpoch++
	n.queueHardStateLocked()
}
