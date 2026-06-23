package raft

import "fmt"

// stepLocked is the single inbound dispatcher. Caller MUST hold n.mu.
//
// INVARIANT (LLD §5.7 — term-first): any inbound message whose Term
// exceeds n.currentTerm flows through maybeStepDownLocked BEFORE per-
// role handling. Per-role handlers MUST NOT inline term checks; this
// is the only place that implements the funnel (ELEC-07 + ADR-0008).
// Drift here would re-introduce P0-5 (TOCTOU step-down) because two
// independent term checks always race with a concurrent role change.
//
// Dispatch is exhaustive over MessageType + Role; unknown MessageType
// values return a wrapped error rather than panicking so a wire-level
// drift (Phase 6 widens the type set) surfaces as a recoverable
// condition rather than crashing the driver loop.
func (n *node) stepLocked(m Message) error {
	if m.Term > n.currentTerm {
		n.maybeStepDownLocked(m.Term)
	}
	switch m.Type {
	case MsgTick:
		switch n.role {
		case Follower:
			n.tickFollowerLocked()
		case Candidate:
			n.tickCandidateLocked()
		case Leader:
			n.tickLeaderLocked()
		}
	case MsgRequestVote:
		n.handleRequestVoteLocked(m)
	case MsgRequestVoteResponse:
		n.handleRequestVoteResponseLocked(m)
	case MsgAppendEntries:
		n.handleAppendEntriesLocked(m)
	case MsgAppendEntriesResp:
		n.handleAppendEntriesRespLocked(m)
	default:
		return fmt.Errorf("raft: stepLocked: unknown MessageType %d", m.Type)
	}
	return nil
}

// Per-role handler skeletons. Each MUST be declared so stepLocked's
// switch is exhaustive at compile time and so plans 05-02 / 05-03 /
// Phase 6 can land independently in parallel without crashing each
// other's tests. Bodies are no-ops; the TODO comments name the plan
// that fills each.
//
// Leader-role handlers (tickLeaderLocked, handleAppendEntriesRespLocked)
// are not exercised by any Phase 5 test path — becomeLeader (05-03)
// transitions to Leader but only after wiring its own tick loop and
// outbound AE pipeline lands in Phase 6.
// tickCandidateLocked + handleRequestVoteResponseLocked: filled by 05-03
// in pkg/raft/candidate.go.
// tickFollowerLocked + handleRequestVoteLocked + handleAppendEntriesLocked:
// filled by 05-02 in pkg/raft/follower.go.
// becomeCandidateLocked / becomeLeaderLocked: filled by 05-03 in
// pkg/raft/candidate.go and pkg/raft/leader_stub.go.
func (n *node) tickLeaderLocked()                     { /* TODO(Phase 6 — leader.go) */ }
func (n *node) handleAppendEntriesRespLocked(Message) { /* TODO(Phase 6) */ }

// queueMsgLocked attaches the current stepDownEpoch to an outbound
// Message and appends it to the Ready() drain buffer. The epoch token
// lets Ready() (plan 05-04) discard messages queued under a prior role
// — ELEC-08 / P0-5. Caller MUST hold n.mu.
func (n *node) queueMsgLocked(m Message) {
	n.pendingMsgs = append(n.pendingMsgs, pendingMsg{epoch: n.stepDownEpoch, msg: m})
}

// queueHardStateLocked snapshots (currentTerm, votedFor, commitIndex)
// so the driver can persist it on the next Ready() before shipping any
// pending Message (SC5 ordering). Last-writer-wins per step — repeated
// calls within one stepLocked overwrite the same buffer. Caller MUST
// hold n.mu.
func (n *node) queueHardStateLocked() {
	hs := HardState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Commit:      n.commitIndex,
	}
	n.pendingHS = &hs
}
