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
// switch is exhaustive at compile time and so plans could land
// independently in parallel without crashing each other's tests.
//
// tickLeaderLocked (the leader heartbeat/append fan-out, REPL-01) is now
// REAL — it lives in pkg/raft/leader.go (06-01). handleAppendEntriesRespLocked
// remains a no-op stub until plan 06-03 wires per-peer progress + the
// commit rule.
// tickCandidateLocked + handleRequestVoteResponseLocked: 05-03 (candidate.go).
// tickFollowerLocked + handleRequestVoteLocked + handleAppendEntriesLocked:
// 05-02 / 06-02 (follower.go / append_entries.go).
// becomeCandidateLocked / becomeLeaderLocked: candidate.go / leader_stub.go.
func (n *node) handleAppendEntriesRespLocked(Message) { /* TODO(Phase 6 — 06-03) */ }

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
