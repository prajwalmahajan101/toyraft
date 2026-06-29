//go:build raftdebug
// +build raftdebug

package raft

import "fmt"

// assertReadyInvariantsLocked panics if the SC5 layer-2 invariant is
// violated. Caller holds n.mu (Ready acquires it before invoking).
//
// Invariant (ELEC-05 / SC5 / PITFALLS P0-4):
//
//	If pendingHS != nil, then for every pendingMsg whose Message is a
//	MsgRequestVoteResponse with VoteGranted == true and Term ==
//	pendingHS.CurrentTerm, the response's To field (the candidate the
//	vote was granted to) MUST equal pendingHS.VotedFor.
//
// Rationale: handleRequestVoteLocked constructs the (HardState, grant
// response) pair atomically inside a single stepLocked call. A future
// refactor that separates the HardState queue from the response queue
// could allow a grant for candidate A to slip through alongside a
// HardState that records VotedFor=B, which would violate Raft §5.2's
// "one vote per term" rule the moment the driver fsynced and shipped
// in that order. This assertion catches that drift at the Ready()
// boundary before the bad pair can leave the process.
//
// The assertion is intentionally a panic, not a t.Fatal: the raftdebug
// build is meant to fail loudly under -race -count=N stress so the
// regression is impossible to silently ship.
func assertReadyInvariantsLocked(n *node) {
	assertAppendPrecedesAEResponseLocked(n)

	if n.pendingHS == nil {
		return
	}
	hsTerm := n.pendingHS.CurrentTerm
	hsVotedFor := n.pendingHS.VotedFor
	for _, pm := range n.pendingMsgs {
		m := pm.msg
		if m.Type != MsgRequestVoteResponse {
			continue
		}
		if !m.VoteGranted {
			continue
		}
		if m.Term != hsTerm {
			continue
		}
		if hsVotedFor != m.To {
			panic(fmt.Sprintf(
				"raft: ready invariant: pendingHS.VotedFor=%q but grant response.To=%q at term %d",
				hsVotedFor, m.To, hsTerm,
			))
		}
	}
}

// assertAppendPrecedesAEResponseLocked panics if the P0-4-final layer-2
// invariant is violated. Caller holds n.mu (Ready acquires it).
//
// Invariant (REPL-09 extended / PITFALLS P0-4 / RESEARCH Pattern 5):
//
//	If the pending Ready batch contains a MsgAppendEntriesResp with
//	Success == true and MatchIndex == M (M > 0), then the node's own log
//	MUST already reflect entries up to M, i.e. n.log.LastIndex() >= M at
//	the Ready boundary.
//
// Rationale: a Success AE response advertises to the leader that the
// follower has durably accepted entries up to MatchIndex. The follower's
// AppendEntries receiver appends to n.log AND mirrors into n.storage
// BEFORE queuing the Success response (plan 06-04 / RATIFIED decision 2).
// If a Success response ever claimed a MatchIndex the node's log does not
// cover, the follower would be advertising durability it had not achieved
// — the P0-4 failure mode. This assertion catches that drift at the
// Ready() boundary before the bad response can leave the process.
//
// Like the vote-grant invariant this is a panic, not a t.Fatal: the
// raftdebug build is meant to fail loudly under -race -count=N stress so
// the regression is impossible to silently ship.
func assertAppendPrecedesAEResponseLocked(n *node) {
	last := n.log.LastIndex()
	for _, pm := range n.pendingMsgs {
		m := pm.msg
		if m.Type != MsgAppendEntriesResp || !m.Success || m.MatchIndex == 0 {
			continue
		}
		if m.MatchIndex > last {
			panic(fmt.Sprintf(
				"raft: ready invariant: MsgAppendEntriesResp Success=true MatchIndex=%d "+
					"exceeds log.LastIndex()=%d (P0-4: advertising entries the node has not appended)",
				m.MatchIndex, last,
			))
		}
	}
}
