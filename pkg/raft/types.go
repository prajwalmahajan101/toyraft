// Package raft implements the ToyRaft consensus core.
//
// Zero-value convention: every exported type in this package is either
// useful at its zero value (e.g. Log{}, HardState{}, Role(0)=Follower)
// or, if a method cannot operate without construction, panics with a
// message of the form "raft.<Type>: zero-value <Type>; construct via raft.New".
// See docs/LLD.md Global Invariant 2.
package raft

// NodeID is a stable, human-readable identifier for a cluster member.
// Must be non-empty, unique across Config.Peers, and stable across restarts.
type NodeID string

// Term is Raft's monotonically increasing logical clock.
type Term uint64

// Index is a 1-based position in the replicated log. Index 0 is reserved
// for the implicit "before the first entry" sentinel used by AppendEntries
// consistency checks.
type Index uint64

// Role is a Raft server state.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

// Entry is one replicated log record.
//
// Invariants:
//   - Term is the term in which the leader created the entry.
//   - Index is 1-based and strictly increasing within a log.
//   - Data is opaque to Raft; the StateMachine defines its meaning.
//   - Once an Entry at (Term, Index) is committed, no other Entry with the
//     same Index may ever be committed (Log Matching, Raft §5.3).
type Entry struct {
	Term  Term
	Index Index
	Data  []byte
}

// MessageType discriminates Raft RPCs.
//
// Wire-visible values 0..3 are frozen by docs/WIRE.md and append-only;
// renumbering is a wire break.
//
// Tick is internal-only — it drives the core state machine from the
// driver's tick loop and MUST NOT appear on any Transport.Send call.
type MessageType uint8

const (
	MsgRequestVote         MessageType = 0
	MsgRequestVoteResponse MessageType = 1
	MsgAppendEntries       MessageType = 2
	MsgAppendEntriesResp   MessageType = 3

	MsgTick MessageType = 255 // internal-only; not wire-visible
)

// Message is the on-the-wire and in-process Raft RPC envelope.
//
// Invariants:
//   - Type, Term, From, To are always set (Tick excepted, which has only Type).
//   - Receivers MUST treat Term > currentTerm as a step-down trigger
//     BEFORE inspecting any other field (Raft §5.1).
//   - The sender MUST NOT mutate a Message after handing it to Transport.Send;
//     transports may queue or fan-out asynchronously.
//   - Entries slices are shared by reference within a process; consumers MUST
//     copy before mutating.
type Message struct {
	Type MessageType
	Term Term
	From NodeID
	To   NodeID

	// RequestVote / RequestVoteResponse
	LastLogIndex Index // RequestVote: candidate's last log index
	LastLogTerm  Term  // RequestVote: candidate's last log term
	VoteGranted  bool  // RequestVoteResponse: vote outcome

	// AppendEntries / AppendEntriesResponse
	PrevLogIndex Index   // AE: index immediately preceding Entries
	PrevLogTerm  Term    // AE: term of log[PrevLogIndex]
	Entries      []Entry // AE: entries to append (empty for heartbeat)
	LeaderCommit Index   // AE: leader's known commit index
	Success      bool    // AE response: consistency-check outcome
	MatchIndex   Index   // AE response: follower's last matching index

	// Fast-rollback hints (Ongaro §5.3 optimisation)
	ConflictTerm  Term  // AE response: term at the divergence point, or 0
	ConflictIndex Index // AE response: first index with ConflictTerm, or
	//              follower's lastIndex+1 if its log is too short
}

// HardState is the durably-persisted slice of Raft state.
//
// Invariants:
//   - Storage.SaveHardState MUST fsync before returning.
//   - A node MUST NOT emit any RPC that depends on a (Term, VotedFor) pair
//     until that pair is durable (REPL-09).
//   - Commit is persisted for fast restart but is also recoverable from the log;
//     loss of Commit on crash is safe but slower to converge.
type HardState struct {
	CurrentTerm Term
	VotedFor    NodeID // empty when no vote cast in CurrentTerm
	Commit      Index  // best-effort; may be stale after crash
}
