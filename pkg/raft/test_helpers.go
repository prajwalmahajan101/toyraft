package raft

// This file ships an exported, test-only handle around the unexported
// *node. It exists so internal/raftest (and only internal/raftest) can
// drive Step + Ready against a real raft state machine while the
// public raft.Node interface is still on the Phase-7 docket.
//
// NOT FOR PRODUCTION USE. Phase 7 lands the canonical raft.New /
// raft.Node surface in pkg/raft/node_public.go; at that point this file
// is deleted and internal/raftest is rewired to the public surface. The
// helper lives in a non-_test.go file because internal/raftest needs to
// reference it at runtime (not just from its own _test.go files —
// raftest exports its own helpers consumed by other packages' tests).
//
// Scope: NewTestNode + the two methods (Step, Ready) the test driver
// needs to land the SC5 OrderingStorage assertion. No other *node
// surface is exposed; tests that need deeper access continue to live
// inside pkg/raft itself.
//
// Phase-6 widening (06-01): the handle grows a client-proposal hook
// (Propose -> proposeLocked, REPL-02) and read-only progress inspectors
// (Log, CommitIndex, MatchIndex, NextIndex) so plans 06-02..06-05 can
// inject entries into a real leader and assert replication/commit
// progress without reaching into the unexported *node. Every method
// takes n.mu (like RoleAndTerm) and returns values / deep copies, never
// live references. This handle is still deleted in Phase 7 when raft.New /
// raft.Node land.

// TestNode is a thin exported wrapper around *node, restricted to the
// Step + Ready surface. Construction goes through NewTestNode.
//
// The wrapper is intentionally minimal: it exists to satisfy plan
// 05-04's SC5 OrderingStorage test driver, not to be a general-purpose
// raft handle.
type TestNode struct {
	n *node
}

// NewTestNode constructs a TestNode from cfg. The full newNode start
// sequence runs (applyDefaults -> Validate -> LoadHardState -> RNG
// seed -> initial election timeout -> wire election trigger ->
// started=true), so the returned TestNode is ready for Step events
// immediately.
//
// Returns the validation / storage error path of newNode unchanged so
// the test driver can assert on construction failure modes.
func NewTestNode(cfg *Config) (*TestNode, error) {
	n, err := newNode(cfg)
	if err != nil {
		return nil, err
	}
	return &TestNode{n: n}, nil
}

// Step delegates to the wrapped *node.Step. Safe for concurrent
// callers; serialised inside *node via n.mu.
func (t *TestNode) Step(m Message) error {
	return t.n.Step(m)
}

// Ready delegates to the wrapped *node.Ready. See pkg/raft/ready.go
// package doc for the persist-then-ship contract the caller MUST
// honour.
func (t *TestNode) Ready() (msgs []Message, hs *HardState) {
	return t.n.Ready()
}

// RoleAndTerm returns the current (Role, Term) under n.mu. Used by
// internal/raftest.Cluster.AssertAtMostOneLeaderPerTerm (SC6 / ELEC-10)
// and the leader-detection helpers (HasLeader, Leader).
//
// The snapshot is taken with the lock briefly held; callers receive
// values, not references, so subsequent state transitions on the
// node do not aliasing-mutate the returned pair.
func (t *TestNode) RoleAndTerm() (Role, Term) {
	t.n.mu.Lock()
	defer t.n.mu.Unlock()
	return t.n.role, t.n.currentTerm
}

// ID returns the configured NodeID. Stable for the lifetime of the
// node; safe to call without locking.
func (t *TestNode) ID() NodeID {
	return t.n.id
}

// Propose injects a client proposal into the wrapped node under n.mu and
// returns (assignedIndex, true) on a leader, or (0, false) otherwise
// (REPL-02). Phase-6 test hook; deleted in Phase 7 with the rest of
// TestNode.
func (t *TestNode) Propose(data []byte) (Index, bool) {
	t.n.mu.Lock()
	defer t.n.mu.Unlock()
	return t.n.proposeLocked(data)
}

// Log returns a deep copy of the wrapped node's replicated log entries
// under n.mu. The copy is owned by the caller; subsequent log mutation
// does not alias it.
func (t *TestNode) Log() []Entry {
	t.n.mu.Lock()
	defer t.n.mu.Unlock()
	out := make([]Entry, len(t.n.log.entries))
	copy(out, t.n.log.entries)
	return out
}

// CommitIndex returns the wrapped node's commitIndex under n.mu.
func (t *TestNode) CommitIndex() Index {
	t.n.mu.Lock()
	defer t.n.mu.Unlock()
	return t.n.commitIndex
}

// MatchIndex returns a clone of the wrapped node's matchIndex map under
// n.mu. The clone is owned by the caller.
func (t *TestNode) MatchIndex() map[NodeID]Index {
	t.n.mu.Lock()
	defer t.n.mu.Unlock()
	out := make(map[NodeID]Index, len(t.n.matchIndex))
	for k, v := range t.n.matchIndex {
		out[k] = v
	}
	return out
}

// NextIndex returns a clone of the wrapped node's nextIndex map under
// n.mu. The clone is owned by the caller.
func (t *TestNode) NextIndex() map[NodeID]Index {
	t.n.mu.Lock()
	defer t.n.mu.Unlock()
	out := make(map[NodeID]Index, len(t.n.nextIndex))
	for k, v := range t.n.nextIndex {
		out[k] = v
	}
	return out
}
