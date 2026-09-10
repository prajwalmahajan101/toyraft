package raft

import (
	"testing"
)

// TestRestartRestoresLogAndAcceptsWrites is the B1 gating regression test
// (ADR-0024). It writes N entries into a durable Storage, drops the node, then
// reconstructs a fresh node from the SAME Storage and asserts (a) the restored
// log reports LastLogIndex==N (before the fix it read 0 while CommitIndex==N),
// and (b) a subsequent leader proposal appends at N+1 and the durable Append
// succeeds (before the fix it appended at 1 over the persisted 1..N and Propose
// returned ErrProposalDropped).
//
// Reuses the append-backed memStorage double from driver_test.go.
func TestRestartRestoresLogAndAcceptsWrites(t *testing.T) {
	t.Parallel()
	const N = 3

	// Seed a durable Storage as if a leader at term 1 had committed N entries.
	st := &memStorage{}
	seed := make([]Entry, 0, N)
	for i := Index(1); i <= N; i++ {
		seed = append(seed, Entry{Term: 1, Index: i, Data: []byte{byte(i)}})
	}
	if err := st.Append(seed); err != nil {
		t.Fatalf("seed Append: %v", err)
	}
	if err := st.SaveHardState(HardState{CurrentTerm: 1, VotedFor: "n0", Commit: N}); err != nil {
		t.Fatalf("seed SaveHardState: %v", err)
	}

	cfg := Config{
		NodeID:       "n0",
		Peers:        []NodeID{"n0"}, // single-node (odd N=1)
		Storage:      st,
		Transport:    nopTransport{},
		StateMachine: nopSM{},
		Seed:         1,
	}

	// (a) Public surface: a reconstructed node reports the restored log length.
	node, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := node.Status().LastLogIndex; got != N {
		t.Fatalf("Status().LastLogIndex after restart = %d, want %d (B1: log not restored)", got, N)
	}
	if got := node.Status().CommitIndex; got != N {
		t.Fatalf("Status().CommitIndex after restart = %d, want %d", got, N)
	}

	// (b) White-box: a restored leader appends at N+1 and the durable mirror
	// succeeds (no ErrProposalDropped). proposeLocked is the exact path Propose
	// drives once leadership is held.
	core := node.(*nodeImpl).core
	core.mu.Lock()
	core.role = Leader
	core.currentTerm = 2 // a fresh leader term after restart
	idx, ok := core.proposeLocked([]byte("post-restart write"))
	core.mu.Unlock()
	if !ok {
		t.Fatalf("proposeLocked after restart returned !ok (B1: would be ErrProposalDropped)")
	}
	if idx != N+1 {
		t.Fatalf("proposeLocked appended at %d, want %d", idx, N+1)
	}
	if last, _ := st.LastIndex(); last != N+1 {
		t.Fatalf("durable Storage LastIndex after write = %d, want %d", last, N+1)
	}
	if err := node.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
