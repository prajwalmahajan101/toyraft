package raftest_test

import (
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/raftest"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	"github.com/prajwalmahajan101/toyraft/pkg/storage/memory"
)

// TestVotePersistsBeforeResponse — SC5 layer-3 (the assertion-enforced
// proof). Drives a vote grant through the test driver pattern that
// 05-05's Cluster.tickOnce will inherit:
//
//	Step(RequestVote) -> Ready() -> SaveHardState(hs) -> RecordSend(m)
//
// The OrderingStorage event log MUST show SaveHardState{Term=1,
// VotedFor=n2} at a strictly lower seq than Send(VoteGranted=true,
// Term=1, To=n2). AssertHardStatePrecedesVoteGrantedResponse panics
// via t.Fatalf if not.
func TestVotePersistsBeforeResponse(t *testing.T) {
	t.Parallel()

	mem := memory.New()
	ord := raftest.NewOrderingStorage(mem)

	cfg := &raft.Config{
		ID:                 "n1",
		Peers:              []raft.NodeID{"n1", "n2", "n3"},
		ElectionTimeoutMin: 300 * time.Millisecond,
		ElectionTimeoutMax: 600 * time.Millisecond,
		HeartbeatInterval:  100 * time.Millisecond,
		Seed:               42,
		Storage:            ord,
	}
	n, err := raft.NewTestNode(cfg)
	if err != nil {
		t.Fatalf("NewTestNode: %v", err)
	}

	// Inbound RequestVote at term 1 from n2 with an up-to-date log
	// (the granter's log is empty, so any (LastLogTerm, LastLogIndex)
	// satisfies §5.4.1).
	if err := n.Step(raft.Message{
		Type:         raft.MsgRequestVote,
		Term:         1,
		From:         "n2",
		To:           "n1",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}); err != nil {
		t.Fatalf("Step RequestVote: %v", err)
	}

	// Drain. Driver discipline (SC5 layer 1): persist hs BEFORE shipping.
	msgs, hs := n.Ready()
	if hs == nil {
		t.Fatalf("Ready: hs nil; vote grant must queue HardState before response")
	}
	if hs.VotedFor != "n2" {
		t.Fatalf("hs.VotedFor=%q, want %q", hs.VotedFor, raft.NodeID("n2"))
	}
	if err := ord.SaveHardState(*hs); err != nil {
		t.Fatalf("SaveHardState: %v", err)
	}

	if len(msgs) != 1 {
		t.Fatalf("Ready: len(msgs)=%d, want 1", len(msgs))
	}
	for _, m := range msgs {
		ord.RecordSend(m)
		// In production the driver would Send(m) here. The test does
		// not need a transport — RecordSend alone proves the ordering.
	}

	// SC5 layer-3 assertion. Fails the test if the event log shows
	// the grant response without a prior matching SaveHardState.
	ord.AssertHardStatePrecedesVoteGrantedResponse(t)
}

// TestOrderingStorageDetectsViolation pins the negative case: if the
// driver ships the grant response BEFORE persisting HardState, the
// assertion MUST flag it. We simulate the violation by recording the
// Send first and then the SaveHardState — opposite of correct order.
//
// Uses a sub-test with t.Run that we expect to fail; we verify the
// failure via a captured *testing.T proxy.
func TestOrderingStorageDetectsViolation(t *testing.T) {
	t.Parallel()

	mem := memory.New()
	ord := raftest.NewOrderingStorage(mem)

	// Send-then-save: the wrong order.
	ord.RecordSend(raft.Message{
		Type:        raft.MsgRequestVoteResponse,
		Term:        1,
		From:        "n1",
		To:          "n2",
		VoteGranted: true,
	})
	if err := ord.SaveHardState(raft.HardState{CurrentTerm: 1, VotedFor: "n2"}); err != nil {
		t.Fatalf("SaveHardState: %v", err)
	}

	// Use the testing.T-free form so we can assert the negative case
	// (a violation IS expected) without failing the surrounding test.
	if err := ord.CheckHardStatePrecedesVoteGrantedResponse(); err == nil {
		t.Fatalf("Check must flag the out-of-order sequence; got nil error")
	}
}

// TestAppendPrecedesAppendEntriesResponse — P0-4-final layer-3 positive
// case. A follower persists entries up to index M via Storage.Append
// BEFORE shipping a Success AppendEntries response claiming MatchIndex=M.
// The OrderingStorage event log MUST show the Append at a strictly lower
// seq than the Send, so the precedence check passes.
func TestAppendPrecedesAppendEntriesResponse(t *testing.T) {
	t.Parallel()

	mem := memory.New()
	ord := raftest.NewOrderingStorage(mem)

	// Persist entries covering index 2 (contiguous from index 1, as the
	// memory store requires).
	if err := ord.Append([]raft.Entry{
		{Term: 1, Index: 1, Data: []byte("a")},
		{Term: 1, Index: 2, Data: []byte("b")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Then ship the Success response claiming MatchIndex=2 (covered).
	ord.RecordSend(raft.Message{
		Type:       raft.MsgAppendEntriesResp,
		Term:       1,
		From:       "n1",
		To:         "n0",
		Success:    true,
		MatchIndex: 2,
	})

	ord.AssertAppendPrecedesAppendEntriesResponse(t)
}

// TestAppendPrecedesAppendEntriesResponse_Violation pins the negative
// case (Pitfall 8 — the assertion must not be vacuously true). A Success
// AppendEntries response claims MatchIndex=5 with NO prior Append covering
// index 5; the check MUST flag it. Proves the assertion actually catches
// an over-claiming response.
func TestAppendPrecedesAppendEntriesResponse_Violation(t *testing.T) {
	t.Parallel()

	mem := memory.New()
	ord := raftest.NewOrderingStorage(mem)

	// Send a Success response claiming MatchIndex=5 with no prior Append.
	ord.RecordSend(raft.Message{
		Type:       raft.MsgAppendEntriesResp,
		Term:       1,
		From:       "n1",
		To:         "n0",
		Success:    true,
		MatchIndex: 5,
	})

	if err := ord.CheckAppendPrecedesAppendEntriesResponse(); err == nil {
		t.Fatalf("Check must flag the over-claiming AE response; got nil error")
	}
}
