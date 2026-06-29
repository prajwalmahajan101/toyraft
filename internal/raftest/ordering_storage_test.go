package raftest_test

import (
	"testing"

	"github.com/prajwalmahajan101/toyraft/internal/raftest"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	"github.com/prajwalmahajan101/toyraft/pkg/storage/memory"
)

// TestVotePersistsBeforeResponse — SC5 layer-3 (the assertion-enforced
// proof), POSITIVE case. Records the correct driver sequence directly into
// OrderingStorage — SaveHardState{Term=1, VotedFor=n2} BEFORE
// Send(VoteGranted=true, Term=1, To=n2):
//
//	SaveHardState(hs) -> RecordSend(grant)
//
// AssertHardStatePrecedesVoteGrantedResponse MUST pass.
//
// 07-04 NOTE: this previously drove a raft.TestNode's Step/Ready to GENERATE
// the (hs, grant) pair. TestNode is deleted in Phase 7 (the public surface
// has no Ready() drain seam), and the production driver's SaveHardState-
// before-Send ordering is now proven by pkg/raft's own driver_test.go. This
// test's job is narrower — it pins the OrderingStorage assertion itself — so
// it records the canonical Save->Send pair directly (mirroring the negative
// case in TestOrderingStorageDetectsViolation, opposite order).
func TestVotePersistsBeforeResponse(t *testing.T) {
	t.Parallel()

	mem := memory.New()
	ord := raftest.NewOrderingStorage(mem)

	// Correct order: persist the vote (Term=1, VotedFor=n2) BEFORE shipping
	// the granted response to n2.
	if err := ord.SaveHardState(raft.HardState{CurrentTerm: 1, VotedFor: "n2"}); err != nil {
		t.Fatalf("SaveHardState: %v", err)
	}
	ord.RecordSend(raft.Message{
		Type:        raft.MsgRequestVoteResponse,
		Term:        1,
		From:        "n1",
		To:          "n2",
		VoteGranted: true,
	})

	// SC5 layer-3 assertion. Fails the test if the event log shows the grant
	// response without a prior matching SaveHardState.
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
