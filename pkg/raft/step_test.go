package raft

import (
	"errors"
	"strings"
	"testing"
)

// fakeStorage is a minimal in-test Storage implementation. We CANNOT
// import pkg/storage/memory from this white-box _test file (import
// cycle: pkg/storage/memory -> pkg/storage -> pkg/raft). The real
// storage backends are exercised by the package-external integration
// tests landed in Phase 7; here we only need a non-nil Storage so
// Config.Validate passes.
type fakeStorage struct{}

func (fakeStorage) Append([]Entry) error                  { return nil }
func (fakeStorage) TruncateSuffix(Index) error            { return nil }
func (fakeStorage) Entries(Index, Index) ([]Entry, error) { return nil, nil }
func (fakeStorage) Term(Index) (Term, error)              { return 0, nil }
func (fakeStorage) FirstIndex() (Index, error)            { return 1, nil }
func (fakeStorage) LastIndex() (Index, error)             { return 0, nil }
func (fakeStorage) SaveHardState(HardState) error         { return nil }
func (fakeStorage) LoadHardState() (HardState, error)     { return HardState{}, nil }
func (fakeStorage) Snapshot() ([]byte, Index, error)      { return nil, 0, ErrSnapshotUnsupported }
func (fakeStorage) Restore([]byte) error                  { return ErrSnapshotUnsupported }

// newTestNode constructs a *node for tests via newNode and then flips
// started=false so callers can exercise the pre-start ErrStopped guard
// (Pitfall 6). 05-02 extended newNode to set started=true after the
// LoadHardState + RNG bootstrap, so the helper must walk that flag
// back for tests that care about the pre-start path.
func newTestNode(t *testing.T, peers []NodeID) *node {
	t.Helper()
	n, err := newNode(&Config{
		ID:      peers[0],
		Peers:   peers,
		Storage: fakeStorage{},
	})
	if err != nil {
		t.Fatalf("newNode: %v", err)
	}
	n.started = false
	return n
}

// newStartedNode returns a node with started=true so Step proceeds to
// stepLocked. After 05-02 this is just newNode (started=true by
// construction), but we keep the helper so existing call sites read
// the same.
func newStartedNode(t *testing.T, peers []NodeID) *node {
	t.Helper()
	n, err := newNode(&Config{
		ID:      peers[0],
		Peers:   peers,
		Storage: fakeStorage{},
	})
	if err != nil {
		t.Fatalf("newNode: %v", err)
	}
	return n
}

// TestStepReturnsErrStoppedPreStart proves Pitfall 6's guard: a Step
// call against a freshly-constructed node (started=false) returns
// ErrStopped rather than racing against an unloaded HardState.
func TestStepReturnsErrStoppedPreStart(t *testing.T) {
	t.Parallel()
	n := newTestNode(t, []NodeID{"n1"})
	if err := n.Step(Message{Type: MsgTick}); !errors.Is(err, ErrStopped) {
		t.Fatalf("Step pre-start: got %v, want ErrStopped", err)
	}
}

// TestMaybeStepDownBumpsEpochAndClearsState locks the term-first
// invariant (LLD §5.7): a higher Term observed by stepLocked MUST
// funnel through maybeStepDownLocked, which clears candidate state,
// drops to Follower, bumps stepDownEpoch, and queues a HardState
// before per-role dispatch (ADR-0008 / P0-5 / ELEC-08).
func TestMaybeStepDownBumpsEpochAndClearsState(t *testing.T) {
	t.Parallel()
	n := newStartedNode(t, []NodeID{"n1", "n2", "n3"})

	// Hand-set a candidate-mid-term shape.
	n.currentTerm = 3
	n.votedFor = "n1"
	n.role = Candidate
	n.stepDownEpoch = 7
	n.votesReceived = map[NodeID]bool{"n1": true}

	if err := n.Step(Message{Type: MsgAppendEntries, Term: 5, From: "n2", To: "n1"}); err != nil {
		t.Fatalf("Step: unexpected error %v", err)
	}

	if n.role != Follower {
		t.Errorf("role: got %v, want Follower", n.role)
	}
	if n.currentTerm != 5 {
		t.Errorf("currentTerm: got %d, want 5", n.currentTerm)
	}
	if n.votedFor != "" {
		t.Errorf("votedFor: got %q, want empty", n.votedFor)
	}
	if n.stepDownEpoch != 8 {
		t.Errorf("stepDownEpoch: got %d, want 8", n.stepDownEpoch)
	}
	if n.votesReceived != nil {
		t.Errorf("votesReceived: got %v, want nil", n.votesReceived)
	}
	// maybeStepDownLocked clears leaderHint; handleAppendEntriesLocked
	// (landed by 05-02) then installs m.From as the new leader because
	// the AE proves there IS a leader at the new term. This is the
	// correct Raft Figure-2 receiver behaviour — the prior assertion
	// (leaderHint=="" after step-down via AE) reflected the 05-01-era
	// no-op handler.
	if n.leaderHint != "n2" {
		t.Errorf("leaderHint: got %q, want %q (handleAppendEntriesLocked installs m.From)", n.leaderHint, "n2")
	}
	if n.pendingHS == nil {
		t.Fatalf("pendingHS: got nil, want queued HardState")
	}
	if n.pendingHS.CurrentTerm != 5 {
		t.Errorf("pendingHS.CurrentTerm: got %d, want 5", n.pendingHS.CurrentTerm)
	}
	if n.pendingHS.VotedFor != "" {
		t.Errorf("pendingHS.VotedFor: got %q, want empty", n.pendingHS.VotedFor)
	}
}

// TestStepUnknownMessageTypeReturnsError proves stepLocked's default
// branch wraps unknown MessageType into a recoverable error (ADR-0008
// — drift surfaces as a returned error, never a panic in the driver
// loop).
func TestStepUnknownMessageTypeReturnsError(t *testing.T) {
	t.Parallel()
	n := newStartedNode(t, []NodeID{"n1"})

	err := n.Step(Message{Type: MessageType(99)})
	if err == nil {
		t.Fatalf("Step(unknown): got nil error, want wrapped 'unknown MessageType'")
	}
	if !strings.Contains(err.Error(), "unknown MessageType") {
		t.Errorf("Step(unknown): error %q does not contain 'unknown MessageType'", err.Error())
	}
}

// TestStepEqualTermDoesNotBumpEpoch confirms the guard inside
// maybeStepDownLocked: an equal-term observation MUST NOT bump
// stepDownEpoch (would invalidate every legitimate in-flight
// candidate message on every received vote).
func TestStepEqualTermDoesNotBumpEpoch(t *testing.T) {
	t.Parallel()
	n := newStartedNode(t, []NodeID{"n1", "n2"})
	n.currentTerm = 3
	n.role = Candidate
	n.stepDownEpoch = 4

	if err := n.Step(Message{Type: MsgRequestVoteResponse, Term: 3, From: "n2", To: "n1"}); err != nil {
		t.Fatalf("Step: %v", err)
	}

	if n.stepDownEpoch != 4 {
		t.Errorf("stepDownEpoch: got %d, want 4 (no bump on equal term)", n.stepDownEpoch)
	}
	if n.role != Candidate {
		t.Errorf("role: got %v, want Candidate (no step-down on equal term)", n.role)
	}
}

// TestStepLowerTermDoesNotBumpEpoch is the symmetric guard: a strictly
// lower Term observed (stale RPC from a partitioned peer) MUST NOT
// step down.
func TestStepLowerTermDoesNotBumpEpoch(t *testing.T) {
	t.Parallel()
	n := newStartedNode(t, []NodeID{"n1", "n2"})
	n.currentTerm = 5
	n.role = Leader
	n.stepDownEpoch = 9

	if err := n.Step(Message{Type: MsgAppendEntries, Term: 3, From: "n2", To: "n1"}); err != nil {
		t.Fatalf("Step: %v", err)
	}

	if n.stepDownEpoch != 9 {
		t.Errorf("stepDownEpoch: got %d, want 9 (no bump on lower term)", n.stepDownEpoch)
	}
	if n.role != Leader {
		t.Errorf("role: got %v, want Leader (no step-down on lower term)", n.role)
	}
	if n.currentTerm != 5 {
		t.Errorf("currentTerm: got %d, want 5 (untouched)", n.currentTerm)
	}
}

// TestStepTickDispatchesByRole confirms the inner switch on n.role.
// We cannot easily observe the no-op handlers, but we CAN verify they
// do not error / panic and do not leak across roles.
func TestStepTickDispatchesByRole(t *testing.T) {
	t.Parallel()
	roles := []Role{Follower, Candidate, Leader}
	for _, r := range roles {
		n := newStartedNode(t, []NodeID{"n1"})
		n.role = r
		if err := n.Step(Message{Type: MsgTick}); err != nil {
			t.Errorf("role=%v: MsgTick returned %v, want nil", r, err)
		}
		if n.role != r {
			t.Errorf("role=%v: role mutated to %v after MsgTick (stubs must be no-ops)", r, n.role)
		}
	}
}

// TestConfigValidate covers the five Validate failure modes plus the
// happy path.
func TestConfigValidate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"happy path", func(c *Config) {}, false},
		{"empty ID", func(c *Config) { c.ID = "" }, true},
		{"nil storage", func(c *Config) { c.Storage = nil }, true},
		{"self not in peers", func(c *Config) { c.Peers = []NodeID{"n2", "n3"} }, true},
		{"min >= max", func(c *Config) { c.ElectionTimeoutMin = 600; c.ElectionTimeoutMax = 600 }, true},
		{"heartbeat margin violated", func(c *Config) { c.HeartbeatInterval = 200; c.ElectionTimeoutMin = 300 }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := &Config{ID: "n1", Peers: []NodeID{"n1"}, Storage: fakeStorage{}}
			c.applyDefaults()
			tc.mutate(c)
			err := c.Validate()
			if tc.wantErr && err == nil {
				t.Fatalf("Validate: got nil, want error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("Validate: got %v, want nil", err)
			}
			if tc.wantErr && !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Validate: err %v does not wrap ErrInvalidConfig", err)
			}
		})
	}
}

// TestNewNodeSortsPeers verifies the C-8 guard: newNode clones and
// sorts the peer slice via slices.Sort so iteration order is platform-
// independent.
func TestNewNodeSortsPeers(t *testing.T) {
	t.Parallel()
	n, err := newNode(&Config{
		ID:      "n2",
		Peers:   []NodeID{"n3", "n1", "n2"},
		Storage: fakeStorage{},
	})
	if err != nil {
		t.Fatalf("newNode: %v", err)
	}
	want := []NodeID{"n1", "n2", "n3"}
	if len(n.peers) != len(want) {
		t.Fatalf("peers len: got %d, want %d", len(n.peers), len(want))
	}
	for i := range want {
		if n.peers[i] != want[i] {
			t.Errorf("peers[%d]: got %q, want %q", i, n.peers[i], want[i])
		}
	}
}
