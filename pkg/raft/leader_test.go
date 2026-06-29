package raft

import (
	"testing"
)

// driveToLeader stands up a node from makeElectionConfig, forces the
// election-timeout path to promote it to Candidate, then feeds enough
// vote grants to reach quorum and flip to Leader. It drains the Ready()
// buffer (the becomeCandidate fan-out + becomeLeader immediate heartbeat
// round) and returns the *node plus a TestNode wrapper for the SC5
// inspectors.
//
// The peers slice MUST contain id at index 0 (makeElectionConfig uses
// peers[0]-style ids). Seed 42 pins the per-node RNG draw inside the
// [3,6)-tick window deterministically (P1-4).
func driveToLeader(t *testing.T, id NodeID, peers []NodeID) (*node, *TestNode) {
	t.Helper()
	cfg := makeElectionConfig(t, id, peers, 42)
	n := mustNewNode(t, cfg)

	// Force the election timeout so the next tick promotes to Candidate.
	n.mu.Lock()
	n.electionElapsed = n.electionTimeout
	n.mu.Unlock()
	if err := n.Step(Message{Type: MsgTick}); err != nil {
		t.Fatalf("Step tick (election): %v", err)
	}
	if n.role != Candidate {
		t.Fatalf("setup: role=%v after election timeout; want Candidate", n.role)
	}
	candTerm := n.currentTerm

	// Feed grants from peers (other than self) until quorum flips us to
	// Leader. quorum() already counts the self-vote.
	for _, peer := range peers {
		if peer == id {
			continue
		}
		if n.role == Leader {
			break
		}
		if err := n.Step(Message{
			Type:        MsgRequestVoteResponse,
			Term:        candTerm,
			From:        peer,
			To:          id,
			VoteGranted: true,
		}); err != nil {
			t.Fatalf("Step grant from %s: %v", peer, err)
		}
	}
	if n.role != Leader {
		t.Fatalf("setup: role=%v after quorum grants; want Leader", n.role)
	}
	// Drain the becomeCandidate fan-out + becomeLeader immediate heartbeat.
	n.Ready()
	return n, &TestNode{n: n}
}

// countAppendEntriesPerPeer drains Ready() once and tallies the
// MsgAppendEntries emitted per destination peer, plus how many of those
// carried no entries (heartbeats).
func countAppendEntriesPerPeer(msgs []Message) (perPeer map[NodeID]int, heartbeats int) {
	perPeer = map[NodeID]int{}
	for _, m := range msgs {
		if m.Type != MsgAppendEntries {
			continue
		}
		perPeer[m.To]++
		if len(m.Entries) == 0 {
			heartbeats++
		}
	}
	return perPeer, heartbeats
}

// TestHeartbeatCadence — SC1 / REPL-01. A leader on the default 150/50
// timings emits at least 3 heartbeats per peer over one ElectionTimeoutMin
// window (ElectionTimeoutMin/tickInterval == 3 ticks), giving a
// heartbeat:election ratio >= 3x. With heartbeatTimeout==1 (one heartbeat
// per tick) and no client proposals, every emitted AppendEntries is an
// empty-Entries heartbeat.
func TestHeartbeatCadence(t *testing.T) {
	t.Parallel()
	peers := []NodeID{"n1", "n2", "n3"}
	n, _ := driveToLeader(t, "n1", peers)

	// One ElectionTimeoutMin window in ticks. tickInterval()==HeartbeatInterval,
	// so this is ElectionTimeoutMin/HeartbeatInterval == 150/50 == 3.
	cfg := n.cfg
	windowTicks := int(cfg.ElectionTimeoutMin / cfg.tickInterval())
	if windowTicks < 3 {
		t.Fatalf("window=%d ticks; default timings must give >= 3 (SC1 ratio)", windowTicks)
	}

	perPeer := map[NodeID]int{}
	heartbeats := 0
	for i := 0; i < windowTicks; i++ {
		if err := n.Step(Message{Type: MsgTick}); err != nil {
			t.Fatalf("Step tick %d: %v", i, err)
		}
		msgs, _ := n.Ready()
		pp, hb := countAppendEntriesPerPeer(msgs)
		for peer, c := range pp {
			perPeer[peer] += c
		}
		heartbeats += hb
	}

	// Every peer except self must have received >= 3 AppendEntries.
	for _, peer := range peers {
		if peer == "n1" {
			if _, ok := perPeer[peer]; ok {
				t.Errorf("leader sent AppendEntries to self %q (Pitfall 8 regression)", peer)
			}
			continue
		}
		if got := perPeer[peer]; got < 3 {
			t.Errorf("peer %q received %d heartbeats over %d-tick window; want >= 3 (SC1)", peer, got, windowTicks)
		}
	}
	// All emissions were empty-Entries heartbeats (no proposals issued).
	total := 0
	for _, c := range perPeer {
		total += c
	}
	if heartbeats != total {
		t.Errorf("non-heartbeat AppendEntries emitted: %d of %d carried entries", total-heartbeats, total)
	}
}

// TestNewLeaderInit — SC5 / REPL-05. On becoming leader, nextIndex[peer]
// must equal log.LastIndex()+1 and matchIndex[peer] must be 0 for every
// peer (including self — becomeLeaderLocked sets the uniform map shape).
// Asserted through the widened TestNode inspectors.
func TestNewLeaderInit(t *testing.T) {
	t.Parallel()
	peers := []NodeID{"n1", "n2", "n3", "n4", "n5"}
	n, tn := driveToLeader(t, "n1", peers)

	wantNext := n.log.LastIndex() + 1
	next := tn.NextIndex()
	match := tn.MatchIndex()
	for _, peer := range peers {
		if got := next[peer]; got != wantNext {
			t.Errorf("nextIndex[%s]=%d; want %d (REPL-05)", peer, got, wantNext)
		}
		if got := match[peer]; got != 0 {
			t.Errorf("matchIndex[%s]=%d; want 0 (REPL-05)", peer, got)
		}
	}
}

// TestFreshLeaderDoesNotCommit — SC5 / P1-2. A fresh leader that has
// received zero AppendEntries responses must NOT advance commitIndex: a
// lone 1-of-N self match is below quorum, so nothing commits. This proves
// the P1-2 guard holds before commit.go exists; the actual commit advance
// is exercised in 06-03.
func TestFreshLeaderDoesNotCommit(t *testing.T) {
	t.Parallel()
	peers := []NodeID{"n1", "n2", "n3"}
	n, tn := driveToLeader(t, "n1", peers)

	if got := tn.CommitIndex(); got != 0 {
		t.Fatalf("fresh leader commitIndex=%d before any replication; want 0 (P1-2)", got)
	}
	// Tick several times (heartbeats only, no responses) — still no commit.
	for i := 0; i < 5; i++ {
		if err := n.Step(Message{Type: MsgTick}); err != nil {
			t.Fatalf("Step tick %d: %v", i, err)
		}
		n.Ready()
	}
	if got := tn.CommitIndex(); got != 0 {
		t.Fatalf("commitIndex=%d after %d ticks with no AE responses; want 0 (P1-2)", got, 5)
	}
}
