package raftest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	"github.com/prajwalmahajan101/toyraft/pkg/transport/inproc"
)

// Cluster is the N-node test fixture. Phase 4 wires up a no-op consensus
// node (noopNode below); Phase 5+ replaces noopNode with raft.Node.
// Cluster fields are exposed for tests to advance the clock, inspect the
// hub, or sample the recorder directly.
type Cluster struct {
	T        testing.TB
	N        int
	Seed     int64
	Clock    *clock.Fake
	Hub      *inproc.Hub
	Recorder *Recorder

	endpoints []*inproc.Endpoint // one per node, indexed 0..N-1
	nodeIDs   []raft.NodeID      // sorted; nodeIDs[i] is the ID for index i
	nodes     []*noopNode        // Phase 5 replaces with raft.Node
	cancel    context.CancelFunc
}

// noopNode is the Phase 4 consensus node. It drains its Endpoint's
// inbound channel and discards messages, and accepts Propose calls by
// echoing them back through the Recorder. No leader election, no log
// replication — Phase 5 replaces this type with raft.Node.
type noopNode struct {
	id  raft.NodeID
	ep  *inproc.Endpoint
	rec *Recorder
}

func newNoopNode(id raft.NodeID, ep *inproc.Endpoint, rec *Recorder) *noopNode {
	return &noopNode{id: id, ep: ep, rec: rec}
}

// drainOnce non-blockingly drains all available inbound messages.
// Called from Cluster.Tick after each Advance step to keep the Hub's
// receiver channels from blocking. Phase 5 replaces this with the
// raft.Node Step loop.
func (n *noopNode) drainOnce() {
	for {
		select {
		case <-n.ep.Recv():
			// Discard: Phase 4 has no consensus logic to feed.
		default:
			return
		}
	}
}

// NewCluster builds an N-node cluster on a shared FakeClock + Hub with
// the given seed. N must be odd and >= 3 (Raft quorum requirement). The
// Hub and Recorder are torn down via t.Cleanup; tests do not need to
// defer Close themselves.
func NewCluster(t testing.TB, n int, seed int64) *Cluster {
	t.Helper()
	if n < 3 || n%2 == 0 {
		t.Fatalf("raftest: N must be odd and >= 3; got %d", n)
	}

	clk := clock.NewFake()
	hub, err := inproc.NewHub(inproc.HubConfig{Clock: clk, Seed: seed})
	if err != nil {
		t.Fatalf("raftest: NewHub: %v", err)
	}
	rec := NewRecorder(clk)

	c := &Cluster{
		T:         t,
		N:         n,
		Seed:      seed,
		Clock:     clk,
		Hub:       hub,
		Recorder:  rec,
		nodeIDs:   make([]raft.NodeID, n),
		endpoints: make([]*inproc.Endpoint, n),
		nodes:     make([]*noopNode, n),
	}

	for i := range n {
		// Zero-padded node ID: "n00", "n01", ... so that the Hub's
		// lex-sorted sortedNodes iteration order matches numeric
		// index order for any N up to 100. RESEARCH lock.
		c.nodeIDs[i] = raft.NodeID(fmt.Sprintf("n%02d", i))
		c.endpoints[i] = hub.Connect(c.nodeIDs[i])
		c.nodes[i] = newNoopNode(c.nodeIDs[i], c.endpoints[i], rec)
	}
	_, c.cancel = context.WithCancel(context.Background())

	t.Cleanup(c.Close)
	return c
}

// Tick advances the FakeClock by d AND synchronously drains each node's
// inbound channel. RESEARCH Open Question 3 resolution: the dispatcher
// pumps inside Tick — no background goroutine drives chaos delivery.
// Drain order is by ascending index, which equals ascending node ID
// thanks to the zero-padded n00/n01/... encoding.
func (c *Cluster) Tick(d time.Duration) {
	c.Clock.Advance(d)
	for _, n := range c.nodes {
		n.drainOnce()
	}
}

// Propose records a client operation against node idx and returns the
// resulting HistoryEvent. Phase 4 semantics: every propose is "applied"
// instantly and returns a synthetic OK result with zero elapsed logical
// time. Phase 5 wires this through raft.Node.Propose with the real
// commit-then-apply flow (which will Advance the clock between Begin
// and End).
func (c *Cluster) Propose(idx int, op any) HistoryEvent {
	if idx < 0 || idx >= c.N {
		c.T.Fatalf("raftest: Propose idx %d out of range [0,%d)", idx, c.N)
	}
	callID := c.Recorder.BeginCall(idx, op)
	c.Recorder.EndCall(idx, callID, struct{ OK bool }{true})

	snap := c.Recorder.Snapshot()
	for i := len(snap) - 1; i >= 0; i-- {
		if snap[i].ClientID == idx && snap[i].Call == callID {
			return snap[i]
		}
	}
	c.T.Fatalf("raftest: Propose did not record event for client %d", idx)
	return HistoryEvent{}
}

// Close tears down the Hub and cancels the cluster's context.
// Idempotent — safe to call from t.Cleanup and from explicit defer.
func (c *Cluster) Close() {
	if c.cancel != nil {
		c.cancel()
	}
	if c.Hub != nil {
		_ = c.Hub.Close()
	}
}
