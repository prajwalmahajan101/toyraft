package raftest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	"github.com/prajwalmahajan101/toyraft/pkg/storage/memory"
	"github.com/prajwalmahajan101/toyraft/pkg/transport/inproc"
)

// Cluster is the N-node test fixture. Plan 05-05 swaps the Phase-4
// noopNode for raftNodeAdapter, which wraps a real *raft.TestNode and
// runs the canonical Step -> Recv -> Step -> Ready -> SaveHardState ->
// RecordSend -> Send driver loop per Tick (RESEARCH Pattern 3 / Plan
// 05-05). The harness fields (T, N, Seed, Clock, Hub, Recorder) keep
// the Phase-4 surface so existing tests (TestCluster_TwoRunsByteIdentical)
// continue to compile against the same struct.
type Cluster struct {
	T        testing.TB
	N        int
	Seed     int64
	Clock    *clock.Fake
	Hub      *inproc.Hub
	Recorder *Recorder

	endpoints []*inproc.Endpoint // one per node, indexed 0..N-1
	nodeIDs   []raft.NodeID      // sorted; nodeIDs[i] is the ID for index i
	nodes     []*raftNodeAdapter // real-node adapters; Phase 5 driver
	ctx       context.Context    // used for Endpoint.Send and shutdown
	cancel    context.CancelFunc
}

// raftNodeAdapter wraps a *raft.TestNode plus the per-node IO
// substrate (OrderingStorage + inproc.Endpoint). The adapter exists
// only inside internal/raftest — Phase 7 lands the canonical
// pkg/raft/driver.go which retires this type and the tickOnce loop
// below.
//
// Field layout mirrors the canonical Step/Ready event-loop driver: one
// node, one storage wrapper (so SC5 ordering is recorded), one Hub
// endpoint (so Send is bound to this node's identity).
type raftNodeAdapter struct {
	id       raft.NodeID
	node     *raft.TestNode
	storage  *OrderingStorage
	endpoint *inproc.Endpoint
}

// Node returns the wrapped *raft.TestNode. Tests that need to inject
// a Step directly (e.g. TestStepDownHaltsInFlight forcing a higher-
// term AppendEntries) reach the node through this accessor rather
// than the IO loop in Cluster.tickOnce.
func (a *raftNodeAdapter) Node() *raft.TestNode { return a.node }

// Storage exposes the OrderingStorage wrapper for tests that want to
// run the SC5 precedence assertion at end-of-test.
func (a *raftNodeAdapter) Storage() *OrderingStorage { return a.storage }

// NewCluster builds an N-node cluster on a shared FakeClock + Hub at
// the given seed. N must be odd and >= 3 (Raft quorum requirement).
// Per-node wiring:
//
//   - memory.New() owns the durable HardState; wrapped in
//     OrderingStorage so SC5 layer-3 is always recorded.
//   - inproc.Endpoint joined to the Hub; Send goes through the
//     chaos layer.
//   - *raft.TestNode constructed via raft.NewTestNode with
//     Config.Seed XOR per-node mixing (handled inside newNodeRNG —
//     see ADR-0009); Config.Clock = c.Clock so wall-clock entropy
//     never leaks in.
//
// The Hub and per-node storage are torn down via t.Cleanup; tests do
// not need to defer Close themselves.
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

	ctx, cancel := context.WithCancel(context.Background())

	c := &Cluster{
		T:         t,
		N:         n,
		Seed:      seed,
		Clock:     clk,
		Hub:       hub,
		Recorder:  rec,
		nodeIDs:   make([]raft.NodeID, n),
		endpoints: make([]*inproc.Endpoint, n),
		nodes:     make([]*raftNodeAdapter, n),
		ctx:       ctx,
		cancel:    cancel,
	}

	// First pass: allocate IDs + endpoints + storages so we can
	// build the full peers slice (a node must know every peer at
	// Config-validation time).
	peers := make([]raft.NodeID, n)
	for i := range n {
		// Zero-padded ID: n00, n01, ... so lex-sort matches numeric
		// index order for N up to 100.
		c.nodeIDs[i] = raft.NodeID(fmt.Sprintf("n%02d", i))
		c.endpoints[i] = hub.Connect(c.nodeIDs[i])
		peers[i] = c.nodeIDs[i]
	}

	// Second pass: build the real *raft.TestNode per node with a
	// shared Peers slice (each node's Config carries a clone so
	// downstream sorting in newNode does not aliasing-mutate ours).
	for i := range n {
		ord := NewOrderingStorage(memory.New())
		cfg := &raft.Config{
			ID:      c.nodeIDs[i],
			Peers:   slices.Clone(peers),
			Seed:    seed,
			Clock:   clk,
			Storage: ord,
		}
		node, err := raft.NewTestNode(cfg)
		if err != nil {
			t.Fatalf("raftest: NewTestNode[%d] (%s): %v", i, c.nodeIDs[i], err)
		}
		c.nodes[i] = &raftNodeAdapter{
			id:       c.nodeIDs[i],
			node:     node,
			storage:  ord,
			endpoint: c.endpoints[i],
		}
	}

	t.Cleanup(c.Close)
	return c
}

// Tick advances the FakeClock by d, gives the Hub dispatcher a brief
// wall-clock window to deliver any due messages, then runs the
// canonical Phase-5 driver loop on every node (tickOnce). RESEARCH
// Pitfall 7 — d SHOULD be <= ElectionTimeoutMin to avoid tick storms.
//
// The post-Advance quiesce is a scheduling hint, not a determinism
// boundary: the trace is determined by (seed, FakeClock state, Hub
// chaos seed), and the sleep only ensures scheduled deliveries have
// landed on receiver channels before tickOnce drains them.
func (c *Cluster) Tick(d time.Duration) {
	c.Clock.Advance(d)
	quiesce()
	for _, a := range c.nodes {
		c.tickOnce(a)
	}
	// Second pass — give the dispatcher a chance to land any
	// messages emitted DURING this Tick onto receivers, and drain
	// those into the next Step. Without this pass an inbound AE
	// from a freshly-elected leader would not be observed until the
	// NEXT Tick, doubling the convergence time and tickling Pitfall
	// 4 retries unnecessarily.
	quiesce()
	for _, a := range c.nodes {
		c.tickOnce(a)
	}
}

// tickOnce runs the per-node driver loop:
//
//  1. Step(MsgTick) — drives the election timer.
//  2. Drain inbound messages from the Hub non-blockingly into Step.
//  3. Ready() — snapshot pendingMsgs + pendingHS under n.mu.
//  4. SaveHardState (via OrderingStorage) BEFORE any Send (SC5).
//  5. RecordSend(m) -> Endpoint.Send(m) for each outbound message.
//
// Send failures are LOGGED, not FATAL — the Hub drops messages by
// design (chaos layer); a dropped Send is correct behaviour and must
// not abort the test. Step errors that are not ErrStopped DO fatal
// (validation drift would otherwise hide).
func (c *Cluster) tickOnce(a *raftNodeAdapter) {
	// 1. MsgTick.
	if err := a.node.Step(raft.Message{Type: raft.MsgTick}); err != nil {
		if !errors.Is(err, raft.ErrStopped) {
			c.T.Fatalf("raftest: Step(MsgTick) on %s: %v", a.id, err)
		}
	}
	// 2. Drain inbound non-blockingly.
	for {
		select {
		case m, ok := <-a.endpoint.Recv():
			if !ok {
				return
			}
			if err := a.node.Step(m); err != nil {
				if errors.Is(err, raft.ErrStopped) {
					return
				}
				c.T.Fatalf("raftest: Step(inbound) on %s: %v", a.id, err)
			}
		default:
			goto drainDone
		}
	}
drainDone:
	// 3. Ready() — outbound messages + pending HardState.
	msgs, hs := a.node.Ready()
	// 4. SC5: persist HardState BEFORE any Send.
	if hs != nil {
		if err := a.storage.SaveHardState(*hs); err != nil {
			c.T.Fatalf("raftest: SaveHardState on %s: %v", a.id, err)
		}
	}
	// 5. Record + Send each outbound message.
	for _, m := range msgs {
		a.storage.RecordSend(m)
		if err := a.endpoint.Send(c.ctx, m); err != nil {
			// Chaos drop is fine. ctx.Cancel during shutdown is fine.
			c.T.Logf("raftest: Send drop on %s -> %s: %v", a.id, m.To, err)
		}
	}
}

// AssertAtMostOneLeaderPerTerm is the SC6 / ELEC-10 invariant.
// Snapshots each node's (Role, Term); groups Leaders by Term; fails
// via t.Fatalf if any term has >1 Leader. Callable from inside any
// tick loop (cheap — N lock acquisitions per call).
//
// The failure message includes the violating Term, the colliding
// NodeIDs (sorted for deterministic output across runs), and the
// Cluster's seed (for bisecting which seed broke).
func (c *Cluster) AssertAtMostOneLeaderPerTerm() {
	c.T.Helper()
	leadersByTerm := make(map[raft.Term][]raft.NodeID)
	for _, a := range c.nodes {
		role, term := a.node.RoleAndTerm()
		if role == raft.Leader {
			leadersByTerm[term] = append(leadersByTerm[term], a.id)
		}
	}
	for term, ids := range leadersByTerm {
		if len(ids) > 1 {
			slices.Sort(ids)
			c.T.Fatalf("ELEC-10 violation: term %d has %d leaders: %v (seed=%d)",
				term, len(ids), ids, c.Seed)
		}
	}
}

// HasLeader reports whether any node currently reports Role=Leader.
// Used by tests that need to wait-for-election before injecting a
// step-down trigger (TestStepDownHaltsInFlight).
func (c *Cluster) HasLeader() bool {
	for _, a := range c.nodes {
		role, _ := a.node.RoleAndTerm()
		if role == raft.Leader {
			return true
		}
	}
	return false
}

// Leader returns the (ID, Term) of the first Leader observed, or
// ("", 0) if no node currently reports Role=Leader. "First" is by
// the cluster's ascending nodeIDs order (lex sort by construction).
func (c *Cluster) Leader() (raft.NodeID, raft.Term) {
	for _, a := range c.nodes {
		role, term := a.node.RoleAndTerm()
		if role == raft.Leader {
			return a.id, term
		}
	}
	return "", 0
}

// NodeByID returns the adapter for id, or nil if no such node. Used
// by integration tests that need direct Step/Ready access on a
// specific node (TestStepDownHaltsInFlight).
func (c *Cluster) NodeByID(id raft.NodeID) *raftNodeAdapter {
	for _, a := range c.nodes {
		if a.id == id {
			return a
		}
	}
	return nil
}

// Propose records a client operation against node idx and returns the
// resulting HistoryEvent. Phase 5 keeps Phase-4 semantics: every
// propose is "applied" instantly and returns a synthetic OK result.
// Phase 7 wires this through the public raft.Node.Propose surface;
// until then the Recorder shim is the only writer into the history,
// which keeps TestCluster_TwoRunsByteIdentical byte-identical even
// after the real-node swap.
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

// quiesce gives the Hub dispatcher goroutine a brief wall-clock
// window to land scheduled deliveries onto receiver channels. The
// trace itself is determined by (seed, FakeClock state); this sleep
// only ensures the dispatcher has had a chance to run before the
// per-node drain in tickOnce inspects the inbound channel.
//
// Sized to give the dispatcher goroutine reliable scheduling slack
// without dominating chaos-suite wall-clock budgets — 2ms is enough
// for runtime.Gosched plus a couple of channel hops on commodity
// hardware, and 100×200×2×2ms ≈ 80s of nominal wait time which the
// Go test parallel runner amortises across cores.
func quiesce() { time.Sleep(2 * time.Millisecond) }
