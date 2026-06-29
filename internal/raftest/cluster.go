package raftest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	"github.com/prajwalmahajan101/toyraft/pkg/storage/memory"
	"github.com/prajwalmahajan101/toyraft/pkg/transport/inproc"
)

// endpointTransport adapts an inproc.Endpoint (the Hub-bound per-node
// channel pair) to the LLD §3 raft.Transport the public raft.Node consumes.
// It lands HERE (internal/raftest), not in pkg/raft, because
// pkg/transport/inproc imports pkg/raft — wiring the adapter inside pkg/raft
// would form the cycle pkg/raft -> inproc -> pkg/raft (the R-5 deferral from
// 07-03; see pkg/raft/driver.go runInbound doc). internal/raftest already
// imports BOTH, so it is the natural home.
//
// GOLEAK CONTRACT (Warning-B, carried forward from 07-03):
//   - The adapter owns its OWN ctx + cancel + sync.WaitGroup. The inproc
//     Endpoint's Recv() channel is NEVER closed by the Hub (CONCURRENCY.md
//     §5), so the inbound pump cannot rely on a channel close to exit; it
//     selects on the adapter ctx instead.
//   - Register spawns the pump (one goroutine) draining Recv() into the
//     node's Step until the adapter ctx is cancelled.
//   - Close() (sync.Once-guarded) cancels the ctx and wg.Wait()s to JOIN
//     the pump before returning. nodeImpl.Stop calls Transport.Close AFTER
//     its own wg.Wait (node_public.go), so the pump is reaped before goleak
//     fires at end-of-test.
type endpointTransport struct {
	ep     *inproc.Endpoint
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
}

// newEndpointTransport wraps ep with a fresh adapter ctx. The ctx is the
// adapter's OWN (derived from Background), independent of any caller ctx —
// the pump lifetime is bounded solely by Close().
func newEndpointTransport(ep *inproc.Endpoint) *endpointTransport {
	ctx, cancel := context.WithCancel(context.Background())
	return &endpointTransport{ep: ep, ctx: ctx, cancel: cancel}
}

// Send routes an outbound message through the Hub's chaos layer. Send
// errors (e.g. ctx.Canceled during shutdown) propagate to the driver,
// which logs-and-drops per the lossy-Transport contract (LLD §3).
func (t *endpointTransport) Send(ctx context.Context, msg raft.Message) error {
	return t.ep.Send(ctx, msg)
}

// Register spawns the inbound pump. The pump selects on the adapter ctx
// (NOT a Recv() close — the Hub never closes Recv) and delivers each inbound
// message to step with the adapter ctx. A nil step is treated as a no-op
// registration (defensive; the public Node always registers n.Step).
func (t *endpointTransport) Register(step func(ctx context.Context, msg raft.Message) error) {
	if step == nil {
		return
	}
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		recv := t.ep.Recv()
		for {
			select {
			case <-t.ctx.Done():
				return
			case msg, ok := <-recv:
				if !ok { // defensive: Hub never closes Recv, but exit cleanly if it does
					return
				}
				// step errors are logged-and-dropped at the transport edge
				// (LLD §3): a validation/ErrStopped error must not kill the
				// pump. We deliberately ignore the return — the node's own
				// logging surfaces drift.
				_ = step(t.ctx, msg)
			}
		}
	}()
}

// Close cancels the adapter ctx and JOINS the pump (sync.Once-guarded,
// idempotent — LLD §3 requires Close be safe to call repeatedly and without
// a prior Register). Returns nil; the join guarantees goleak sees no
// residual pump goroutine.
func (t *endpointTransport) Close() error {
	t.once.Do(func() {
		t.cancel()
		t.wg.Wait()
	})
	return nil
}

// recordingSM is the per-node StateMachine. It records every applied entry
// under a mutex so apply progress is observable. Apply returns a trivial OK
// result; the harness asserts on log/commit state (via Storage + Status), not
// on apply outputs. Required because Config.Validate rejects a nil
// StateMachine (07-01 R-2).
type recordingSM struct {
	mu      sync.Mutex
	applied []raft.Entry
}

// Apply records the entry and returns a trivial OK result. Never errors —
// a recording SM has no failure mode (API-06 panic-recovery is exercised by
// pkg/raft's own apply_test.go, not here).
func (s *recordingSM) Apply(e raft.Entry) (any, error) {
	s.mu.Lock()
	s.applied = append(s.applied, e)
	s.mu.Unlock()
	return struct{ OK bool }{true}, nil
}

// Snapshot / Restore are unsupported (snapshots land in a later phase).
func (s *recordingSM) Snapshot() ([]byte, raft.Index, error) {
	return nil, 0, raft.ErrSnapshotUnsupported
}
func (s *recordingSM) Restore([]byte) error { return raft.ErrSnapshotUnsupported }

// Cluster is the N-node test fixture. 07-04 rewires it onto the public
// raft.New / raft.Node surface: each member is a real raft.Node driven
// synchronously by the harness (model ii) over the shared clock.Fake + inproc
// Hub. The Phase-4 hand-driven tickOnce loop and the exported raft.TestNode
// handle are DELETED; the synchronous per-node driver step is the production
// driver+apply sequence run inline via the TickForTest/DrainForTest seam. The
// harness fields (T, N, Seed, Clock, Hub, Recorder) keep the Phase-4 surface
// so existing tests compile.
type Cluster struct {
	T        testing.TB
	N        int
	Seed     int64
	Clock    *clock.Fake
	Hub      *inproc.Hub
	Recorder *Recorder

	endpoints []*inproc.Endpoint // one per node, indexed 0..N-1
	nodeIDs   []raft.NodeID      // sorted; nodeIDs[i] is the ID for index i
	nodes     []*RaftNodeAdapter // real raft.Node adapters
	ctx       context.Context    // cluster lifetime; cancelled in Close
	cancel    context.CancelFunc

	// committed is the cluster-wide record of every committed entry the
	// suite has ever observed, keyed by 1-based log index. Populated by
	// AssertNoCommittedEntryLost on each call (REPL-10 spirit): a
	// committed entry is immutable, so once recorded it must never change
	// at any node. nil until the first AssertNoCommittedEntryLost call.
	committed map[raft.Index]raft.Entry
}

// nodeTestDriver is the narrow synchronous test-driver seam (07-04 model ii)
// raftest type-asserts raft.Node to. It carries a single EXPORTED method
// defined on the UNEXPORTED *nodeImpl in pkg/raft (driver_testseam.go) — so
// it stays out of the LLD go-doc golden while being callable here. One
// TickForTest call runs the EXACT production driver+apply sequence
// (Step(MsgTick) -> Ready -> SaveHardState -> Send -> apply committed)
// synchronously on the caller's goroutine.
type nodeTestDriver interface {
	TickForTest()
	DrainForTest()
}

// RaftNodeAdapter wraps a real raft.Node plus the per-node IO substrate
// (OrderingStorage + inproc.Endpoint + endpointTransport + recordingSM).
// The adapter exists only inside internal/raftest.
//
// 07-04 HARNESS MODEL (ii): the node is NOT Start'd (no async driver/applier/
// inbound-pump goroutines). Instead the harness drives it DETERMINISTICALLY
// and SYNCHRONOUSLY: each Tick drains inbound (public Step) then calls
// driver.TickForTest() (the production driver+apply sequence, run inline).
// This restores the proven Phase-5 synchronous determinism while exercising
// the REAL production driver logic + the public surface. The own-driver
// async model (i) was correct but not settle-able within the chaos suite's
// wall-clock budget (see driver_testseam.go rationale) — the plan authorised
// this fallback.
type RaftNodeAdapter struct {
	id        raft.NodeID
	node      raft.Node
	driver    nodeTestDriver // the synchronous TickForTest seam (model ii)
	storage   *OrderingStorage
	endpoint  *inproc.Endpoint
	transport *endpointTransport
	sm        *recordingSM
}

// Node returns the wrapped raft.Node. Tests that need the public surface
// (Step/Propose/Status) reach the node through this accessor.
func (a *RaftNodeAdapter) Node() raft.Node { return a.node }

// Storage exposes the OrderingStorage wrapper for tests that want to run
// the SC5 precedence assertion at end-of-test.
func (a *RaftNodeAdapter) Storage() *OrderingStorage { return a.storage }

// roleAndTerm returns the node's (Role, Term) via the public Status surface.
// raftest-level read seam the Assert* methods use (Task-1 introduced; Task-2
// swapped the body off TestNode onto Status()).
func (a *RaftNodeAdapter) roleAndTerm() (raft.Role, raft.Term) {
	s := a.node.Status()
	return s.Role, s.Term
}

// commitIndex returns the node's reported commitIndex via Status().
func (a *RaftNodeAdapter) commitIndex() raft.Index {
	return a.node.Status().CommitIndex
}

// logFromStorage reads the node's replicated log from its OrderingStorage
// (R-4 LOCKED decision): the Storage mirror IS the durable log written in
// lockstep with the in-memory log (ADR-0011), so this is byte-equivalent to
// the old TestNode.Log() WITHOUT any exported pkg/raft accessor.
func (a *RaftNodeAdapter) logFromStorage() []raft.Entry {
	last, err := a.storage.LastIndex()
	if err != nil {
		a.adapterFatalf("raftest: LastIndex on %s: %v", a.id, err)
		return nil
	}
	if last == 0 {
		return nil
	}
	ents, err := a.storage.Entries(1, last+1)
	if err != nil {
		a.adapterFatalf("raftest: Entries(1,%d) on %s: %v", last+1, a.id, err)
		return nil
	}
	return ents
}

// adapterFatalf reports a storage-read failure. The adapter has no
// testing.TB handle, so it panics — a storage read failure is a harness bug
// (the in-memory mirror never errors in practice), surfaced loudly.
func (a *RaftNodeAdapter) adapterFatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

// LogOf returns a copy of node id's replicated log read from its
// OrderingStorage mirror (R-4 LOCKED). figure8_test.go consumes this for its
// logTermAt helper; adds NO exported pkg/raft symbol.
func (c *Cluster) LogOf(id raft.NodeID) []raft.Entry {
	a := c.NodeByID(id)
	if a == nil {
		return nil
	}
	return a.logFromStorage()
}

// NewCluster builds an N-node cluster on a shared FakeClock + Hub at the
// given seed. N must be odd and >= 3 (Raft quorum requirement). Each member
// is a real raft.Node from raft.New, driven SYNCHRONOUSLY by the harness
// (model ii — the node is NOT Start'd). Per-node wiring:
//
//   - memory.New() owns the durable HardState + log; wrapped in
//     OrderingStorage so SC5 layer-3 is always recorded.
//   - inproc.Endpoint joined to the Hub; wrapped in endpointTransport (the
//     R-5 inproc -> LLD Transport adapter, goleak-contract above). The
//     adapter is still constructed (Config requires a non-nil Transport) and
//     used for Send; in model ii inbound is drained synchronously by the
//     harness via the public Step, so the adapter's Register pump is unused.
//   - recordingSM observes applied entries.
//   - raft.New(cfg) -> raft.Node, type-asserted to nodeTestDriver for the
//     synchronous TickForTest seam. node.Start is deliberately NOT called: the
//     harness owns the tick schedule (no async driver/applier/pump goroutines
//     to race), which is what makes the model deterministic + fast.
//
// The Hub is Close'd in c.Close; the cluster ctx is cancelled there too
// (reaping any parked Propose goroutine from ProposeIntoForTest).
// t.Cleanup(c.Close) handles teardown; tests need not defer.
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
		nodes:     make([]*RaftNodeAdapter, n),
		ctx:       ctx,
		cancel:    cancel,
	}

	// First pass: allocate IDs + endpoints so we can build the full peers
	// slice (a node must know every peer at Config-validation time).
	peers := make([]raft.NodeID, n)
	for i := range n {
		// Zero-padded ID: n00, n01, ... so lex-sort matches numeric index
		// order for N up to 100.
		c.nodeIDs[i] = raft.NodeID(fmt.Sprintf("n%02d", i))
		c.endpoints[i] = hub.Connect(c.nodeIDs[i])
		peers[i] = c.nodeIDs[i]
	}

	// Second pass: build a real raft.Node per node via raft.New, wiring the
	// inproc Transport adapter + recordingSM (07-01 made both required by
	// Validate). Each Config carries a clone of peers so downstream sorting
	// in newNode does not aliasing-mutate ours.
	for i := range n {
		ord := NewOrderingStorage(memory.New())
		transport := newEndpointTransport(c.endpoints[i])
		sm := &recordingSM{}
		cfg := raft.Config{
			NodeID:       c.nodeIDs[i],
			Peers:        slices.Clone(peers),
			Seed:         seed,
			Clock:        clk,
			Storage:      ord,
			Transport:    transport,
			StateMachine: sm,
		}
		node, err := raft.New(cfg)
		if err != nil {
			t.Fatalf("raftest: raft.New[%d] (%s): %v", i, c.nodeIDs[i], err)
		}
		driver, ok := node.(nodeTestDriver)
		if !ok {
			t.Fatalf("raftest: raft.New[%d] (%s): node does not implement the "+
				"TickForTest synchronous driver seam (model ii)", i, c.nodeIDs[i])
		}
		c.nodes[i] = &RaftNodeAdapter{
			id:        c.nodeIDs[i],
			node:      node,
			driver:    driver,
			storage:   ord,
			endpoint:  c.endpoints[i],
			transport: transport,
			sm:        sm,
		}
	}

	// Model ii: nodes are NOT Start'd. The harness owns the tick schedule and
	// drives each node synchronously via TickForTest (see Tick). No async
	// driver/applier/inbound-pump goroutines run, so there is nothing to race
	// and no per-tick wall-clock settle is needed.

	t.Cleanup(c.Close)
	return c
}

// Tick advances the FakeClock by d, then runs the synchronous deterministic
// driver loop (model ii) on every node:
//
//  1. Advance(d): moves fake time forward so the Hub schedules due deliveries
//     and election-timeout accounting (in TickForTest's Step(MsgTick))
//     reflects the elapsed window.
//  2. Two PASSES of {drain inbound -> TickForTest}: pass 1 lets each node
//     react to messages already on its inbound channel and emit this tick's
//     outbound (heartbeats / AE / votes); a brief delivery window lets the
//     async Hub dispatcher land those onto receiver channels; pass 2 drains
//     them so a freshly-emitted AE is observed THIS Tick rather than the next
//     (mirrors the proven Phase-5 two-pass loop — without it convergence
//     doubles and follower timeouts tickle spurious re-elections).
//
// RESEARCH Pitfall 7 — d SHOULD be <= ElectionTimeoutMin to avoid tick
// storms. The trace is determined by (seed, FakeClock state, Hub chaos seed);
// the only wall-clock dependency is the brief deliveryWindow that lets the
// (still-async) single Hub dispatcher goroutine flush — identical in spirit
// to the Phase-5 quiesce.
func (c *Cluster) Tick(d time.Duration) {
	c.Clock.Advance(d)

	// Pass 1: drain any already-delivered inbound, then drive ONE synchronous
	// tick (MsgTick + Ready/Send/apply) per node. This is the SOLE MsgTick per
	// Cluster.Tick, so the tick-domain election timer advances exactly once
	// per Tick (one logical tick = one Tick) — the cadence the Figure-8/chaos
	// budgets assume.
	for _, a := range c.nodes {
		c.drainInbound(a)
		a.driver.TickForTest()
	}
	// Let the async Hub dispatcher deliver the messages emitted in pass 1 onto
	// receiver inbound channels before pass 2 drains them.
	c.deliveryQuiesce()
	// Pass 2: drain the freshly-delivered inbound + DRAIN (Ready/Send/apply,
	// NO MsgTick) so an inbound AE/vote from pass 1 is reacted to within THIS
	// Tick (its response shipped, its commit applied) WITHOUT double-counting
	// the election timer. Without this pass, convergence doubles and follower
	// timeouts tickle spurious re-elections.
	for _, a := range c.nodes {
		c.drainInbound(a)
		a.driver.DrainForTest()
	}
}

// drainInbound non-blockingly drains every message currently on the node's
// inproc inbound channel and delivers each to the node via the PUBLIC
// raft.Node.Step. In model ii the endpointTransport's async Register pump is
// unused — the harness owns inbound delivery so the schedule stays
// deterministic. ErrStopped is treated as terminal (node torn down);
// validation errors are logged-and-dropped at the transport edge (LLD §3).
func (c *Cluster) drainInbound(a *RaftNodeAdapter) {
	for {
		select {
		case m, ok := <-a.endpoint.Recv():
			if !ok {
				return
			}
			if err := a.node.Step(c.ctx, m); err != nil {
				if errors.Is(err, raft.ErrStopped) {
					return
				}
				// Validation / unknown-message errors: log-and-drop at the
				// transport edge (the node's own logging surfaces real drift).
				c.T.Logf("raftest: Step(inbound) on %s: %v", a.id, err)
			}
		default:
			return
		}
	}
}

// deliveryQuiesce gives the single async Hub dispatcher goroutine a brief
// wall-clock window to land scheduled deliveries onto receiver inbound
// channels between the two driver passes. The trace itself is determined by
// (seed, FakeClock state, Hub chaos seed); this sleep only ensures the
// dispatcher has run before pass 2 inspects the inbound channels — the same
// role the Phase-5 harness's quiesce() played (sized identically at 2ms).
func (c *Cluster) deliveryQuiesce() { time.Sleep(2 * time.Millisecond) }

// AssertAtMostOneLeaderPerTerm is the SC6 / ELEC-10 invariant. Snapshots
// each node's (Role, Term); groups Leaders by Term; fails via t.Fatalf if
// any term has >1 Leader.
func (c *Cluster) AssertAtMostOneLeaderPerTerm() {
	c.T.Helper()
	leadersByTerm := make(map[raft.Term][]raft.NodeID)
	for _, a := range c.nodes {
		role, term := a.roleAndTerm()
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

// AssertLogMatching is the REPL-11 log-matching invariant. For every ordered
// PAIR of nodes it walks the common index range; if at some index i both logs
// carry an entry with the SAME term, then every entry at indices < i MUST be
// byte-identical (term + data) between the two logs.
//
// This is the Raft §5.3 Log Matching property as a continuous invariant: it
// holds at every tick under chaos. Log snapshots are read from each node's
// OrderingStorage mirror (logFromStorage; R-4 LOCKED), which is the durable
// log written in lockstep (ADR-0011), so no live reference escapes.
func (c *Cluster) AssertLogMatching() {
	c.T.Helper()
	logs := make([][]raft.Entry, c.N)
	for i, a := range c.nodes {
		logs[i] = a.logFromStorage()
	}
	for i := range c.nodes {
		for j := range c.nodes {
			if i == j {
				continue
			}
			c.assertPairLogMatch(c.nodes[i].id, logs[i], c.nodes[j].id, logs[j])
		}
	}
}

// assertPairLogMatch enforces log-matching for one ordered (a, b) pair.
// Scanning from the highest common index downward, the first index where
// both logs agree on term obligates every earlier index to be byte-identical.
func (c *Cluster) assertPairLogMatch(aID raft.NodeID, aLog []raft.Entry, bID raft.NodeID, bLog []raft.Entry) {
	common := min(len(aLog), len(bLog))
	for k := common - 1; k >= 0; k-- {
		if aLog[k].Term != bLog[k].Term {
			continue
		}
		// log[k] agrees on term: indices 0..k MUST be byte-identical.
		for m := 0; m <= k; m++ {
			if aLog[m].Term != bLog[m].Term || !bytes.Equal(aLog[m].Data, bLog[m].Data) {
				c.T.Fatalf("REPL-11 log-matching violation: nodes %s and %s agree at index %d "+
					"(term %d) but diverge at index %d: %s=%+v %s=%+v (seed=%d)",
					aID, bID, k+1, aLog[k].Term, m+1, aID, aLog[m], bID, bLog[m], c.Seed)
			}
		}
		return
	}
}

// AssertNoCommittedEntryLost enforces that committed entries are immutable
// (REPL-10 spirit, as a continuous invariant). It maintains a cluster-wide
// record of every committed entry observed: on each call, for every node,
// for every index up to that node's commitIndex, it records the entry on
// first sight and asserts equality against the record thereafter.
//
// Committed entries live at log indices <= commitIndex; the log is 1-based so
// log[i-1] is the entry at index i. Log snapshots come from the
// OrderingStorage mirror (logFromStorage; R-4 LOCKED); commitIndex from the
// node's commitIndex() seam.
func (c *Cluster) AssertNoCommittedEntryLost() {
	c.T.Helper()
	if c.committed == nil {
		c.committed = make(map[raft.Index]raft.Entry)
	}
	for _, a := range c.nodes {
		entries := a.logFromStorage()
		ci := a.commitIndex()
		for idx := raft.Index(1); idx <= ci; idx++ {
			if int(idx) > len(entries) {
				// A node may not yet hold every committed entry it has learned
				// about via LeaderCommit; only assert entries it actually has.
				break
			}
			got := entries[idx-1]
			prev, seen := c.committed[idx]
			if !seen {
				c.committed[idx] = got
				continue
			}
			if prev.Term != got.Term || !bytes.Equal(prev.Data, got.Data) {
				c.T.Fatalf("REPL-10 committed-entry-lost violation: node %s changed committed "+
					"index %d from %+v to %+v (seed=%d)", a.id, idx, prev, got, c.Seed)
			}
		}
	}
}

// ProposeToLeader is the real-replication client entry point. It finds the
// current leader via c.Leader(), then injects op through the public
// raft.Node.Propose surface (NOT the retired recorder shim). It returns
// (assignedIndex, true) once the leader has locally appended op (the entry's
// assigned index, read from the leader's Storage mirror once proposeLocked has
// run), or (0, false) when no leader exists or the leader refuses the proposal.
//
// FAITHFULNESS to the old (Index, bool) contract: the public Propose BLOCKS
// until the entry is APPLIED (SC3). The Figure-8 / chaos callers want the
// ASSIGNED INDEX at append time, not the applied result — and the Figure-8
// script deliberately proposes entries into isolated leaders that NEVER
// commit. So Propose is launched on a goroutine BOUND TO THE CLUSTER CTX
// (cancelled in Close, so a never-applied proposal goroutine is reaped at
// teardown — goleak clean) and the assigned index is read from the leader's
// Storage LastIndex after the local append settles. See ProposeIntoForTest
// for the index-pinned seam Figure-8 consumes.
func (c *Cluster) ProposeToLeader(op []byte) (raft.Index, bool) {
	leaderID, _ := c.Leader()
	if leaderID == "" {
		return 0, false
	}
	return c.ProposeIntoForTest(leaderID, op)
}

// ProposeIntoForTest injects op into the SPECIFIC node id through the public
// raft.Node.Propose path and returns the leader's assigned index immediately
// (mirroring the retired TestNode.Propose (Index, bool) contract). It does
// NOT block on apply — faithful to the non-committing isolated-leader
// Figure-8 cases (stages a/b/c) where the proposed entry never commits.
//
// Mechanism (goroutine + LastIndex poll, goleak-safe under model ii):
//   - Snapshot the leader's Storage LastIndex BEFORE proposing.
//   - Launch raft.Node.Propose on a goroutine bound to the CLUSTER CTX. The
//     public Propose appends + mirrors the entry to Storage SYNCHRONOUSLY
//     (proposeLocked) and THEN parks on the apply waiter. The committing case
//     unblocks when a later Tick applies the entry; the non-committing
//     isolated-leader case stays parked until Close cancels c.ctx — no Propose
//     goroutine outlives the test (goleak clean).
//   - Poll the leader's Storage LastIndex (yielding the scheduler so the
//     proposal goroutine runs past proposeLocked) until it advances past the
//     snapshot, then return (newLastIndex, true). No Tick is needed for the
//     LOCAL append — it is synchronous inside Propose.
//   - If id is not currently a leader (Propose returns *ErrNotLeader) or the
//     append never lands within budget, return (0, false) — preserving the
//     old "!ok -> caller Fatalf" semantics.
func (c *Cluster) ProposeIntoForTest(id raft.NodeID, op []byte) (raft.Index, bool) {
	a := c.NodeByID(id)
	if a == nil {
		return 0, false
	}
	// Reject non-leaders up front (cheap; mirrors proposeLocked's role check
	// and avoids spawning a doomed goroutine on a follower).
	if s := a.node.Status(); s.Role != raft.Leader {
		return 0, false
	}

	before, err := a.storage.LastIndex()
	if err != nil {
		a.adapterFatalf("raftest: LastIndex on %s: %v", id, err)
		return 0, false
	}

	// notLeader is set if the proposal goroutine observes an immediate role
	// refusal; guarded by its own mutex (the goroutine and the poller race).
	var mu sync.Mutex
	notLeader := false
	go func() {
		// ctx bound to the cluster: a never-applied proposal (isolated leader)
		// is unblocked when Close cancels c.ctx — never leaks past the test.
		_, _, perr := a.node.Propose(c.ctx, op)
		if perr != nil {
			mu.Lock()
			// A clean ErrNotLeader / ErrProposalDropped means the local append
			// never happened; flag it so the poller can return (0,false).
			if raftIsNotLeaderOrDropped(perr) {
				notLeader = true
			}
			mu.Unlock()
		}
	}()

	// Poll until the leader's durable log advances past `before` (the local
	// append, which proposeLocked does synchronously inside Propose) or the
	// goroutine reports a role refusal. Bounded so a wedged proposal fails
	// rather than hanging.
	const (
		maxPolls    = 100000
		pollTimeout = 2 * time.Second
	)
	deadline := time.Now().Add(pollTimeout)
	for range maxPolls {
		runtime.Gosched() // let the proposal goroutine run past proposeLocked

		mu.Lock()
		refused := notLeader
		mu.Unlock()
		if refused {
			return 0, false
		}

		last, lerr := a.storage.LastIndex()
		if lerr != nil {
			a.adapterFatalf("raftest: LastIndex on %s: %v", id, lerr)
			return 0, false
		}
		if last > before {
			return last, true
		}
		if time.Now().After(deadline) {
			return 0, false
		}
	}
	return 0, false
}

// raftIsNotLeaderOrDropped reports whether err is the public propose-refusal
// contract (*ErrNotLeader or ErrProposalDropped) — i.e. the local append
// never happened, so ProposeIntoForTest must report (0,false). A ctx.Err()
// (cluster Close during a non-committing proposal) is NOT a refusal: by then
// the local append already landed and LastIndex advanced, so the poller has
// already returned.
func raftIsNotLeaderOrDropped(err error) bool {
	if err == nil {
		return false
	}
	var nl *raft.ErrNotLeader
	if errors.As(err, &nl) {
		return true
	}
	return errors.Is(err, raft.ErrProposalDropped)
}

// HasLeader reports whether any node currently reports Role=Leader.
func (c *Cluster) HasLeader() bool {
	for _, a := range c.nodes {
		role, _ := a.roleAndTerm()
		if role == raft.Leader {
			return true
		}
	}
	return false
}

// Leader returns the (ID, Term) of the first Leader observed, or ("", 0) if
// no node currently reports Role=Leader. "First" is by ascending nodeIDs
// order (lex sort by construction).
func (c *Cluster) Leader() (raft.NodeID, raft.Term) {
	for _, a := range c.nodes {
		role, term := a.roleAndTerm()
		if role == raft.Leader {
			return a.id, term
		}
	}
	return "", 0
}

// NodeByID returns the adapter for id, or nil if no such node.
func (c *Cluster) NodeByID(id raft.NodeID) *RaftNodeAdapter {
	for _, a := range c.nodes {
		if a.id == id {
			return a
		}
	}
	return nil
}

// Close tears the cluster down. Order matters for goleak:
//   - Cancel the cluster ctx FIRST: this unblocks any parked Propose goroutine
//     from ProposeIntoForTest (a non-committing isolated-leader proposal that
//     never applies) via ctx.Done, so it returns instead of leaking.
//   - node.Stop() each node: idempotent; sets stopped + closes the Transport
//     adapter (sync.Once-guarded). In model ii the node was never Start'd, so
//     there are no async driver/applier/inbound-pump goroutines to join — Stop
//     is effectively a clean transport-close + stopped-flag set.
//   - Hub.Close(): joins the single dispatcher goroutine within its timeout.
//
// Idempotent — safe from t.Cleanup and explicit defer.
func (c *Cluster) Close() {
	if c.cancel != nil {
		c.cancel()
	}
	for _, a := range c.nodes {
		if a != nil && a.node != nil {
			_ = a.node.Stop()
		}
	}
	if c.Hub != nil {
		_ = c.Hub.Close()
	}
}
