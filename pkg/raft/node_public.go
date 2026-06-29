package raft

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"
)

// Node is the runtime handle a consumer holds. One Node per cluster member.
//
// The interface is frozen verbatim from docs/LLD.md §3. The concrete
// implementation is *nodeImpl, a thin lifecycle wrapper over the internal
// *node state machine (node.go). 07-02 lands the lifecycle spine
// (New/Start/Stop/Status/LeaderHint); the Propose/Step I/O path is completed
// in 07-03 (driver + apply channel).
type Node interface {
	// Start spawns the tick loop and brings the node online.
	//
	// Invariants:
	//   - Idempotent: a second Start on a running Node returns nil (API-02).
	//   - Returns only once internal goroutines (tick, apply, transport
	//     dispatch) are running and Transport.Register has been called.
	//
	// Error contract:
	//   - ErrStopped if called after Stop.
	//   - Validation errors from Config are surfaced verbatim.
	//
	// The ctx argument bounds BRING-UP only. Node lifetime is governed by the
	// internal root context cancelled in Stop(): a caller passing a
	// request-scoped ctx into Start MUST NOT thereby kill the node (RESEARCH
	// Open-Q 5 / anti-pattern). 07-02 spawns the goroutines under a fresh
	// context.Background()-derived root, never the caller ctx.
	Start(ctx context.Context) error

	// Stop drains the apply channel, closes the transport, and joins the
	// tick loop. Blocks for at most Config.StopTimeout.
	//
	// Invariants:
	//   - Idempotent across concurrent and repeated calls (API-02).
	//   - After Stop returns, Propose, Step, and Status all return ErrStopped.
	Stop() error

	// Propose submits a command for replication. Blocks until the entry is
	// committed AND applied (Apply has been called on this node's StateMachine),
	// or the context expires, or leadership is lost.
	//
	// Invariants:
	//   - Returns (index, term, nil) only after the command has been applied.
	//   - On leader loss before commit, returns ErrProposalDropped.
	//   - On follower/candidate role, returns ErrNotLeader with LeaderHint set.
	//
	// Error contract:
	//   - ErrNotLeader { LeaderHint: NodeID } — retry against the hint.
	//   - ErrProposalDropped — safe to retry.
	//   - ErrStopped — node is shut down; do not retry on this Node.
	//   - ctx.Err() — caller's deadline; the entry MAY still commit later.
	Propose(ctx context.Context, data []byte) (Index, Term, error)

	// Step is the inbound RPC entry point. Transport implementations call
	// Step for every Message received from a peer.
	//
	// Invariants:
	//   - Step is safe to call from any goroutine.
	//   - Step MUST NOT block on I/O; it hands off to the core tick loop.
	//   - Messages with Term > currentTerm trigger a step-down before any
	//     other processing.
	//
	// Error contract:
	//   - ErrStopped if the Node has been stopped.
	//   - Validation errors (e.g. unknown MessageType) are returned but the
	//     transport SHOULD log-and-drop rather than propagate to the wire.
	Step(ctx context.Context, msg Message) error

	// Status returns a copy of the current observable state (see Status type).
	// Cheap (microsecond-scale); safe to poll.
	Status() Status

	// LeaderHint returns the currently-believed leader, or empty if unknown.
	// Equivalent to Status().LeaderHint but avoids the map copy.
	LeaderHint() NodeID
}

// nodeImpl is the concrete Node. It wraps the internal *node state machine
// and owns the public lifecycle: the goroutine fleet (ticker, inbound,
// apply), the idempotency guards (startOnce/stopOnce), the bounded apply
// channel, and the proposal waiter registry.
//
// 07-02 declares ALL lifecycle fields — including those only 07-03's driver
// and apply loops consume — so 07-03 adds no fields, only method bodies.
// RESEARCH Pattern 1.
type nodeImpl struct {
	core *node
	cfg  *Config

	startOnce sync.Once
	stopOnce  sync.Once
	stopErr   error
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	applyCh chan Entry // bounded; 07-03 single reader (apply loop)

	// enqueuedIdx is DRIVER-owned: the highest log index the driver has handed
	// to applyCh (the channel fill frontier; 07-03 drainCommitsToApply). It is
	// NOT the applied index — entries on the channel may not yet have been
	// passed to StateMachine.Apply.
	enqueuedIdx atomic.Uint64

	// appliedIdx is APPLIER-owned: the highest index for which
	// StateMachine.Apply HAS RETURNED (07-03 applyOne advances it AFTER Apply
	// returns). Status().ApplyIndex reads THIS (API-07 / LLD §3 define
	// ApplyIndex as the last APPLIED index), never the enqueue frontier.
	appliedIdx atomic.Uint64

	waiters sync.Map              // map[Index]chan proposeResult; 07-03 registers/resolves
	fatal   atomic.Pointer[error] // set on Apply panic (API-06); 07-03
	stopped atomic.Bool           // true after Stop; Propose/Step guard
}

// proposeResult carries an applied proposal's outcome from the apply loop
// back to a blocked Propose caller. Used by 07-03's waiter registry.
type proposeResult struct {
	res any
	err error
}

// defaultApplyBuf is the bounded apply-channel capacity (RESEARCH Open-Q 4;
// API-05 — the apply channel is NEVER unbounded).
const defaultApplyBuf = 256

// applyBuf returns the bounded apply-channel capacity for cfg. Centralised so
// 07-03 (and any future tuning knob) has a single source of truth; today it is
// a fixed bound per API-05.
func applyBuf(cfg *Config) int {
	_ = cfg // reserved: a future Config.ApplyBuffer knob would read here
	return defaultApplyBuf
}

// New constructs a Node from the given Config. It applies defaults, validates
// all required fields (surfacing the first Config.Validate error VERBATIM per
// LLD §3 — even-N, nil-Transport, nil-StateMachine, etc. from 07-01), then
// builds the internal *node and the lifecycle wrapper.
//
// NOTE: newNode (node.go) ALSO calls cfg.applyDefaults()+cfg.Validate()
// internally. The double call is harmless — applyDefaults is idempotent and
// Validate is pure — so New()'s explicit pass exists to surface the error
// BEFORE construction rather than to gate newNode.
func New(cfg Config) (Node, error) {
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	core, err := newNode(&cfg)
	if err != nil {
		return nil, err
	}
	return &nodeImpl{
		core:    core,
		cfg:     &cfg,
		applyCh: make(chan Entry, applyBuf(&cfg)),
	}, nil
}

// Status returns a self-consistent snapshot of the node's observable state.
//
// SC6 / API-07: non-blocking, copy-under-lock. The core mutex is held ONLY for
// the field copy and the leader-only matchIndex clone — never across a channel
// operation or I/O (Pitfall 4). ApplyIndex is read from the appliedIdx atomic
// (the APPLIED index, advanced by 07-03's applyOne AFTER Apply returns — NOT
// the enqueue frontier), outside the lock.
func (n *nodeImpl) Status() Status {
	n.core.mu.Lock()
	s := Status{
		Role:        n.core.role,
		Term:        n.core.currentTerm,
		CommitIndex: n.core.commitIndex,
		LeaderHint:  n.core.leaderHint,
	}
	// matchIndex is leader-only; followers expose nil (LLD §3). Clone under the
	// lock so the caller owns the map.
	if n.core.role == Leader {
		s.MatchIndex = maps.Clone(n.core.matchIndex)
	}
	n.core.mu.Unlock()

	s.ApplyIndex = Index(n.appliedIdx.Load())
	return s
}

// LeaderHint returns the best-known current leader, or empty if unknown. It
// takes the core lock only briefly and — unlike Status — avoids the matchIndex
// map copy (LLD §3).
func (n *nodeImpl) LeaderHint() NodeID {
	n.core.mu.Lock()
	defer n.core.mu.Unlock()
	return n.core.leaderHint
}

// Step is the inbound RPC entry point. It guards against a stopped node, then
// honours ctx cancellation at the handoff boundary before delegating to the
// ctx-free internal core (R-6: the core never sees ctx; ctx is used only to
// drop a message whose caller has already cancelled).
//
// 07-03 completes this: the real driver routes Step through the inbound queue
// rather than calling the core synchronously. For 07-02 the direct delegation
// is correct-but-incomplete and satisfies the interface + Transport.Register.
func (n *nodeImpl) Step(ctx context.Context, msg Message) error {
	if n.stopped.Load() {
		return ErrStopped
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return n.core.Step(msg) // 07-03 completes this (inbound-queue handoff)
}

// Propose submits a command for replication.
//
// 07-03 completes this: the real path registers a waiter keyed by the assigned
// index, hands the entry to the leader, and blocks until the apply loop
// resolves the waiter (or ctx/leadership loss intervenes). For 07-02 it returns
// a TODO-marked placeholder so the type satisfies the interface; the spine
// (waiters, applyCh) it needs is already declared on nodeImpl.
func (n *nodeImpl) Propose(ctx context.Context, data []byte) (Index, Term, error) {
	if n.stopped.Load() {
		return 0, 0, ErrStopped
	}
	_ = ctx
	_ = data
	// 07-03 completes this: replace with the real waiter-registry + leader
	// proposeLocked path. Until then a Propose is a no-op drop.
	return 0, 0, ErrProposalDropped
}

// Start brings the node online: it registers the inbound callback on the
// transport and spawns the three lifecycle goroutines (ticker, inbound, apply)
// under a fresh ROOT context derived from context.Background() — NOT the
// caller's ctx (RESEARCH Pattern 2 anti-pattern: a request-scoped Start ctx
// must not kill the node). Idempotent via startOnce; a second Start is a no-op
// returning nil (API-02).
//
// The ctx argument bounds bring-up only; node lifetime is bounded by the root
// context cancelled in Stop().
func (n *nodeImpl) Start(ctx context.Context) error {
	_ = ctx // bring-up scope only; node lifetime is the internal root ctx
	n.startOnce.Do(func() {
		rctx, cancel := context.WithCancel(context.Background()) // ROOT ctx, NOT caller ctx
		n.cancel = cancel
		n.cfg.Transport.Register(n.Step) // LLD §3: Register BEFORE goroutines run
		n.wg.Add(3)
		go n.runTicker(rctx)  // 07-03 supplies the real tick loop
		go n.runInbound(rctx) // 07-03 supplies the real inbound dispatch
		go n.runApply(rctx)   // 07-03 supplies the real apply loop
	})
	return nil // second Start is a no-op nil (API-02)
}

// Stop cancels the root context, joins the goroutine fleet under
// Config.StopTimeout, and closes the transport. Idempotent via stopOnce; every
// caller (concurrent or repeated) observes the same captured stopErr (Global
// Invariant 3 / API-02). Bounded by StopTimeout (default 5s, API-08/SC5): a
// wedged goroutine yields a timeout error rather than a hung Stop.
//
// Stop-before-Start is safe: cancel is nil (no goroutine ran) and the wait
// completes immediately because wg is zero. Transport.Close is called
// unconditionally — LLD §3 guarantees Close is idempotent and safe without a
// prior Register, so test doubles MUST honour that (return nil regardless).
func (n *nodeImpl) Stop() error {
	n.stopOnce.Do(func() {
		n.stopped.Store(true)
		if n.cancel != nil { // nil-safe: Stop-before-Start
			n.cancel()
		}
		done := make(chan struct{})
		go func() {
			n.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(n.cfg.StopTimeout): // default 5s (API-08)
			n.stopErr = fmt.Errorf("raft: Stop timed out after %s", n.cfg.StopTimeout)
		}
		_ = n.cfg.Transport.Close() // idempotent; safe without a prior Register (LLD §3)
	})
	return n.stopErr // all callers observe the same result (Global Invariant 3)
}

// runTicker is the leader heartbeat / election tick loop.
//
// 07-03 replaces this placeholder body with the real driver tick loop (it will
// drive node.Step(MsgTick) at the configured cadence and pump Ready()). For
// 07-02 it simply blocks until the root context is cancelled, then exits — this
// makes Start/Stop fully goleak-clean in isolation.
func (n *nodeImpl) runTicker(ctx context.Context) {
	defer n.wg.Done()
	<-ctx.Done() // 07-03 replaces this placeholder body
}

// runInbound is the transport inbound-dispatch loop.
//
// 07-03 replaces this placeholder body with the real inbound queue drain. For
// 07-02 it blocks until the root context is cancelled, then exits.
func (n *nodeImpl) runInbound(ctx context.Context) {
	defer n.wg.Done()
	<-ctx.Done() // 07-03 replaces this placeholder body
}

// runApply is the single-goroutine apply loop that drains applyCh and calls
// StateMachine.Apply in index order (LLD §5 invariant 4), advancing appliedIdx
// after each Apply returns and resolving any registered waiter.
//
// 07-03 replaces this placeholder body with the real apply loop. For 07-02 it
// blocks until the root context is cancelled, then exits.
func (n *nodeImpl) runApply(ctx context.Context) {
	defer n.wg.Done()
	<-ctx.Done() // 07-03 replaces this placeholder body
}

// Static assertion: *nodeImpl satisfies the public Node interface.
var _ Node = (*nodeImpl)(nil)
