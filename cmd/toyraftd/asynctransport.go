package main

import (
	"context"
	"expvar"
	"sync"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// asyncTransport decouples the raft tick loop from peer network latency.
//
// WHY THIS EXISTS (a real liveness fix, not cosmetics): the pkg/raft driver
// calls Transport.Send SYNCHRONOUSLY, once per outbound message, inside the
// per-tick loop. The frozen raft.Transport contract explicitly permits a
// best-effort, fire-and-forget Send whose failures are retried by the NEXT
// heartbeat ("Returning an error does NOT cause Raft to retry; the next
// heartbeat is the retry signal"). But the underlying HTTP Send still BLOCKS
// its caller for up to SendTimeout when a peer is unreachable — and a
// kill -STOP'd node keeps its TCP port bound (the kernel completes the
// handshake) while never answering, so every Send to it blocks on read until
// the timeout. With several peers per tick, those synchronous stalls
// desynchronise heartbeats badly enough that a larger cluster (N=5) churns
// through elections (terms climbing into the dozens) and never settles a
// leader while a node is partitioned.
//
// asyncTransport wraps the real transport so Send NEVER blocks the tick loop:
// each peer gets a small buffered queue drained by a dedicated goroutine that
// does the (possibly slow) real Send. A full queue drops the message — exactly
// the loss the Raft core already tolerates, with the next heartbeat as the
// retry. This keeps the tick loop's heartbeat cadence rock-steady regardless of
// any dead/frozen peer, which is what makes re-election deterministic in the
// demo (SC5). Register/Close delegate to the wrapped transport; Close stops the
// drain goroutines first, then closes the inner transport.
type asyncTransport struct {
	inner raft.Transport

	// Observability counters (OBS-04). These live DAEMON-SIDE on the transport
	// wrapper — never in pkg/raft — so the single-mutex core (ADR-0004) and its
	// stdlib-only ethos stay untouched. rpcSent increments at the Send enqueue
	// seam; rpcReceived increments in a counting closure wrapping the registered
	// step callback (the one point every inbound message flows through).
	rpcSent     *expvar.Int
	rpcReceived *expvar.Int

	mu     sync.Mutex
	queues map[raft.NodeID]chan raft.Message
	wg     sync.WaitGroup
	stopCh chan struct{}
	once   sync.Once
}

// asyncQueueDepth bounds each peer's pending-send queue. A steady leader emits
// one heartbeat per peer per tick; a few slots absorb a burst without letting a
// slow peer back-pressure the tick loop. Overflow is dropped (best-effort).
const asyncQueueDepth = 16

// newAsyncTransport wraps inner so Send is non-blocking. The wrapper owns the
// per-peer drain goroutines; call Close to join them.
func newAsyncTransport(inner raft.Transport, rpcSent, rpcReceived *expvar.Int) *asyncTransport {
	return &asyncTransport{
		inner:       inner,
		rpcSent:     rpcSent,
		rpcReceived: rpcReceived,
		queues:      make(map[raft.NodeID]chan raft.Message),
		stopCh:      make(chan struct{}),
	}
}

// Send enqueues msg on the destination peer's queue and returns immediately —
// it NEVER blocks on the network. A full queue drops the message (best-effort;
// the next heartbeat is the retry, per the raft.Transport contract).
func (a *asyncTransport) Send(_ context.Context, msg raft.Message) error {
	// Count at enqueue (not at drain): a dropped-full-queue message still counts
	// as an ATTEMPTED send, consistent with the fire-and-forget contract where
	// the next heartbeat is the retry (raft.rpc.sent, OBS-04).
	a.rpcSent.Add(1)
	q := a.queueFor(msg.To)
	if q == nil { // Close already ran — silently drop.
		return nil
	}
	select {
	case q <- msg:
	default: // queue full: drop, next heartbeat retries
	}
	return nil
}

// queueFor returns the drain queue for peer to, lazily creating it (and its
// drain goroutine) on first use. Returns nil once Close has stopped the wrapper.
func (a *asyncTransport) queueFor(to raft.NodeID) chan raft.Message {
	a.mu.Lock()
	defer a.mu.Unlock()
	select {
	case <-a.stopCh: // stopped: no new queues
		return nil
	default:
	}
	if q, ok := a.queues[to]; ok {
		return q
	}
	q := make(chan raft.Message, asyncQueueDepth)
	a.queues[to] = q
	a.wg.Add(1)
	go a.drain(q)
	return q
}

// drain runs one peer's send loop: it performs the real (possibly slow) inner
// Send for each queued message, off the tick-loop goroutine. It exits when
// stopCh is closed. Send errors are swallowed here — the inner transport already
// logs them, and the contract makes the next heartbeat the retry.
func (a *asyncTransport) drain(q chan raft.Message) {
	defer a.wg.Done()
	for {
		select {
		case <-a.stopCh:
			return
		case msg := <-q:
			_ = a.inner.Send(context.Background(), msg)
		}
	}
}

// Register installs the inbound callback on the wrapped transport. It wraps the
// step callback in a counting closure so raft.rpc.received increments once per
// inbound wire message BEFORE delegating to node.Step — Register is the ONE
// daemon-side point every inbound message flows through (pkg/raft calls
// Transport.Register(n.Step) internally), which keeps expvar entirely out of the
// core. Inbound wire messages are never MsgTick (Tick is core-internal), but
// guard defensively so the counter can only ever reflect real received RPCs.
func (a *asyncTransport) Register(step func(ctx context.Context, msg raft.Message) error) {
	counted := func(ctx context.Context, msg raft.Message) error {
		if msg.Type != raft.MsgTick {
			a.rpcReceived.Add(1)
		}
		return step(ctx, msg)
	}
	a.inner.Register(counted)
}

// Close stops the drain goroutines (idempotent) and then closes the wrapped
// transport, releasing its listeners/connections.
func (a *asyncTransport) Close() error {
	a.once.Do(func() {
		close(a.stopCh)
		a.wg.Wait()
	})
	return a.inner.Close()
}

// compile-time proof the wrapper still satisfies the frozen interface.
var _ raft.Transport = (*asyncTransport)(nil)
