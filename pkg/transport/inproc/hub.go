package inproc

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// Hub is the in-process transport bus. NewHub returns a running Hub with
// a single dispatcher goroutine; per-node endpoints are produced by
// Connect. Shutdown is via Close (idempotent, sync.Once-guarded). Hub
// state is guarded by a single mutex per ADR-0004.
type Hub struct {
	cfg HubConfig
	clk clock.Clock

	mu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once
	wg     sync.WaitGroup

	// sortedNodes is the canonical iteration order over nodes. Map
	// iteration is never used in delivery paths (RESEARCH Pitfall 2).
	sortedNodes []raft.NodeID
	nodes       map[raft.NodeID]*nodeState

	queue *pendingHeap
	seq   uint64

	// wake is a 1-buffered signal that send uses to nudge the
	// dispatcher when it parks the heap empty. Non-blocking writers,
	// single reader (the dispatcher).
	wake chan struct{}

	// chaos owns the split-seed sub-RNGs and per-knob state for the
	// LLD §6 chaos surface (Partition / Heal / DropRate / Delay /
	// Reorder / Duplicate). All reads + writes happen under h.mu.
	chaos *chaos
}

// nodeState owns a single connected node's inbound channel.
type nodeState struct {
	id      raft.NodeID
	inbound chan raft.Message
}

// Endpoint is a per-node handle returned by Connect. Send is bound to
// this node's identity at Connect time so callers cannot spoof From.
type Endpoint struct {
	hub *Hub
	id  raft.NodeID
	in  <-chan raft.Message
}

// NewHub returns a running Hub configured by cfg. The dispatcher
// goroutine is started before return; the caller must Close to release
// it.
func NewHub(cfg HubConfig) (*Hub, error) {
	if cfg.Clock == nil {
		return nil, errors.New("inproc: HubConfig.Clock required")
	}
	if cfg.InboundCap == 0 {
		cfg.InboundCap = defaultInboundCap
	}
	if cfg.CloseTimeout == 0 {
		cfg.CloseTimeout = defaultCloseTimeout
	}

	ctx, cancel := context.WithCancel(context.Background())
	h := &Hub{
		cfg:    cfg,
		clk:    cfg.Clock,
		ctx:    ctx,
		cancel: cancel,
		nodes:  make(map[raft.NodeID]*nodeState),
		queue:  &pendingHeap{},
		wake:   make(chan struct{}, 1),
		chaos:  newChaos(cfg.Seed),
	}

	h.wg.Add(1)
	go h.dispatch()

	return h, nil
}

// Connect returns the Endpoint for id, allocating a new bounded inbound
// channel on first call. Idempotent: subsequent calls with the same id
// return an Endpoint sharing the same inbound channel.
func (h *Hub) Connect(id raft.NodeID) *Endpoint {
	h.mu.Lock()
	defer h.mu.Unlock()

	if ns, ok := h.nodes[id]; ok {
		return &Endpoint{hub: h, id: id, in: ns.inbound}
	}

	ns := &nodeState{
		id:      id,
		inbound: make(chan raft.Message, h.cfg.InboundCap),
	}
	h.nodes[id] = ns

	// Binary-insert id into sortedNodes to preserve deterministic
	// iteration order (RESEARCH Pitfall 2).
	idx := sort.Search(len(h.sortedNodes), func(i int) bool {
		return h.sortedNodes[i] >= id
	})
	h.sortedNodes = append(h.sortedNodes, "")
	copy(h.sortedNodes[idx+1:], h.sortedNodes[idx:])
	h.sortedNodes[idx] = id

	return &Endpoint{hub: h, id: id, in: ns.inbound}
}

// Close cancels the Hub's context, joins the dispatcher within
// CloseTimeout, and is safe to call from multiple goroutines (sync.Once
// guarded). Close always returns nil; a dispatcher-join overrun is a
// best-effort leak that goleak will catch in tests.
func (h *Hub) Close() error {
	h.once.Do(func() {
		h.cancel()
		done := make(chan struct{})
		go func() {
			h.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(h.cfg.CloseTimeout):
			// Dispatcher did not join in budget. Tests assert
			// against goleak post-Close; we intentionally do not
			// panic here so production callers see a clean Close
			// even when something has gone wrong.
		}
	})
	return nil
}

// nextSeq returns a monotonic ordering counter. Caller must hold h.mu.
func (h *Hub) nextSeq() uint64 {
	h.seq++
	return h.seq
}

// send enqueues a message for delivery, consulting the chaos layer
// first: partitioned or dropped messages are silently discarded; the
// surviving message receives a per-Send delay (delivered at
// clk.Now()+delay); if the dup decision fires a second copy is enqueued
// one nanosecond later so the dispatcher's (deliverAt, seq) total order
// is preserved.
func (h *Hub) send(_ context.Context, from raft.NodeID, msg raft.Message) error {
	h.mu.Lock()
	if h.chaos.isPartitioned(from, msg.To) {
		h.mu.Unlock()
		return nil
	}
	if h.chaos.dropDecision(from) {
		h.mu.Unlock()
		return nil
	}
	delay := h.chaos.sampleDelay()
	dup := h.chaos.dupDecision()
	deliverAt := h.clk.Now().Add(delay)
	h.push(&pending{
		deliverAt: deliverAt,
		seq:       h.nextSeq(),
		from:      from,
		to:        msg.To,
		msg:       msg,
	})
	if dup {
		h.push(&pending{
			deliverAt: deliverAt.Add(time.Nanosecond),
			seq:       h.nextSeq(),
			from:      from,
			to:        msg.To,
			msg:       msg,
		})
	}
	h.mu.Unlock()

	// Non-blocking wake. wake is 1-buffered; if a notification is
	// already pending the dispatcher will see this message on its next
	// drain pass anyway.
	select {
	case h.wake <- struct{}{}:
	default:
	}
	return nil
}

// Partition installs a symmetric cut between a and b: both (a -> b)
// and (b -> a) are dropped silently from Send. Asymmetric partitions
// are out of scope for v1 (RESEARCH Pitfall 7, ADR-0007).
func (h *Hub) Partition(a, b raft.NodeID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.chaos.partitions[partitionKey{A: a, B: b}] = struct{}{}
	h.chaos.partitions[partitionKey{A: b, B: a}] = struct{}{}
}

// Heal removes the symmetric cut installed by Partition. Idempotent.
func (h *Hub) Heal(a, b raft.NodeID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.chaos.partitions, partitionKey{A: a, B: b})
	delete(h.chaos.partitions, partitionKey{A: b, B: a})
}

// DropRate sets the per-Send drop probability for messages originating
// from id. p == 0 disables drops; p == 1 drops every message. Out-of-
// range values are stored as-is — the Float64 draw will simply never
// be less than 1.0 and always be at least 0.0.
func (h *Hub) DropRate(id raft.NodeID, p float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.chaos.dropPerNode[id] = p
}

// Delay sets the global per-Send delay range [min, max). When max <= min
// the deterministic min value is applied with no RNG draw (zero-span
// is the recommended way to write delay-only tests).
func (h *Hub) Delay(minDelay, maxDelay time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.chaos.delayMin = minDelay
	h.chaos.delayMax = maxDelay
}

// Reorder toggles per-receiver reorder and sets the soft bucket size.
// queueDepth is a soft minimum messages-per-receiver-per-drain; lower
// values approach FIFO (RESEARCH Pitfall 6 — queueDepth of 1
// degenerates to FIFO by construction).
func (h *Hub) Reorder(enabled bool, queueDepth int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.chaos.reorderOn = enabled
	h.chaos.reorderQD = queueDepth
}

// Duplicate sets the per-Send duplicate probability. p == 1 causes
// every surviving message to be delivered twice (the duplicate arrives
// one nanosecond after the original to preserve the dispatcher's
// (deliverAt, seq) total order).
func (h *Hub) Duplicate(p float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.chaos.dupRate = p
}

// Send delivers msg from this endpoint to msg.To. Source identity is
// taken from the endpoint binding, not msg.From — callers cannot spoof
// the sender. The returned error is always nil in plan 04-03; plan
// 04-04 may surface context.Canceled when Hub is closed.
func (e *Endpoint) Send(ctx context.Context, msg raft.Message) error {
	return e.hub.send(ctx, e.id, msg)
}

// Recv returns the receive channel for this endpoint. The Hub never
// closes this channel (CONCURRENCY.md §5); shutdown is via Hub.Close
// and the dispatcher's ctx.Done escape.
func (e *Endpoint) Recv() <-chan raft.Message { return e.in }

// ID returns the node identity this endpoint is bound to.
func (e *Endpoint) ID() raft.NodeID { return e.id }
