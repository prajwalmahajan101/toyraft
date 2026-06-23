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

// dispatch is the single delivery goroutine. The full loop body lands
// in plan 04-03 Task 2 (dispatcher.go). This Task-1 stub exists only so
// NewHub can launch + Close can join; FIFO delivery is not yet wired.
func (h *Hub) dispatch() {
	defer h.wg.Done()
	<-h.ctx.Done()
}

// send enqueues a message for delivery. FIFO semantics in plan 04-03:
// deliverAt is the current clock instant with no added delay. Plan
// 04-04 inserts the chaos decision layer between Connect/Endpoint and
// this method.
func (h *Hub) send(_ context.Context, from raft.NodeID, msg raft.Message) error {
	h.mu.Lock()
	p := &pending{
		deliverAt: h.clk.Now(),
		seq:       h.nextSeq(),
		from:      from,
		to:        msg.To,
		msg:       msg,
	}
	h.push(p)
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
