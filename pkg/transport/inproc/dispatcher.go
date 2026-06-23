package inproc

import (
	"container/heap"
	"slices"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// pending is one in-flight message destined for h.nodes[to].inbound at
// deliverAt. seq breaks (deliverAt, seq) ties deterministically so the
// dispatcher's pop order is a total order under one seed.
type pending struct {
	deliverAt time.Time
	seq       uint64
	from      raft.NodeID
	to        raft.NodeID
	msg       raft.Message
	idx       int // heap.Interface bookkeeping; -1 once popped
}

// pendingHeap is a min-heap over (deliverAt, seq). It implements
// heap.Interface; callers go through container/heap.Push / Pop so the
// invariants stay intact.
type pendingHeap []*pending

func (h pendingHeap) Len() int { return len(h) }

func (h pendingHeap) Less(i, j int) bool {
	if h[i].deliverAt.Equal(h[j].deliverAt) {
		return h[i].seq < h[j].seq
	}
	return h[i].deliverAt.Before(h[j].deliverAt)
}

func (h pendingHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idx = i
	h[j].idx = j
}

func (h *pendingHeap) Push(x any) {
	p := x.(*pending)
	p.idx = len(*h)
	*h = append(*h, p)
}

func (h *pendingHeap) Pop() any {
	old := *h
	n := len(old)
	p := old[n-1]
	old[n-1] = nil
	p.idx = -1
	*h = old[:n-1]
	return p
}

// push wraps heap.Push so callers do not have to import container/heap.
// Caller holds h.mu.
func (h *Hub) push(p *pending) {
	heap.Push(h.queue, p)
}

// peekDue returns the heap top if it is due (deliverAt <= now), else
// nil. Caller holds h.mu.
func (h *Hub) peekDue(now time.Time) *pending {
	if h.queue.Len() == 0 {
		return nil
	}
	top := (*h.queue)[0]
	if top.deliverAt.After(now) {
		return nil
	}
	return top
}

// drainDueLocked pops every pending message whose deliverAt <= now and
// returns them in (deliverAt, seq) total order. Caller holds h.mu.
func (h *Hub) drainDueLocked(now time.Time) []*pending {
	var due []*pending
	for {
		p := h.peekDue(now)
		if p == nil {
			break
		}
		heap.Pop(h.queue)
		due = append(due, p)
	}
	return due
}

// dispatch runs on a single goroutine started by NewHub. The loop:
//
//  1. Wait on (wake, h.ctx.Done, or h.clk.After(timeUntilNext)) — the
//     After branch makes FakeClock.Advance synchronously wake the
//     dispatcher when a delayed message becomes due, which is the
//     mechanism plan 04-04 chaos tests rely on for delay / reorder /
//     duplicate determinism.
//  2. Drain every due pending under h.mu via drainDueLocked.
//  3. When chaos.reorderOn is set, bucket the due batch per receiver
//     (walking h.sortedNodes for deterministic iteration order — never
//     ranging the nodes map; RESEARCH Pitfall 2) and shuffle each
//     bucket with chaos.reorderRNG under h.mu so the RNG draw is
//     deterministic w.r.t. seed.
//  4. Deliver each pending to its receiver's inbound channel; the send
//     selects on h.ctx.Done so a parked dispatcher unblocks within
//     CloseTimeout (SC4, RESEARCH Pitfall 5).
//
// No per-message goroutines: this is the single delivery goroutine
// (RESEARCH Pattern 2, Pitfall 4).
func (h *Hub) dispatch() {
	defer h.wg.Done()
	// A single reusable Timer is allocated lazily on the first loop
	// iteration that needs to park on a future deliverAt. Reusing it
	// (Stop + Reset) avoids leaking a fakeTimer into FakeClock's heap
	// on every iteration.
	var nextTimer clock.Timer
	stopTimer := func() {
		if nextTimer != nil {
			nextTimer.Stop()
		}
	}
	defer stopTimer()
	for {
		// Compute time until next due message under h.mu, then arm
		// nextTimer and select on (wake | ctx.Done | nextTimer.C).
		// When the heap is empty we park on wake / ctx.Done alone.
		h.mu.Lock()
		var armCh <-chan time.Time
		if h.queue.Len() > 0 {
			top := (*h.queue)[0]
			d := max(top.deliverAt.Sub(h.clk.Now()), 0)
			h.mu.Unlock()
			if nextTimer == nil {
				nextTimer = h.clk.NewTimer(d)
			} else {
				// Stop + drain the channel before Reset so an
				// already-fired stale value cannot race the new
				// arming. Standard time.Timer reset pattern.
				if !nextTimer.Stop() {
					select {
					case <-nextTimer.C():
					default:
					}
				}
				nextTimer.Reset(d)
			}
			armCh = nextTimer.C()
		} else {
			h.mu.Unlock()
		}
		select {
		case <-h.ctx.Done():
			return
		case <-h.wake:
			if nextTimer != nil {
				nextTimer.Stop()
			}
		case <-armCh:
		}
		for {
			h.mu.Lock()
			due := h.drainDueLocked(h.clk.Now())
			if len(due) == 0 {
				h.mu.Unlock()
				break
			}
			ordered := h.orderLocked(due)
			// Snapshot the receiver map under h.mu so the inbound
			// sends below can run lock-free (the dispatcher is the
			// only writer to inbound channels per nodeState).
			recvs := make([]*nodeState, len(ordered))
			for i, p := range ordered {
				recvs[i] = h.nodes[p.to]
			}
			h.mu.Unlock()

			for i, p := range ordered {
				ns := recvs[i]
				if ns == nil {
					// Receiver never Connected — silently drop.
					continue
				}
				select {
				case ns.inbound <- p.msg:
				case <-h.ctx.Done():
					return
				}
			}
		}
	}
}

// orderLocked applies the reorder knob to a batch drained at the same
// logical instant. With reorder disabled, the batch is returned as-is
// (already in (deliverAt, seq) total order from drainDueLocked).
//
// With reorder enabled, the batch is bucketed per receiver — iteration
// over the bucketing walks h.sortedNodes so the visit order is
// deterministic, never the nodes map (RESEARCH Pitfall 2). Each
// non-singleton bucket is shuffled by chaos.reorderRNG. queueDepth of
// 1 degenerates to FIFO by construction (RESEARCH Pitfall 6, ADR-0007)
// because a one-element bucket has nothing to permute.
//
// Caller holds h.mu so the RNG draw is deterministic.
func (h *Hub) orderLocked(due []*pending) []*pending {
	if !h.chaos.reorderOn || len(due) < 2 || h.chaos.reorderQD < 2 {
		// queueDepth < 2 degenerates to FIFO by construction — a
		// one-element shuffle window has nothing to permute
		// (RESEARCH Pitfall 6, ADR-0007).
		return due
	}
	qd := h.chaos.reorderQD
	byTo := make(map[raft.NodeID][]*pending, len(due))
	for _, p := range due {
		byTo[p.to] = append(byTo[p.to], p)
	}
	out := due[:0]
	emit := func(bucket []*pending) {
		// Walk the bucket in qd-sized chunks; shuffle each chunk
		// independently. With qd>=len(bucket) this is a single
		// shuffle of the whole bucket; with qd<len(bucket) the
		// dispatcher emits qd-sized permuted windows in order.
		for start := 0; start < len(bucket); start += qd {
			end := min(start+qd, len(bucket))
			chunk := bucket[start:end]
			if len(chunk) > 1 {
				h.chaos.reorderRNG.Shuffle(len(chunk), func(i, j int) {
					chunk[i], chunk[j] = chunk[j], chunk[i]
				})
			}
			out = append(out, chunk...)
		}
	}
	for _, to := range h.sortedNodes {
		emit(byTo[to])
		delete(byTo, to)
	}
	// Any receiver not in sortedNodes (should not happen — Send only
	// enqueues for connected ids — but defensive): append in nodeID
	// order to keep this branch deterministic too.
	if len(byTo) > 0 {
		remainder := make([]raft.NodeID, 0, len(byTo))
		for id := range byTo {
			remainder = append(remainder, id)
		}
		slices.Sort(remainder)
		for _, to := range remainder {
			emit(byTo[to])
		}
	}
	return out
}
