package inproc

import (
	"container/heap"
	"time"

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

// dispatch runs on a single goroutine started by NewHub. The loop:
//
//  1. Block on wake OR h.ctx.Done.
//  2. While the heap has a due message, pop it under h.mu and deliver
//     to the receiver's inbound channel.
//
// Both the outer wait and the inbound send escape via h.ctx.Done so
// that Close unblocks a parked dispatcher within CloseTimeout (SC4).
// No per-message goroutines: this is the single delivery goroutine
// (RESEARCH Pattern 2, Pitfall 4).
func (h *Hub) dispatch() {
	defer h.wg.Done()
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-h.wake:
		}
		for {
			h.mu.Lock()
			p := h.peekDue(h.clk.Now())
			if p == nil {
				h.mu.Unlock()
				break
			}
			heap.Pop(h.queue)
			ns, ok := h.nodes[p.to]
			h.mu.Unlock()
			if !ok {
				// Receiver never Connected — silently drop.
				continue
			}
			// Load-bearing escape: without this select branch a
			// parked dispatcher could never unblock on Close
			// (RESEARCH Pitfall 5, SC4).
			select {
			case ns.inbound <- p.msg:
			case <-h.ctx.Done():
				return
			}
		}
	}
}
