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
