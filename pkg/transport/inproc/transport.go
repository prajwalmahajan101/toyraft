package inproc

import (
	"context"
	"sync"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// hubTransport adapts a Hub-bound *Endpoint (the channel-PULL API) to the LLD §3
// raft.Transport PUSH interface the public raft.Node consumes. It is the additive
// reconciliation of pkg/transport/inproc to the frozen Transport surface
// (ADR-0014): the Endpoint/Recv pull API is left untouched — 5 test files plus
// internal/raftest depend on it — and Hub.Transport(id) hands out this wrapper.
//
// The pump is COPIED VERBATIM from the proven internal/raftest/cluster.go
// endpointTransport adapter (goleak-clean, RESEARCH Pattern 1):
//
//   - It owns its OWN ctx + cancel + sync.WaitGroup. The Hub NEVER closes an
//     Endpoint's Recv() channel (CONCURRENCY.md §5), so the pump exits on its
//     own ctx, not on a channel close.
//   - Register spawns one pump goroutine draining Recv() into step until Close.
//   - Close (sync.Once) cancels the ctx and JOINS the pump before returning, so
//     goleak sees no residual goroutine.
//
// DISCONNECT SEMANTIC = PUMP-STOP (ADR-0014): Close stops THIS node's inbound
// delivery to its step ONLY. The Hub and every other endpoint keep running; the
// Hub is not told to forget the node. This mirrors the HTTP transport, where
// Close stops the listener but the process persists.
type hubTransport struct {
	ep     *Endpoint
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
}

// compile-time assertion: hubTransport satisfies the frozen raft.Transport.
var _ raft.Transport = (*hubTransport)(nil)

// newHubTransport wraps ep with a fresh adapter ctx derived from Background. The
// pump lifetime is bounded solely by Close(), independent of any caller ctx.
func newHubTransport(ep *Endpoint) *hubTransport {
	ctx, cancel := context.WithCancel(context.Background())
	return &hubTransport{ep: ep, ctx: ctx, cancel: cancel}
}

// Send routes an outbound message through the Hub (and its chaos layer, if any).
// Source identity is the endpoint binding, not msg.From. Send errors propagate
// to the driver, which logs-and-drops per the lossy-Transport contract (LLD §3).
func (t *hubTransport) Send(ctx context.Context, msg raft.Message) error {
	return t.ep.Send(ctx, msg)
}

// Register spawns the inbound pump. A nil step is a safe no-op (matches the pull
// adapter and the raft.Transport C8 conformance case). The pump selects on the
// adapter ctx — never on a Recv() close, because the Hub never closes Recv — and
// delivers each inbound message to step. step errors are logged-and-dropped at
// the transport edge (LLD §3): a validation/ErrStopped error must not kill the
// pump, so the return is deliberately ignored.
func (t *hubTransport) Register(step func(ctx context.Context, msg raft.Message) error) {
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
				_ = step(t.ctx, msg)
			}
		}
	}()
}

// Close cancels the adapter ctx and JOINS the pump (sync.Once-guarded,
// idempotent — second Close returns nil, no panic, per LLD §3 and C4). The join
// guarantees goleak sees no residual pump goroutine. Pump-stop only: the Hub and
// other endpoints are unaffected (ADR-0014, SC6).
func (t *hubTransport) Close() error {
	t.once.Do(func() {
		t.cancel()
		t.wg.Wait()
	})
	return nil
}
