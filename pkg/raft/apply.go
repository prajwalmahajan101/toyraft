package raft

import (
	"context"
	"fmt"
	"runtime/debug"
)

// apply.go is the consumer half of the apply channel (07-03). runApply is the
// SINGLE applier goroutine (LLD §5 invariant 4 — exactly one goroutine calls
// StateMachine.Apply, in index order); applyOne calls Apply with a defer/recover
// so a consumer panic flips the node to fatal-status WITHOUT killing the applier
// (SC4), and signals any blocked Propose waiter on both the success and panic
// paths.
//
// SINGLE-WRITER DISCIPLINE: the applier is the SOLE writer of appliedIdx (the
// driver advances enqueuedIdx only). appliedIdx is advanced on BOTH the success
// path (post-Apply) and the panic path (the entry is committed-by-definition, so
// ApplyIndex must move past it or the node would re-block on the poisoned index
// forever). Status().ApplyIndex reads appliedIdx.

// runApply is the single-goroutine apply loop. It drains the bounded applyCh and
// calls applyOne for each entry, in the channel's (index) order. It exits when
// the root context is cancelled (Stop). One goroutine, so Apply is never
// concurrent (API-05 / LLD §5 invariant 4).
func (n *nodeImpl) runApply(ctx context.Context) {
	defer n.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-n.applyCh:
			n.applyOne(e)
		}
	}
}

// applyOne calls StateMachine.Apply for one committed entry, advances appliedIdx,
// and signals any registered proposal waiter.
//
// SC4 / API-06 / Pitfall 3: a panic inside the consumer's Apply is RECOVERED.
// The stack is logged via slog (debug.Stack), the node flips to fatal-status
// (n.fatal), appliedIdx is advanced past the poisoned index (the entry IS
// committed — re-blocking on it would wedge the applier), and any blocked
// Propose is unblocked with the apply error. The goroutine then KEEPS DRAINING
// — a single bad Apply must not stop replication delivery (SC4).
func (n *nodeImpl) applyOne(e Entry) {
	defer func() {
		if r := recover(); r != nil {
			n.cfg.Logger.Error("raft: StateMachine.Apply panicked",
				"index", e.Index, "term", e.Term, "panic", r, "stack", string(debug.Stack()))
			err := fmt.Errorf("apply panic at index %d: %v", e.Index, r)
			n.fatal.Store(&err)                 // node-level fatal-status (API-06)
			n.appliedIdx.Store(uint64(e.Index)) // entry IS committed; mark consumed so
			//                                     ApplyIndex advances + drain continues (SC4)
			if ch, ok := n.waiters.LoadAndDelete(e.Index); ok {
				ch.(chan proposeResult) <- proposeResult{err: err}
			}
		}
	}()
	res, err := n.cfg.StateMachine.Apply(e) // single-goroutine, in index order (API-05)
	n.appliedIdx.Store(uint64(e.Index))     // SOLE writer of appliedIdx (success path);
	//                                         this is what Status().ApplyIndex reads.
	if ch, ok := n.waiters.LoadAndDelete(e.Index); ok {
		ch.(chan proposeResult) <- proposeResult{res: res, err: err}
	}
}

// Fatal returns the node-level fatal error set when a StateMachine.Apply call
// panicked (API-06), or nil if the applier is healthy. This is a method on the
// concrete *nodeImpl (NOT part of the frozen Node interface) — callers that need
// to observe apply-panic fatal-status type-assert to the fatalReporter interface
// or the *nodeImpl type. RESEARCH Open-Q 4 leaves the surface to us; exposing it
// off-interface keeps the frozen LLD §3 Node signature byte-identical.
func (n *nodeImpl) Fatal() error {
	if p := n.fatal.Load(); p != nil {
		return *p
	}
	return nil
}

// fatalReporter is the optional interface a Node concrete type may satisfy to
// surface apply-panic fatal-status without widening the frozen Node interface.
// TestApplyPanic type-asserts Node to this.
type fatalReporter interface {
	Fatal() error
}

// Compile-time assertion: *nodeImpl exposes the off-interface fatal surface.
var _ fatalReporter = (*nodeImpl)(nil)
