package raft

import (
	"context"
	"errors"
)

// driver.go is the I/O half of the public Node (07-03). It promotes the
// proven internal/raftest.Cluster.tickOnce loop (RESEARCH Pattern 3) to a
// real-clock tick loop, owns the commit->apply enqueue seam, and supplies the
// inproc.Endpoint -> LLD Transport adapter that 07-04's raftest harness uses to
// run a real raft.Node over the inproc Hub.
//
// The three goroutine bodies (runTicker, runInbound, runApply) replace the
// 07-02 placeholders in node_public.go; runApply + applyOne live in apply.go.

// runTicker is the real-clock promotion of internal/raftest.Cluster.tickOnce
// (RESEARCH Pattern 3). The ordering is preserved VERBATIM from the proven
// Phase-5/6 driver and is load-bearing:
//
//	tick -> Step(MsgTick) -> Ready() -> SaveHardState (SC5) -> Send -> drain commits
//
// CRITICAL (Global Invariant 1 / SC5 / Pitfall 4): SaveHardState MUST precede
// any Transport.Send. Ready() copies pendingMsgs/pendingHS under n.core.mu and
// releases the lock on return, so every Send below happens with NO lock held —
// the node NEVER sends while holding n.core.mu.
//
// Send errors are best-effort (logged at Debug, not fatal) — the Transport is
// lossy by contract (LLD §3). A tick Step error that is not ErrStopped is
// logged at Error (validation drift would otherwise hide), matching tickOnce.
func (n *nodeImpl) runTicker(ctx context.Context) {
	defer n.wg.Done()
	tk := n.cfg.Clock.NewTicker(n.cfg.tickInterval()) // == HeartbeatInterval
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C():
			if err := n.core.Step(Message{Type: MsgTick}); err != nil && !errors.Is(err, ErrStopped) {
				n.cfg.Logger.Error("raft: tick Step", "err", err)
			}
			msgs, hs := n.core.Ready() // copy-under-lock; lock released on return
			if hs != nil {             // SC5: persist HardState BEFORE any Send
				_ = n.cfg.Storage.SaveHardState(*hs)
			}
			for _, m := range msgs { // best-effort; Send errors logged, not fatal
				if err := n.cfg.Transport.Send(ctx, m); err != nil {
					n.cfg.Logger.Debug("raft: Send", "to", m.To, "err", err)
				}
			}
			n.drainCommitsToApply(ctx) // R-7 apply seam (pushes onto bounded applyCh)
		}
	}
}

// runInbound is the documented inbound seam. The LLD §3 Transport delivers
// inbound messages via the Register(step) callback (Start already called
// Transport.Register(n.Step) in 07-02), so the production inbound path is
// callback-driven and runInbound has NOTHING to poll — it simply parks until
// Stop cancels the root context. Inbound work happens in the Transport's own
// Register-spawned goroutine (e.g. the inproc->Transport adapter's pump),
// NOT here.
//
// Keeping runInbound as a ctx-parked goroutine preserves the 07-02 wg.Add(3)
// accounting (one of three goroutines) while leaving the inbound mechanism with
// the Transport, where the goleak contract (Warning-B) is satisfiable.
//
// R-5 ADAPTER PLACEMENT (Rule-3 deviation): the plan specified an
// endpointTransport adapter (inproc.Endpoint -> LLD Transport) IN THIS FILE.
// That is IMPOSSIBLE: pkg/transport/inproc imports pkg/raft, so importing
// inproc here forms the cycle pkg/raft -> inproc -> pkg/raft (the plan's own
// note "do NOT import inproc into the core driver path" anticipates this). The
// adapter therefore lands in internal/raftest (07-04), which already imports
// BOTH pkg/raft and inproc — exactly where the plan says it "exists so raftest
// (07-04) can build a real raft.Node over the inproc Hub". The Warning-B
// own-ctx+wg+Close-joins goleak contract is preserved as a documented
// obligation on that 07-04 adapter; this driver supplies only the seam.
func (n *nodeImpl) runInbound(ctx context.Context) {
	defer n.wg.Done()
	<-ctx.Done()
}

// drainCommitsToApply is the R-7 commit->apply seam. The core advances
// commitIndex (commit.go) but has no apply tracking; the DRIVER owns
// enqueuedIdx — the channel-fill frontier (the highest index handed to
// applyCh). It reads each newly-committed entry from Storage and pushes it onto
// the bounded applyCh via a ctx-aware select.
//
// CHECKER CONTRACT: this advances enqueuedIdx ONLY. It NEVER touches appliedIdx
// — the applier (apply.go applyOne) is the SOLE writer of appliedIdx (on both
// the success and panic paths). enqueuedIdx tracks "handed to the channel";
// appliedIdx tracks "StateMachine.Apply returned". Status().ApplyIndex reads
// appliedIdx, so keeping them split keeps ApplyIndex honest (entries applied,
// not merely queued).
//
// Pitfall 1 / API-05: the channel is BOUNDED (cap defaultApplyBuf); the
// ctx-aware select means a slow or full applyCh never deadlocks the tick loop
// on shutdown. enqueuedIdx advances ONLY AFTER an entry is accepted by the
// channel, so a shutdown mid-drain re-enqueues from the correct index on the
// (non-existent post-Stop) next run and never double-enqueues within a run.
func (n *nodeImpl) drainCommitsToApply(ctx context.Context) {
	n.core.mu.Lock()
	ci := n.core.commitIndex // brief copy-under-lock
	n.core.mu.Unlock()

	for i := Index(n.enqueuedIdx.Load()) + 1; i <= ci; i++ {
		ents, err := n.cfg.Storage.Entries(i, i+1) // half-open [i, i+1)
		if err != nil || len(ents) == 0 {
			n.cfg.Logger.Error("raft: read commit", "index", i, "err", err)
			return
		}
		select {
		case n.applyCh <- ents[0]: // bounded; absorbs bursts (API-05)
			n.enqueuedIdx.Store(uint64(i)) // advance the ENQUEUE frontier only
		case <-ctx.Done(): // shutdown: stop draining (Pitfall 1)
			return
		}
	}
}
