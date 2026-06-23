# ADR-0008 — step() event loop and Ready() drain pattern

**Status:** Accepted
**Date:** 2026-06-23
**Scope:** `pkg/raft` (internal `*node` state machine + future `Ready()` /
Driver surface in plan 05-04 and Phase 7)

## Context

Phase 5 lands the election state machine on top of the single-mutex
discipline ADR-0004 already pins. The architectural shape that survives
contact with that constraint is the one etcd-io/raft popularised:
serialise every inbound event — incoming RPCs, ticks, control messages —
through one entry point, and drain everything the state machine wants to
emit through a paired output point. etcd's implementation funnels its
single entry point through a per-node goroutine and a `chan stepInput`;
ADR-0004 forbids the goroutine, so we keep the funnel and drop the
channel — the mutex provides the serialisation directly.

The competing design (hashicorp/raft's per-role goroutine fan-out)
imports the entire C-1..C-8 catalogue in `docs/CONCURRENCY.md §7`. Two
specific failure modes the per-role design cannot escape are the ones
this ADR is shaped to defeat:

1. **P0-5 — TOCTOU step-down.** A candidate goroutine that has already
   read `n.currentTerm` then sends an outbound `RequestVote`. Between
   the read and the send, the state machine has stepped down (a follower
   handler observed a higher-term AppendEntries). The outbound vote is
   stale and, depending on which peer receives it first, can corrupt a
   legitimate election in progress. The only fix in a multi-goroutine
   design is per-message double-check-on-send, which is itself racy.

2. **ELEC-08 — leadership-handoff drift.** A leader that has been
   demoted by a higher term continues to ship heartbeats from an
   outbound queue populated under the prior role. Followers that
   receive these heartbeats can incorrectly believe a leader at the
   old term is still alive, delaying convergence.

The single-mutex + step-event-loop design lets us close both at the
queue boundary with a one-line invariant: outbound messages carry an
epoch token (`stepDownEpoch`); the drain rejects any message whose
epoch is older than the current. Every step-down bumps the epoch
exactly once via `maybeStepDownLocked`, the SINGLE path that handles
the higher-term observation.

LLD §3 already specifies the public `Node` interface shape that Phase 7
will wrap around the internal `*node`; LLD §5.7 already specifies the
term-first invariant. This ADR ratifies the implementation pattern
those documents anticipate and freezes the per-role handler split
plan 05-02 / 05-03 will fill.

References: ADR-0004 (single-mutex), LLD §3 (Node interface), LLD §5.7
(term-first invariant), CONCURRENCY §4 (copy-under-lock), PITFALLS P0-5
(TOCTOU step-down), ELEC-07 + ELEC-08 (election-safety pitfalls),
RESEARCH §Pattern 1 + §Pattern 2.

## Decision

We adopt the **step()+Ready() event-loop pattern** for the internal
`*node` state machine. The pattern is fixed by four rules.

**1. `node.Step(Message) error` is the single inbound entry point.**
It acquires `n.mu` exactly once, returns `ErrStopped` if the start
sequence (LoadHardState) has not completed, and delegates to
`stepLocked(Message)`. Concurrent callers (the Phase 7 Driver, tests)
serialise on the mutex automatically — no channel funnel goroutine
exists.

**2. `stepLocked` dispatches by `MessageType` after a term-first
funnel.** The dispatcher's first action on any inbound Message is:

```go
if m.Term > n.currentTerm {
    n.maybeStepDownLocked(m.Term)
}
```

Per-role handlers (`tickFollowerLocked`, `handleRequestVoteLocked`,
…) MUST NOT inline a term check. This is the only place that
implements the funnel. Drift here re-introduces P0-5.

**3. `maybeStepDownLocked(rpcTerm)` is the SINGLE step-down path.** It
sets `currentTerm`, clears `votedFor`, drops `role` to Follower,
clears `leaderHint` and `votesReceived`, bumps `stepDownEpoch`, and
queues a HardState — exactly once per higher-term observation. No
caller is permitted to mutate these fields independently.

**4. `Ready() (msgs []Message, hs *HardState)` drains output under
the same mutex.** Outbound messages are queued via `queueMsgLocked`
which captures the current `stepDownEpoch` per message. `Ready()`
filters out any message whose epoch is older than the current
`stepDownEpoch`. HardState is queued via `queueHardStateLocked` and
returned alongside messages in the SAME call; the driver MUST persist
HardState before shipping any returned Message (SC5 — persist-first
ordering). Plan 05-04 lands the `Ready()` implementation; this ADR
freezes its contract.

## Consequences

**Positive**

- One mutex acquisition per inbound event. Tests can drive the state
  machine via direct function calls, no clock manipulation needed for
  unit-level coverage of dispatch.
- TOCTOU-free step-down via the epoch token. The drift class P0-5
  represents is structurally impossible at the drain boundary.
- Uniform term handling kills an entire drift class: every test that
  needs to assert "higher term demotes" can rely on a single
  `maybeStepDownLocked` proof point rather than auditing every handler.
- ADR-0004's single mutex compounds cleanly with this pattern; the
  per-role goroutine alternative would have required revisiting ADR-0004.
- Phase 6 replication handlers slot into existing `*Locked` stubs
  without architectural change — the contract those plans depend on
  is already frozen.

**Negative**

- `Ready()` couples HardState + Messages into the same return value.
  The Phase 7 Driver MUST handle both atomically; deferring HardState
  persistence past message shipping silently breaks SC5. The
  driver-side enforcement lands in plan 05-04.
- We pay one extra mutex acquisition per `Ready()` call versus etcd's
  channel-funnel goroutine. In exchange we eliminate a goroutine per
  node — at ToyRaft's target load (3–5 nodes, single-digit-kops) the
  contention budget covers the cost.
- The exhaustive `MessageType` switch in `stepLocked` means a Phase 6
  wire-protocol extension requires touching `step.go` even if the new
  message only concerns one role; this is the trade we make for the
  drift-prevention guarantee.

**Follow-ups**

- Plan 05-02 fills `tickFollowerLocked`, `handleRequestVoteLocked`,
  `handleAppendEntriesLocked`.
- Plan 05-03 fills `tickCandidateLocked`,
  `handleRequestVoteResponseLocked`, `becomeCandidate`, `becomeLeader`.
- Plan 05-04 lands `Ready()` (the drain side of this ADR) with epoch
  filtering and persist-first ordering enforcement.
- Phase 6 lands `tickLeaderLocked` and `handleAppendEntriesRespLocked`.
- Phase 7 wraps `*node` behind the public `raft.Node` interface in
  `node_public.go` and lands the `Driver` that calls `Step` /
  `Ready` against the public surface.

## Usage

Inside `pkg/raft`:

```go
func (n *node) Step(m Message) error {
    n.mu.Lock()
    defer n.mu.Unlock()
    if !n.started {
        return ErrStopped
    }
    return n.stepLocked(m)
}

func (n *node) stepLocked(m Message) error {
    if m.Term > n.currentTerm {
        n.maybeStepDownLocked(m.Term)
    }
    switch m.Type {
    case MsgTick:           // dispatch by role …
    case MsgRequestVote:    // handleRequestVoteLocked(m)
    case MsgAppendEntries:  // handleAppendEntriesLocked(m)
    // …
    }
    return nil
}
```

The Phase 7 driver loop will resemble:

```go
for {
    select {
    case <-clock.After(cfg.tickInterval()):
        _ = node.Step(Message{Type: MsgTick})
    case m := <-inbound:
        _ = node.Step(m)
    }
    msgs, hs := node.Ready()
    if hs != nil {
        storage.SaveHardState(*hs) // fsync BEFORE Send
    }
    for _, m := range msgs {
        transport.Send(m)
    }
}
```

The persist-first ordering between `SaveHardState` and `transport.Send`
is what SC5 requires; plan 05-04 lands the test that proves it holds.
