# ToyRaft — CONCURRENCY

**Status:** Accepted
**Date:** 2026-06-18
**Scope:** `pkg/raft` core + driver, `pkg/storage`, `pkg/transport`

## Purpose

This document is the goroutine + lock + shutdown rulebook for the entire
codebase. Every later phase reasons against the model declared here. The
Concurrency pitfall catalogue (`research/PITFALLS.md` §Concurrency C-1
through C-8) is reproduced at the bottom of this document with the rule
in this doc that prevents each one.

If you find yourself about to add a second mutex, spawn an unsupervised
goroutine, send on a channel you didn't create, or hold a lock across a
network call — stop and re-read this document. One of the rules below
forbids it.

---

## 1. Goroutine census

Every long-lived goroutine in a running ToyRaft node is listed here.
A goroutine not in this table is a bug.

| Name                     | Owner          | Lifetime                                | Who closes / joins it                                    |
| ------------------------ | -------------- | --------------------------------------- | -------------------------------------------------------- |
| Tick loop                | `Node` (core)  | `Start()` → `Stop()`                    | `Stop()` cancels ctx; ticker drains and returns          |
| Apply loop               | `Node` (core)  | `Start()` → apply channel close         | `Stop()` drains then closes the apply channel; loop exits |
| Transport listener       | HTTP transport | `Start()` → transport `Close()`         | `Stop()` calls `Transport.Close()`; `http.Server.Shutdown` |
| Transport per-peer sender | HTTP transport | first send to peer → ctx-cancel         | Sender selects on `ctx.Done()`; cancelled by `Stop()`     |
| FakeClock advance loop   | `internal/clock` | test setup → test teardown            | Test owns the FakeClock; `t.Cleanup` cancels its ctx     |

Notes:

- The tick loop is the **single writer** of core Raft state (term, role,
  log, commitIndex, votedFor). All inbound events (RPCs, ticks,
  proposals) are funnelled to it through bounded channels; the loop
  processes them serially under the single mutex (§2).
- The apply loop is a strictly downstream consumer: it reads committed
  entries off the apply channel and invokes `StateMachine.Apply`. It
  NEVER mutates Raft state (no commitIndex writes, no role transitions,
  no log writes). This is what makes `Apply` safely synchronous from the
  consumer's perspective without blocking replication.
- The transport listener and per-peer senders are owned by the
  Transport implementation (`pkg/transport/http`). The driver hands them
  a context; cancellation is the shutdown signal.
- The FakeClock advance loop is **test-only**; it does not exist in
  production binaries. It is enumerated here so that test code that
  forgets to cancel it surfaces as a goleak failure (see §6, API-08).

Verification target: `goleak` baseline assertion at the end of every
test in `pkg/raft` and `internal/raftest` — `runtime.NumGoroutine()`
delta must be 0 between `TestMain` start and end (modulo runtime-owned
goroutines).

---

## 2. Single-mutex policy

One `sync.Mutex` named `mu` on the `Node` struct guards the entire
core Raft state. Forward reference: this is **ADR-0001** (to be written
in Phase 2 alongside the first implementation commit).

### Rule

```go
type Node struct {
    mu sync.Mutex // guards everything below

    // Persistent state on every server (Raft Figure 2)
    currentTerm Term
    votedFor    NodeID
    log         []Entry

    // Volatile state on every server
    commitIndex Index
    lastApplied Index
    role        Role

    // Volatile state on leaders only
    nextIndex  map[NodeID]Index
    matchIndex map[NodeID]Index

    // Driver / lifecycle
    lastHeartbeat time.Time
    leaderHint    NodeID
}
```

### Rationale

- **Simpler reasoning.** With one mutex, there is no lock-ordering
  graph to maintain. Every state read is `mu.Lock()` / read / `mu.Unlock()`;
  every state write is the same. The Raft safety properties (Election
  Safety, Leader Append-Only, Log Matching, Leader Completeness, State
  Machine Safety) all reason about transitions on the single state
  bundle — one mutex matches that mental model.
- **Eliminates an entire class of pitfalls.** C-1 (lock-ordering
  deadlock between state mutex and log mutex) cannot occur if there is
  only one mutex.
- **Matches production references.** `etcd-io/raft` is single-threaded
  via channels; `hashicorp/raft` uses a single mutex per Raft instance.
  See `research/PITFALLS.md` §C-1 for the canonical write-up.

### Tradeoff

- **Contention under heavy load.** Every RPC handler, every tick, every
  client proposal contends on `mu`. Acceptable for the toy-bar target
  (3-node demo, single-digit-thousands ops/sec); revisit only if
  benchmarks force the issue. The path to revisit is a new ADR that
  supersedes ADR-0001, not silent sharding of the lock.

### What `mu` does NOT guard

- The **storage handle** (file descriptors, write buffers inside
  `pkg/storage/file`). Storage methods are called from inside the
  critical section but the I/O itself is the slow part; see §3 and §4
  for the rule on holding `mu` across calls.
- The **transport listener** (`http.Server`). It runs in its own
  goroutine; its mutation surface is `Start` / `Close`, both invoked
  from the driver, not from inside the tick loop.
- The **apply channel** ownership (creation, close). The driver owns it;
  see §5.

---

## 3. Lock hierarchy

There is only one application-level mutex (`Node.mu`), so a hierarchy is
nominal — but the rule below codifies the order in which resources are
**acquired** when more than one is touched in a single critical section.

```
Node.mu  →  Log access (internal to Node, no separate mutex)  →  Storage handle
```

### Rules

1. `Node.mu` is the **outermost** lock. It is acquired first because
   nearly every operation touches core state.
2. Log access is performed under `Node.mu` (since the log slice is part
   of the state guarded by `Node.mu`). There is no separate "log mutex";
   listing the log here is documentation of the conceptual ordering for
   future contributors who may be tempted to split it out (don't —
   re-read §2).
3. The **storage handle** is the innermost — I/O is the slowest layer,
   and storage methods (`Storage.Append`, `Storage.SaveHardState`) MUST
   not call back into `Node`. The dependency direction is strict:
   `Node` → `Storage`, never the reverse.

### Why this order

- State mutex outermost: nearly every operation reads or writes
  `currentTerm`, `role`, or `log`. Putting `mu` second would force every
  operation to acquire two locks just to read state.
- Storage innermost: `Storage.Append` performs `fsync`, which is the
  slowest call in the system. We hold `mu` across this call only when
  REPL-09 demands it (fsync-before-RPC-response). The alternative —
  releasing `mu` during fsync — would require a re-check of role and
  term after the fsync returns, with attendant TOCTOU complexity. v1
  trades latency for correctness simplicity.

### Never invert

Inverting (e.g., holding a storage write-batch lock while calling back
into `Node`) is forbidden. Storage implementations MUST NOT spawn
goroutines that call into `Node` callbacks. If a future storage
implementation wants async batching, the batching layer lives in
`pkg/storage` and is invisible to `Node`.

---

## 4. Copy-under-lock pattern

Outbound RPCs (`RequestVote`, `AppendEntries`, `RequestVoteResponse`,
`AppendEntriesResponse`) are constructed under `mu` but **sent without
it**. Holding `mu` across `Transport.Send` is forbidden — it would
deadlock with an inbound `step()` (PITFALL C-2 / C-5) when the peer
synchronously replies through the same transport on the same goroutine
pool.

### The pattern

```go
// CORRECT — copy-under-lock, send without lock.
func (n *Node) broadcastAppendEntries() {
    n.mu.Lock()
    if n.role != RoleLeader {
        n.mu.Unlock()
        return
    }
    // Snapshot everything we need to send. These are value-typed copies;
    // mutating the local copy after Unlock does not affect Node state.
    snapshots := make([]Message, 0, len(n.peers))
    for _, peer := range n.peers {
        prevIdx := n.nextIndex[peer] - 1
        prevTerm := n.termAt(prevIdx)
        entries := append([]Entry(nil), n.log[n.nextIndex[peer]-n.logBase:]...)
        snapshots = append(snapshots, Message{
            Type:         MsgAppendEntries,
            From:         n.id,
            To:           peer,
            Term:         n.currentTerm,
            PrevLogIndex: prevIdx,
            PrevLogTerm:  prevTerm,
            Entries:      entries,
            LeaderCommit: n.commitIndex,
        })
    }
    n.mu.Unlock()

    // Send happens OUTSIDE the lock. Transport.Send may block on network
    // I/O; any inbound RPC arriving on another goroutine can still take
    // mu and make progress.
    for _, msg := range snapshots {
        n.transport.Send(msg.To, msg) // best-effort; errors logged
    }
}

// WRONG — holds mu across Send. Will deadlock with an inbound RPC that
// needs mu (the inbound handler will block on mu while this goroutine
// is blocked in Send waiting for the very same peer to respond).
func (n *Node) broadcastAppendEntriesBAD() {
    n.mu.Lock()
    defer n.mu.Unlock()
    for _, peer := range n.peers {
        msg := n.buildAppendEntries(peer)
        n.transport.Send(peer, msg) // DO NOT DO THIS
    }
}
```

### Re-check on return

If the response is processed back through the tick loop's inbound
channel (the standard path), no re-check is needed at the call site
because the inbound handler will re-acquire `mu` and check term/role
itself. If for any reason a future code path needs to act on the result
synchronously (it shouldn't), it MUST re-acquire `mu` and re-validate
`role` and `currentTerm` before mutating state — between the `Unlock()`
and the `Lock()`, state may have changed.

### What this prevents

- **C-2** (handler holds mutex across outbound RPC → deadlock with
  inbound handler needing the same mutex).
- **C-5** (send on a channel that has been closed by shutdown — see §5
  for the channel-ownership rule that also addresses this).
- Cascading timeouts under partition: with copy-under-lock, a slow peer
  delays only its own send, not the entire node's RPC fan-out.

---

## 5. Channel ownership

Every channel in ToyRaft has a single owner. The owner is the goroutine
or struct that creates the channel and is the only entity that may
close it. Senders that are not the owner MUST send under a
`select { case ch <- v: case <-ctx.Done(): }` to avoid blocking on a
shut-down channel.

| Channel               | Owner | Created in       | Closed in             | Senders                    | Receivers          |
| --------------------- | ----- | ---------------- | --------------------- | -------------------------- | ------------------ |
| `applyCh`             | Node  | `NewNode`        | `Stop()` after drain  | tick loop                  | apply loop         |
| `proposeCh`           | Node  | `NewNode`        | `Stop()` after drain  | `Node.Propose` callers     | tick loop          |
| `inboundCh`           | Node  | `NewNode`        | NEVER closed (drained on ctx-cancel) | transport listener         | tick loop          |
| `tickerCh` (`Clock.NewTicker().C`) | Clock | `NewTicker`     | `Ticker.Stop()`       | clock implementation       | tick loop          |
| `done` (per-peer sender) | per-peer sender goroutine | sender start | sender exit | sender itself | sender itself (select) |

### The apply channel — worked example

```go
// applyCh is unbuffered: the tick loop blocks on send until the apply
// loop is ready, providing natural backpressure (committed entries
// cannot pile up in memory). If a consumer's StateMachine.Apply is slow,
// the tick loop slows down — which is the correct behaviour.
applyCh := make(chan Committed)

// The tick loop sends committed entries:
select {
case n.applyCh <- Committed{Index: idx, Entry: entry}:
case <-n.ctx.Done():
    return // shutdown in progress; do not send
}

// The apply loop reads them:
for committed := range n.applyCh {
    n.sm.Apply(committed.Index, committed.Entry)
}
// When applyCh is closed (by Stop() — see §6), the range loop exits.
```

### Why `inboundCh` is never closed

Closing a channel that producers (the transport listener, multiple
per-peer senders for incoming RPC replies) might still be writing to
panics (`send on closed channel` — C-5). The transport is shut down
**first** (see §6 step 4); after that, no new sends will occur, and
the tick loop draining `inboundCh` is safe even without close. On
context cancellation, the tick loop simply returns without draining
further.

### `sync.Once` for closes

Any close that is reachable from more than one shutdown path is
protected by `sync.Once`:

```go
n.closeApplyOnce.Do(func() { close(n.applyCh) })
```

This prevents the second-close panic if `Stop()` is called twice
(API-08: `Stop()` is idempotent).

---

## 6. Shutdown ordering

`Stop()` is the sole shutdown entry point. It is idempotent (calling it
twice has the same effect as calling it once) and bounded
(`Config.StopTimeout`, default 5 seconds — see `docs/LLD.md` §Config).

### The sequence (numbered, never reorder)

1. **Cancel context.** `n.cancel()` signals every goroutine that holds
   `n.ctx` to begin shutdown. The tick loop, apply loop, transport
   listener, and per-peer senders all select on `ctx.Done()`.
2. **Tick loop exits.** It observes `ctx.Done()`, drains `proposeCh` and
   `inboundCh` of any in-flight events (replying `ErrStopped` to
   proposals), then returns. No more entries will be appended to
   `applyCh` after this point.
3. **Drain `applyCh` (then close).** The tick loop, as the sole sender,
   closes `applyCh` after step 2 completes:
   `n.closeApplyOnce.Do(func() { close(n.applyCh) })`. The apply loop
   drains any remaining committed entries — calling `StateMachine.Apply`
   for each so that `lastApplied` reflects everything that was committed
   before shutdown — then exits when the range loop ends.
4. **Close transport.** `n.transport.Close()` calls
   `http.Server.Shutdown(ctx)` (graceful — finishes in-flight
   handlers; uses the same `ctx` so it inherits the StopTimeout
   deadline) and signals per-peer senders to exit. After this point,
   no new RPCs (inbound or outbound) are accepted.
5. **Join goroutines.** `Stop()` waits on a `sync.WaitGroup` for the
   tick loop, apply loop, transport listener, and all per-peer senders
   to return. If `StopTimeout` elapses before they all join, `Stop()`
   returns `context.DeadlineExceeded` and logs which goroutine failed
   to join (debug aid — surfacing leaks).
6. **Close storage handles.** `n.storage.Close()` flushes any unflushed
   buffers (there should be none — `Append` and `SaveHardState` fsync
   before returning) and closes file descriptors. This is the last
   step so that any apply-loop drain in step 3 that touched storage
   has already returned.

### Default timeout

`Config.StopTimeout = 5 * time.Second` (see `docs/LLD.md`). The 5-second
bar covers a slow `http.Server.Shutdown` waiting for an in-flight RPC
plus a slow final `StateMachine.Apply` on a drained entry. If your apply
takes longer than 5 seconds, raise the timeout in `Config` rather than
sharding the lock or backgrounding apply — see §1 on the apply-loop
contract.

### Goroutine-leak guarantee

After `Stop()` returns (with `nil` or `DeadlineExceeded`), every
goroutine listed in §1 has either returned or been explicitly logged as
leaked. The `goleak` baseline assertion in tests catches regressions.

### Why this order

- Cancel-first ensures every goroutine has a uniform shutdown signal.
- Tick before apply ensures no further committed entries appear on
  `applyCh` after we begin draining (otherwise the drain may never
  finish).
- Drain-then-close `applyCh` so committed entries are not silently
  discarded — `lastApplied` reflects everything that was committed.
- Transport close before goroutine-join so blocked senders observe
  cancellation (the `http.Server.Shutdown` returns waiters with
  `http.ErrServerClosed`).
- Storage close last so no goroutine that might touch storage is still
  running (this is what makes the storage `Close()` safe to perform
  outside `mu`).

---

## 7. Pitfall catalogue (C-1 through C-8)

Each row reproduces the pitfall from `research/PITFALLS.md` §Concurrency
and names the rule in this document that prevents it. If a future change
introduces a new concurrency pitfall, append a new row here AND add the
preventing rule above.

| #   | Pitfall                                                    | Symptom                                                                   | Prevention (rule in this doc)                                                                       |
| --- | ---------------------------------------------------------- | ------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| C-1 | Lock-ordering deadlock between state mutex and log mutex   | Goroutines stalled in pprof; cluster unresponsive                         | §2 — single mutex. There is no second mutex to invert against.                                       |
| C-2 | RPC handler holds the mutex while making outbound RPCs     | Cascading RPC timeouts; election storm; pprof shows handler blocked in `Send` | §4 — copy-under-lock. `mu` is never held across `Transport.Send`.                                  |
| C-3 | RPC handlers blocking the main Raft loop                   | Tick loop stalled; heartbeats stop; spurious re-elections                 | §1 — tick loop processes bounded channels with `select`; §5 — `inboundCh` is bounded.                |
| C-4 | Goroutine leak on partition / shutdown                     | `NumGoroutine()` grows across tests; OOM under churn                      | §1 — every long-lived goroutine takes `ctx`; §6 — `Stop()` joins on `WaitGroup` with timeout.        |
| C-5 | Send on closed channel after node shutdown                 | Process-killing panic (`send on closed channel`) — takes consumer down    | §5 — `inboundCh` is never closed; closes that are reachable from multiple paths use `sync.Once`.    |
| C-6 | `time.Now()` races with the election timer                 | Spurious elections; data race under `-race`                               | §2 — `lastHeartbeat` lives under `mu`; the election ticker reads under `mu` from the tick loop.      |
| C-7 | `time.Timer.Reset` bug — stale event fires after heartbeat | Spurious election after heartbeat received                                | §1 — election timing is driven by a single `Clock.Ticker` consumed by the tick loop, not per-event timers. |
| C-8 | `map` iteration order assumed stable for peer ordering     | Tests pass on Linux, fail under `-race` or other platforms                | §2 — peer IDs are sorted once at construction and iterated in slice order; matchIndex quorum sorts a copy. |

---

## Cross-references

- `docs/LLD.md` §Node — interface signatures (`Start`, `Stop`, `Propose`,
  `Step`) that the concurrency model implements.
- `docs/LLD.md` §Config — `StopTimeout`, `HeartbeatInterval`,
  `ElectionTimeoutMin`, `ElectionTimeoutMax`.
- `docs/LLD.md` §Storage — invariants that constrain the storage
  innermost-lock ordering (REPL-09 fsync-before-RPC).
- `docs/FLOWS.md` §Election + §Replication — the per-flow sequence
  diagrams that the goroutine census serves.
- `docs/TESTING.md` — `goleak` baseline assertion, deterministic chaos
  contract that exploits the concurrency model.
- `research/PITFALLS.md` §Concurrency — source for C-1..C-8.
- Future ADR-0001 — single-mutex policy ratified.
- Future ADR-0013 — shutdown via `context.Context`, not channel close.
