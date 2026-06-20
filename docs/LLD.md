# LLD — ToyRaft Low-Level Design

**Status:** v1 frozen
**Scope:** Go public API surface for `pkg/raft`, `pkg/storage`, `pkg/transport`, `pkg/kvsm`, plus `internal/clock` and `internal/raftest` test fixtures consumers may depend on.

> **Source of truth.** This document locks the Go **shape** (names, types, invariants) that every later phase must compile against. Phase 2+ provides bodies. Drift between LLD and code is a review-blocking finding (PROJECT.md Working Agreement 4).
>
> **Companion docs.** `docs/WIRE.md` is the JSON projection of [`Message`](#message). `docs/CONCURRENCY.md` is the goroutine and lock model that drives these interfaces.

---

## 1. Package surface

| Package                              | Role                                                                                  | Stability         |
| ------------------------------------ | ------------------------------------------------------------------------------------- | ----------------- |
| `pkg/raft`                           | Consensus core, public types, sentinel errors, `Node` API.                            | Stable (semver).  |
| `pkg/storage`                        | `Storage` / `LogStorage` / `StateStorage` interfaces + `ErrSnapshotUnsupported`.      | Stable.           |
| `pkg/storage/memory`                 | In-RAM `Storage` for tests and ephemeral clusters.                                    | Stable.           |
| `pkg/storage/file`                   | Append-only file-backed `Storage` with fsynced HardState.                             | Stable.           |
| `pkg/storage/storagetest`            | Conformance suite a third-party `Storage` implementor can run.                        | Stable test API.  |
| `pkg/transport/inproc`               | In-process `Hub` + chaos knobs for deterministic multi-node tests.                    | Stable test API.  |
| `pkg/transport/http`                 | HTTP/JSON `Transport` implementation.                                                 | Stable.           |
| `pkg/transport/transporttest`        | Conformance suite a third-party `Transport` implementor can run.                      | Stable test API.  |
| `pkg/kvsm`                           | Reference KV `StateMachine` for the demo binary.                                      | Stable as a demo. |
| `internal/clock`                     | `FakeClock` driver for deterministic tests.                                           | Internal.         |
| `internal/raftest`                   | Test-cluster spin-up helpers used by `test/chaos` and `test/linearizability`.         | Internal.         |

Consumers import only `pkg/...`. The `internal/...` tree is reserved per Go module conventions.

---

## 2. Public types

All types live in `pkg/raft` unless noted.

### NodeID, Term, Index, Role

```go
// NodeID is a stable, human-readable identifier for a cluster member.
// Must be non-empty, unique across Config.Peers, and stable across restarts.
type NodeID string

// Term is Raft's monotonically increasing logical clock.
type Term uint64

// Index is a 1-based position in the replicated log. Index 0 is reserved
// for the implicit "before the first entry" sentinel used by AppendEntries
// consistency checks.
type Index uint64

// Role is a Raft server state.
type Role uint8

const (
    Follower Role = iota
    Candidate
    Leader
)
```

### Entry

```go
// Entry is one replicated log record.
//
// Invariants:
//   - Term is the term in which the leader created the entry.
//   - Index is 1-based and strictly increasing within a log.
//   - Data is opaque to Raft; the StateMachine defines its meaning.
//   - Once an Entry at (Term, Index) is committed, no other Entry with the
//     same Index may ever be committed (Log Matching, Raft §5.3).
type Entry struct {
    Term  Term
    Index Index
    Data  []byte
}
```

### MessageType

```go
// MessageType discriminates Raft RPCs.
//
// Wire-visible values 0..3 are frozen by docs/WIRE.md and append-only;
// renumbering is a wire break.
//
// Tick is internal-only — it drives the core state machine from the
// driver's tick loop and MUST NOT appear on any Transport.Send call.
type MessageType uint8

const (
    MsgRequestVote         MessageType = 0
    MsgRequestVoteResponse MessageType = 1
    MsgAppendEntries       MessageType = 2
    MsgAppendEntriesResp   MessageType = 3

    MsgTick MessageType = 255 // internal-only; not wire-visible
)
```

### Message (worked struct)

`Message` is the single envelope carrying every Raft RPC. Per-`MessageType` field usage is documented inline; unused fields MUST be zero on the wire. `docs/WIRE.md` mirrors this struct as JSON.

```go
// Message is the on-the-wire and in-process Raft RPC envelope.
//
// Invariants:
//   - Type, Term, From, To are always set (Tick excepted, which has only Type).
//   - Receivers MUST treat Term > currentTerm as a step-down trigger
//     BEFORE inspecting any other field (Raft §5.1).
//   - The sender MUST NOT mutate a Message after handing it to Transport.Send;
//     transports may queue or fan-out asynchronously.
//   - Entries slices are shared by reference within a process; consumers MUST
//     copy before mutating.
type Message struct {
    Type MessageType
    Term Term
    From NodeID
    To   NodeID

    // RequestVote / RequestVoteResponse
    LastLogIndex Index // RequestVote: candidate's last log index
    LastLogTerm  Term  // RequestVote: candidate's last log term
    VoteGranted  bool  // RequestVoteResponse: vote outcome

    // AppendEntries / AppendEntriesResponse
    PrevLogIndex Index   // AE: index immediately preceding Entries
    PrevLogTerm  Term    // AE: term of log[PrevLogIndex]
    Entries      []Entry // AE: entries to append (empty for heartbeat)
    LeaderCommit Index   // AE: leader's known commit index
    Success      bool    // AE response: consistency-check outcome
    MatchIndex   Index   // AE response: follower's last matching index

    // Fast-rollback hints (Ongaro §5.3 optimisation)
    ConflictTerm  Term  // AE response: term at the divergence point, or 0
    ConflictIndex Index // AE response: first index with ConflictTerm, or
                        //              follower's lastIndex+1 if its log is too short
}
```

### HardState

```go
// HardState is the durably-persisted slice of Raft state.
//
// Invariants:
//   - Storage.SaveHardState MUST fsync before returning.
//   - A node MUST NOT emit any RPC that depends on a (Term, VotedFor) pair
//     until that pair is durable (REPL-09).
//   - Commit is persisted for fast restart but is also recoverable from the log;
//     loss of Commit on crash is safe but slower to converge.
type HardState struct {
    CurrentTerm Term
    VotedFor    NodeID // empty when no vote cast in CurrentTerm
    Commit      Index  // best-effort; may be stale after crash
}
```

### Config

```go
// Config is the per-node Raft configuration passed to raft.New.
//
// Invariants:
//   - NodeID is non-empty and appears in Peers.
//   - Peers contains all v1 cluster members (static membership).
//   - Storage, Transport, StateMachine are non-nil.
//   - 0 < HeartbeatInterval < ElectionTimeoutMin <= ElectionTimeoutMax.
//   - Logger defaults to slog.New(slog.NewTextHandler(io.Discard, nil)).
//   - Seed of 0 means "use a non-deterministic seed from time.Now"; tests
//     SHOULD set an explicit Seed for reproducibility.
//   - Zero-value Config is NOT valid; raft.New returns an error naming
//     the first invalid field (FOUND-05).
type Config struct {
    NodeID       NodeID
    Peers        []NodeID
    Storage      Storage
    Transport    Transport
    StateMachine StateMachine

    ElectionTimeoutMin time.Duration // e.g. 150ms
    ElectionTimeoutMax time.Duration // e.g. 300ms
    HeartbeatInterval  time.Duration // e.g.  50ms

    Logger      *slog.Logger
    Seed        int64
    StopTimeout time.Duration // upper bound on Stop() drain; default 5s
}
```

### Status

```go
// Status is a point-in-time snapshot of a node's observable state.
// Returned by Node.Status() for /debug and operator tooling.
//
// Invariants:
//   - The returned MatchIndex map is a copy; callers may mutate freely.
//   - Status is consistent within itself (no partial reads across fields)
//     but may be stale by the time it is observed.
type Status struct {
    Role        Role
    Term        Term
    CommitIndex Index
    ApplyIndex  Index
    LeaderHint  NodeID            // best-known current leader, or empty
    MatchIndex  map[NodeID]Index  // leader-only; nil on followers
}
```

---

## 3. Public interfaces

Each method below carries the **prose summary**, **Invariants**, and **Error contract** pattern from RESEARCH.md §LLD.md Detail-Level Recommendation.

### Node

```go
// Node is the runtime handle a consumer holds. One Node per cluster member.
type Node interface {
    // Start spawns the tick loop and brings the node online.
    //
    // Invariants:
    //   - Idempotent: a second Start on a running Node returns nil (API-02).
    //   - Returns only once internal goroutines (tick, apply, transport
    //     dispatch) are running and Transport.Register has been called.
    //
    // Error contract:
    //   - ErrStopped if called after Stop.
    //   - Validation errors from Config are surfaced verbatim.
    Start(ctx context.Context) error

    // Stop drains the apply channel, closes the transport, and joins the
    // tick loop. Blocks for at most Config.StopTimeout.
    //
    // Invariants:
    //   - Idempotent across concurrent and repeated calls (API-02).
    //   - After Stop returns, Propose, Step, and Status all return ErrStopped.
    Stop() error

    // Propose submits a command for replication. Blocks until the entry is
    // committed AND applied (Apply has been called on this node's StateMachine),
    // or the context expires, or leadership is lost.
    //
    // Invariants:
    //   - Returns (index, term, nil) only after the command has been applied.
    //   - On leader loss before commit, returns ErrProposalDropped.
    //   - On follower/candidate role, returns ErrNotLeader with LeaderHint set.
    //
    // Error contract:
    //   - ErrNotLeader { LeaderHint: NodeID } — retry against the hint.
    //   - ErrProposalDropped — safe to retry.
    //   - ErrStopped — node is shut down; do not retry on this Node.
    //   - ctx.Err() — caller's deadline; the entry MAY still commit later.
    Propose(ctx context.Context, data []byte) (Index, Term, error)

    // Step is the inbound RPC entry point. Transport implementations call
    // Step for every Message received from a peer.
    //
    // Invariants:
    //   - Step is safe to call from any goroutine.
    //   - Step MUST NOT block on I/O; it hands off to the core tick loop.
    //   - Messages with Term > currentTerm trigger a step-down before any
    //     other processing.
    //
    // Error contract:
    //   - ErrStopped if the Node has been stopped.
    //   - Validation errors (e.g. unknown MessageType) are returned but the
    //     transport SHOULD log-and-drop rather than propagate to the wire.
    Step(ctx context.Context, msg Message) error

    // Status returns a copy of the current observable state (see Status type).
    // Cheap (microsecond-scale); safe to poll.
    Status() Status

    // LeaderHint returns the currently-believed leader, or empty if unknown.
    // Equivalent to Status().LeaderHint but avoids the map copy.
    LeaderHint() NodeID
}

// New constructs a Node from the given Config. Validates all required fields;
// returns an error naming the first invalid field on failure.
func New(cfg Config) (Node, error)
```

### StateMachine

```go
// StateMachine is the consumer-owned replicated state. Apply is called
// exactly once per committed entry, in index order, from a single goroutine.
//
// v1: Snapshot and Restore are stubs; implementors return
// ErrSnapshotUnsupported. v2 will define the snapshot contract WITHOUT
// breaking this interface (STOR-01 forward-compat).
type StateMachine interface {
    // Apply executes a committed Entry and returns an opaque result that is
    // delivered back to the proposing client if the proposal was local.
    //
    // Invariants:
    //   - Called exactly once per committed Entry, in strictly increasing
    //     Index order (API-05).
    //   - Called from a single goroutine; implementations need not be
    //     internally synchronized for Apply.
    //   - MUST be deterministic: identical Entries from identical state
    //     MUST yield identical results across replicas.
    //   - MUST NOT block indefinitely; long work belongs in a background
    //     goroutine the StateMachine owns.
    //
    // Error contract:
    //   - Non-nil err is delivered to the proposing client's Propose call.
    //   - An err here does NOT roll back the commit; the entry is committed
    //     by definition. Implementations SHOULD treat Apply errors as fatal
    //     (panic) unless the error encodes an application-level "rejected"
    //     outcome.
    Apply(entry Entry) (result any, err error)

    // Snapshot serialises the state up to lastIndex.
    //
    // v1: implementors MUST return (nil, 0, ErrSnapshotUnsupported).
    // v2: will define snapshot semantics; this signature is forward-compatible.
    Snapshot() (data []byte, lastIndex Index, err error)

    // Restore replaces the state from a snapshot produced by Snapshot.
    //
    // v1: implementors MUST return ErrSnapshotUnsupported.
    Restore(data []byte) error
}
```

### Storage (LogStorage + StateStorage)

```go
// Storage composes log and hard-state persistence. Implementations live in
// pkg/storage/memory and pkg/storage/file; consumers may write their own
// and exercise pkg/storage/storagetest for conformance.
//
// The Storage interface and its constituents live in pkg/storage (Phase 3
// freeze; see ADR-0005).
type Storage interface {
    LogStorage
    StateStorage
}

// LogStorage persists the replicated log.
type LogStorage interface {
    // Append persists entries in order. Entries are contiguous and start at
    // LastIndex()+1.
    //
    // Invariants:
    //   - MUST fsync the underlying storage before returning success (REPL-09).
    //   - MUST NOT modify entries after return; caller may reuse the slice.
    //   - On error, the on-disk state is unchanged (atomic per-call).
    //
    // Error contract:
    //   - Wraps the underlying I/O error with %w.
    Append(entries []Entry) error

    // TruncateSuffix discards entries with index >= from. Used on log conflict.
    //
    // Invariants:
    //   - MUST fsync before returning.
    //   - No-op (returns nil) if from > LastIndex().
    //
    // Error contract:
    //   - Wraps the underlying I/O error with %w.
    TruncateSuffix(from Index) error

    // Entries returns the half-open range [lo, hi).
    //
    // Invariants:
    //   - Returned slice is freshly allocated; caller may mutate freely.
    //
    // Error contract:
    //   - Returns an error wrapping io.ErrUnexpectedEOF if hi > LastIndex()+1.
    //   - v2: returns ErrCompacted if lo is below the snapshot horizon.
    Entries(lo, hi Index) ([]Entry, error)

    // Term returns the term of the entry at index, or 0 if index == 0
    // (the implicit pre-log sentinel).
    //
    // Error contract:
    //   - Returns an error wrapping io.ErrUnexpectedEOF if index > LastIndex().
    Term(index Index) (Term, error)

    // FirstIndex returns the smallest index present in the log. 1 in v1
    // (no compaction); v2 returns snapshotIndex+1.
    FirstIndex() (Index, error)

    // LastIndex returns the largest index present in the log, or 0 if the
    // log is empty.
    LastIndex() (Index, error)
}

// StateStorage persists HardState (the durable Raft state) and exposes the
// v1 snapshot stubs.
type StateStorage interface {
    // SaveHardState durably persists the given HardState.
    //
    // Invariants:
    //   - MUST fsync before returning (REPL-09).
    //   - MUST be atomic: a crash mid-call leaves either the prior or the new
    //     HardState fully on disk, never a torn write.
    //   - Implementations SHOULD use the tmp+rename pattern on Unix.
    //
    // Error contract:
    //   - Wraps the underlying I/O error with %w.
    SaveHardState(hs HardState) error

    // LoadHardState returns the most recently persisted HardState, or the
    // zero value if none has ever been saved (fresh node).
    //
    // Error contract:
    //   - A missing file is NOT an error; returns (HardState{}, nil).
    //   - A corrupt file IS an error; wraps the parse error with %w.
    LoadHardState() (HardState, error)

    // Snapshot serialises the persisted state up to lastIndex.
    //
    // v1: implementors MUST return (nil, 0, ErrSnapshotUnsupported).
    // v2: will define snapshot semantics; this signature is forward-compatible
    // (STOR-01; Global Invariant 5). See ADR-0005.
    Snapshot() (data []byte, lastIndex Index, err error)

    // Restore replaces the persisted state from a snapshot produced by
    // Snapshot.
    //
    // v1: implementors MUST return ErrSnapshotUnsupported.
    Restore(data []byte) error
}
```

### Transport

```go
// Transport ships Raft Messages between peers. Implementations may be lossy,
// reorder, or duplicate — Raft is resilient to all three. They MUST NOT
// mutate a Message after it is handed to Send.
type Transport interface {
    // Send is best-effort. Implementations SHOULD apply a bounded timeout;
    // errors are logged but not surfaced to the Raft core (the heartbeat
    // mechanism is the retry strategy).
    //
    // Invariants:
    //   - Safe to call from any goroutine.
    //   - MUST NOT block indefinitely; bound by an implementation-defined
    //     timeout (e.g. 1s for HTTP).
    //
    // Error contract:
    //   - Wraps the underlying network error with %w.
    //   - Returning an error does NOT cause Raft to retry; the next
    //     heartbeat is the retry signal.
    Send(ctx context.Context, msg Message) error

    // Register installs the inbound callback. The transport invokes step
    // for every received Message. MUST be called before Start.
    //
    // Invariants:
    //   - Called exactly once per Transport instance, by Node.Start.
    //   - step is safe to call from any goroutine.
    Register(step func(ctx context.Context, msg Message) error)

    // Close releases listeners and connections. Idempotent.
    //
    // Error contract:
    //   - Returns the first non-nil error from shutting down resources;
    //     subsequent calls return nil.
    Close() error
}
```

### Clock and Ticker

```go
// Clock abstracts time so tests can run deterministically. Production code
// uses internal/clock.Real; tests use internal/clock.Fake (consumers do not
// normally instantiate these — Node selects the appropriate Clock from Config
// when Config.Clock is nil; tests inject a Fake via raftest helpers).
type Clock interface {
    // Now returns the current time.
    Now() time.Time

    // NewTicker returns a Ticker that fires every d. Under a fake clock,
    // it only fires when Advance is called on the underlying fake.
    NewTicker(d time.Duration) Ticker
}

// Ticker mirrors the subset of time.Ticker that Raft uses.
type Ticker interface {
    // C returns the channel on which ticks are delivered.
    C() <-chan time.Time

    // Stop releases the Ticker. Safe to call multiple times.
    Stop()
}
```

---

## 4. Sentinel errors

```go
// ErrNotLeader is returned by Propose and by Transport handlers when the
// receiving node is not the current leader. LeaderHint carries the best
// known leader NodeID (or empty if unknown).
//
// Invariants:
//   - LeaderHint, if non-empty, MUST refer to a NodeID in Config.Peers.
//   - Wire projection: docs/WIRE.md X-Raft-Leader-Hint header carries
//     LeaderHint verbatim; the JSON error envelope carries it as
//     "leader_hint".
type ErrNotLeader struct {
    LeaderHint NodeID
}

func (e *ErrNotLeader) Error() string {
    if e.LeaderHint == "" {
        return "raft: not leader"
    }
    return "raft: not leader (leader hint: " + string(e.LeaderHint) + ")"
}

// Plain sentinel errors. Compare with errors.Is.
var (
    ErrStopped             = errors.New("raft: node stopped")
    ErrProposalDropped     = errors.New("raft: proposal dropped (leader lost)")
    ErrSnapshotUnsupported = errors.New("raft: snapshot not supported in v1")
)
```

`ErrSnapshotUnsupported` lives in `pkg/raft` and is re-exported from `pkg/storage` as `storage.ErrSnapshotUnsupported`. Direction is forced by import-cycle avoidance: `pkg/storage` already imports `pkg/raft` for `Entry`/`HardState`/`Index`/`Term`, so the sentinel cannot live in `pkg/storage`. `errors.Is` resolves identically against either name. See ADR-0005.

---

## 5. Global invariants

These cross-cutting rules bind every implementation in `pkg/raft` and its plug-ins. Violating any of these is a review-blocking finding.

1. **fsync ordering (REPL-09).** A node MUST NOT emit an outbound RPC whose correctness depends on `HardState` (RequestVote, vote-granting RequestVoteResponse, AppendEntries from a new leader) until the corresponding `HardState` has been durably persisted via `Storage.SaveHardState`. The driver enforces this between core emission and Transport.Send.

2. **Zero-value safety (FOUND-05).** Every exported function on a zero-value receiver of an exported type either does something useful or panics with a message that names the offending field (e.g. `"raft.Node: zero-value Node; construct via raft.New"`). Silent zero-value misuse is a bug.

3. **Start/Stop idempotency (API-02).** `Node.Start` may be called multiple times; only the first call has effect. `Node.Stop` may be called multiple times and from multiple goroutines concurrently; only one drain executes and all callers observe the same return value.

4. **Apply-in-order via bounded channel (API-05).** Committed entries are delivered to `StateMachine.Apply` from a single goroutine reading a bounded channel owned by `Node`. Apply MUST NOT be invoked inline on the Raft tick loop; this preserves liveness when a slow Apply backs up.

5. **Snapshot-stub forward-compat (STOR-01).** `StateMachine.Snapshot`, `StateMachine.Restore`, and any storage snapshot hooks return `ErrSnapshotUnsupported` in v1. v2 will populate them WITHOUT changing the interface signatures; consumers that compile against v1 will continue to compile against v2.

6. **Message immutability after Send.** Once `Transport.Send(ctx, msg)` has been called, the sender MUST NOT mutate `msg` or any slice it references (notably `msg.Entries`). Transports may queue, retry, or fan out asynchronously.

7. **Term-first processing.** Any inbound `Message` with `Term > currentTerm` triggers a step-down (Role = Follower, CurrentTerm = msg.Term, VotedFor = "") and a `HardState` persist BEFORE any other field of the message is inspected (Raft §5.1).

---

## 6. Test-only public surface

The `pkg/transport/inproc.Hub` exposes chaos knobs intentionally. These are **stable public surface**, but they live in **test-fixture territory**: consumers may depend on them for their own chaos suites, but production code SHOULD NOT.

```go
// Hub is the in-memory bus shared by all nodes in an inproc cluster.
// Constructed once per test; one Transport per node is obtained via Connect.
type Hub struct { /* unexported */ }

// NewHub constructs an empty Hub.
func NewHub() *Hub

// Connect returns a Transport bound to nodeID. Messages between connected
// nodes are delivered via channels. Closing the returned Transport
// disconnects only that node (TRAN-06).
func (h *Hub) Connect(nodeID raft.NodeID) raft.Transport

// --- Chaos knobs ---

// Partition drops all messages between a and b in both directions until Heal.
func (h *Hub) Partition(a, b raft.NodeID)

// Heal restores delivery between a and b.
func (h *Hub) Heal(a, b raft.NodeID)

// DropRate sets the probability [0,1] that a message from nodeID is dropped.
// Uses the Hub's seeded RNG so chaos is reproducible.
func (h *Hub) DropRate(nodeID raft.NodeID, p float64)

// Delay sets the per-message delivery delay range [min, max] sampled
// uniformly per message.
func (h *Hub) Delay(min, max time.Duration)

// Reorder enables out-of-order delivery: when true, the Hub buffers up to
// queueDepth messages per receiver and delivers them in random order.
func (h *Hub) Reorder(enabled bool, queueDepth int)

// Duplicate sets the probability [0,1] that a message is delivered twice.
func (h *Hub) Duplicate(p float64)
```

All chaos knobs are seeded from the Hub's RNG, which is itself seeded from `Config.Seed` of the first connected node. This makes chaos suites reproducible by seed.

---

## 7. Cross-references

- **Wire JSON projection of `Message`:** see [`docs/WIRE.md`](./WIRE.md).
- **Goroutine and lock model that drives `Node`, `Storage`, `Transport`:** see `docs/CONCURRENCY.md` (Phase 1 Plan 4).
- **Source material:**
  - `.planning/research/ARCHITECTURE.md` §Interface Contracts (verbatim signatures).
  - `.planning/research/SUMMARY.md` §2 (SDK shape rationale).
  - `.planning/REQUIREMENTS.md` FOUND-05, STOR-01, REPL-09, API-02, API-05, TRAN-01, TRAN-06.
