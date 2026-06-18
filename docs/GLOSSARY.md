# ToyRaft Glossary

**Status:** Accepted
**Date:** 2026-06-18
**Scope:** project-wide vocabulary

A shared vocabulary for the consensus core, the testing layers, and the
project-specific extensions. When a Raft term appears in an ADR, RFC, PRD,
HLD, LLD, or WIRE doc, the definition lives here and nowhere else.

When in doubt, cross-reference the [Raft paper (Ongaro/Ousterhout,
2014)][raft-paper] §5 for the consensus terms; project-specific terms are
defined in this repository's specs (PRD/HLD/LLD/WIRE/CONCURRENCY).

[raft-paper]: https://raft.github.io/raft.pdf

## Raft Roles

- **leader** — The single node per term that accepts client proposals,
  replicates them via `AppendEntries`, and advances `commitIndex`.
- **follower** — A node that passively accepts `AppendEntries` /
  `RequestVote` from a current-term leader/candidate. Default starting
  role for any node.
- **candidate** — A node that has timed out, incremented its term, voted
  for itself, and is soliciting votes via `RequestVote`. Transitional
  role: becomes leader on quorum, returns to follower on a higher-term
  observation or election timeout.
- **role FSM** — `Follower ↔ Candidate ↔ Leader` with the transition
  triggers defined in `docs/HLD.md`. There is no separate "learner" role
  in v1 (deferred — see `REQUIREMENTS.md` MEMB-03).

## Raft Terms (consensus vocabulary)

- **term** — A monotonically increasing integer that partitions logical
  time. Each term has at most one leader. Persisted in `HardState`
  (`currentTerm`) and incremented when a follower starts an election or
  when any node observes a higher term on an inbound RPC.
- **log** — The replicated, ordered sequence of entries owned by the
  leader and copied to followers. v1 logs grow unbounded (snapshots are
  deferred — see `REQUIREMENTS.md` SNAP-01..04).
- **entry** — A single record in the log: `{Term, Index, Data}`. The
  consensus core treats `Data` as opaque `[]byte`; the
  consumer's `StateMachine.Apply` decodes it.
- **index** — A 1-based contiguous identifier for a log position. ToyRaft
  uses index 0 to mean "no entry" in zero-value contexts (e.g.
  `commitIndex=0` at boot).
- **commit** — A log entry is committed when the leader observes
  majority `matchIndex >= entry.Index` AND `log[entry.Index].Term ==
  currentTerm` (the Figure 8 rule). Once committed, an entry will be
  visible to all future leaders.
- **apply** — Delivery of a committed entry to the consumer's
  `StateMachine.Apply(Entry)` via a bounded `ApplyChannel`. Apply is
  monotonic and in index order.
- **quorum** — Strict majority of the configured peer set. For N peers,
  quorum is `floor(N/2) + 1`. Required to be odd in v1
  (see `REQUIREMENTS.md` constraints).
- **majority** — Synonym for quorum in v1 (no joint consensus, so there
  is exactly one configuration at any time).

## Raft Safety Properties

These five properties are the contract Raft maintains and ToyRaft must
preserve. Sourced from the Raft paper §5.

- **election safety** — At most one leader can be elected in a given
  term (paper §5.2). Verified as a chaos invariant — see
  `REQUIREMENTS.md` ELEC-10.
- **leader append-only** — A leader never overwrites or deletes entries
  in its own log; it only appends new entries (paper §5.3).
- **log matching** — If two logs contain an entry at the same index with
  the same term, then (a) those entries are identical and (b) all
  preceding entries are identical. Enforced by the `prevLogIndex` /
  `prevLogTerm` check in `AppendEntries`. See `REQUIREMENTS.md` REPL-03,
  REPL-11.
- **leader completeness** — If an entry is committed in some term, that
  entry will appear in the logs of all leaders for all higher-numbered
  terms (paper §5.4). The election restriction (below) is what
  guarantees this.
- **state machine safety** — If any server has applied a log entry at a
  given index, no other server will ever apply a different entry for the
  same index (paper §5.4.3). Apply happens via the `ApplyChannel`, in
  index order, after the entry is committed.

## Raft Mechanisms

- **election restriction** — A voter rejects `RequestVote` if the
  candidate's log is not "at least as up-to-date" as its own
  (paper §5.4.1). "Up-to-date" is defined as a lexicographic compare on
  `(lastLogTerm, lastLogIndex)`: higher term wins, tie broken by higher
  index. This is what enforces leader completeness. See
  `REQUIREMENTS.md` ELEC-06, ELEC-09.
- **current-term commit rule** — A leader may only mark an entry
  committed if `log[N].Term == currentTerm` AND a quorum of `matchIndex
  >= N` (paper §5.4.2, Figure 8). This fixes the unsafe optimisation of
  committing entries from a previous term by replica count alone. See
  `REQUIREMENTS.md` REPL-06, REPL-10.
- **slow probe / fast rollback** — On `AppendEntries` rejection, v1
  leaders decrement `nextIndex[peer]` by 1 and retry (slow probe). The
  faster `ConflictTerm` / `ConflictIndex` optimisation is wire-reserved
  but the leader-side implementation is deferred to a future ADR. See
  `REQUIREMENTS.md` REPL-04 and v2 PERF-01.
- **pre-vote** — Optimisation where a candidate first asks "would you
  vote for me?" without bumping its term, to avoid disrupting a healthy
  leader after a brief partition. Deferred to v2 — see `REQUIREMENTS.md`
  PROD-02.

## Figure References (Raft paper)

- **Figure 7** — Six scenarios `(a)`–`(f)` showing followers with logs
  that differ from a newly-elected leader. The figure is the canonical
  proof that voting based purely on cluster membership is unsafe and
  that the election restriction (above) is necessary: only candidates
  whose `(lastLogTerm, lastLogIndex)` is at least as up-to-date as a
  majority can be elected. ToyRaft unit-tests all six rows
  (`REQUIREMENTS.md` ELEC-09).

- **Figure 8** — Five-frame walkthrough where leader churn causes an
  entry to be replicated to a majority of followers and then *lost*
  because a later leader (with a stale log) overwrites it. The figure
  proves an entry from a previous term cannot be safely committed by
  counting replicas alone; the leader must commit at least one
  current-term entry, which transitively commits the older entries
  beneath it. ToyRaft enforces this via the current-term commit rule
  and includes Figure 8 as a scripted scenario in the linearizability
  suite (`REQUIREMENTS.md` REPL-10, LIN-05).

## Project-Specific Terms

These terms are ToyRaft's own and are not defined in the Raft paper.

- **Hub** — The in-process transport (`pkg/transport/inproc.Hub`) that
  delivers `Message`s between N nodes via in-memory channels. The Hub
  exposes chaos knobs (partition, drop probability, delay range,
  reorder, duplicate) all driven by a single seed — this is what makes
  the seeded chaos suite reproducible. Not for production use; this is
  a test fixture (see `REQUIREMENTS.md` TEST-03..TEST-06).

- **Driver** — The single goroutine that shuttles I/O between the pure
  consensus core (no I/O, no goroutines, deterministic step function)
  and the side-effecting world: `Transport.Send`, `Storage.Append`,
  `StateMachine.Apply`. The core produces a list of outputs per step
  (messages to send, entries to persist, entries to apply); the Driver
  executes them in order. See `docs/HLD.md` and `docs/CONCURRENCY.md`.

- **ApplyChannel** — The bounded `chan Entry` that decouples commit
  (which happens on the Raft loop) from apply (which calls into
  consumer code). Owned by the `Node`; closed only on `Stop()`. Bound
  prevents a slow `StateMachine.Apply` from stalling the Raft loop —
  back-pressure is intentional. See `REQUIREMENTS.md` API-05.

- **LeaderHint** — A `NodeID` carried in `ErrNotLeader` and in the
  `X-Raft-Leader-Hint` HTTP response header. Best-effort: may be zero
  if the follower hasn't observed a leader yet, may be stale during
  elections. Consumers use it to redirect / retry against the current
  leader. See `REQUIREMENTS.md` API-04, TRAN-04.

- **HardState** — The persisted tuple `(currentTerm, votedFor)` that
  must survive crashes for ToyRaft to maintain election safety.
  Persisted via `Storage.SaveHardState` and fsynced before any RPC
  response that depends on it (REPL-09).

- **Entry.Data** — Opaque `[]byte` payload that the consumer encodes
  before `Propose` and decodes inside `StateMachine.Apply`. ToyRaft
  intentionally does not impose a serialisation format — see
  `REQUIREMENTS.md` Out of Scope ("Reflection-based FSM with
  interface{} entries").

- **toyraftd / toyraftctl** — The reference demo binary
  (`cmd/toyraftd`) and CLI client (`cmd/toyraftctl`) that exercise the
  library against the reference `pkg/kvsm` state machine. See
  `REQUIREMENTS.md` DEMO-01..DEMO-09.

## Cross-References

- **Paper definitions:** [Raft paper][raft-paper] §5 (consensus core),
  Figures 7 & 8 (safety proofs).
- **In-repo:** `docs/HLD.md` (component map + role FSM),
  `docs/LLD.md` (Go signatures + invariants), `docs/WIRE.md` (RPC
  schemas), `docs/CONCURRENCY.md` (Driver + ApplyChannel ownership).
- **Requirements:** `.planning/REQUIREMENTS.md` is the canonical home
  for every `FOUND-*`, `ELEC-*`, `REPL-*`, etc. identifier referenced
  above.
