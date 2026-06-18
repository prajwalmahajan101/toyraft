# ToyRaft — Product Requirements Document

**Status:** Accepted (Phase 1 contract)
**Date:** 2026-06-18
**Scope:** project-wide; consumer-facing contract for v1

Sourced from `.planning/PROJECT.md` (What This Is, Core Value, Out of Scope, Context) and `.planning/REQUIREMENTS.md` (Active bucket). This document is the consumer-facing contract: a downstream service should be able to read this file alone and decide whether ToyRaft fits.

---

## What it is

ToyRaft is a Go implementation of the Raft consensus algorithm packaged as a reusable library/SDK, plus a reference N-node demo cluster (defaults to 3 nodes). It is the third corner of a three-project trilogy of distributed-systems primitives — log (`toymq`), map (`toykv`), consensus (`toyraft`) — each shipped as its own Go module.

ToyRaft is designed to be `import`ed by other Go services to add leader election and log replication on top of any deterministic state machine the consumer provides.

Source: `PROJECT.md` §What This Is.

## What it isn't

ToyRaft is not a production-grade consensus runtime. It is not a fork of `etcd/raft` or `hashicorp/raft`. It does not ship snapshots, dynamic membership, TLS, authentication, multi-Raft, or a shared wire protocol with `toymq`/`toykv` in v1. See the [Non-goals](#non-goals) table below for the full boundary and the rationale for each item.

Source: `PROJECT.md` §Out of Scope.

## Scope

The v1 scope, bulleted verbatim from `PROJECT.md` §Active:

**Consensus core (the SDK)**

- N-node cluster support (odd N ≥ 3, configurable at init — no hardcoded peer count).
- Leader election with randomised election timeouts (default 150–300 ms).
- Log replication — leader appends, replicates, commits on majority ack.
- Term-based safety: monotonic terms, one vote per term, log-up-to-date check before voting.
- `StateMachine` interface — consumer provides `Apply(entry)`; ToyRaft owns the rest.
- `Transport` interface with two implementations: HTTP/JSON (real) + in-process channels (deterministic tests).

**Reference demo + KV state machine**

- In-memory KV state machine (GET / SET / DELETE) as the reference `StateMachine`.
- HTTP API on the leader for client writes; followers proxy/redirect to leader.
- 3-node demo runnable with `make demo`.
- Demo script: write a value, partition the leader, verify new leader elected, verify writes survive.
- Partition heals → old leader steps down, logs converge.

**Testing (layered)**

- Unit tests for state-machine transitions, log invariants, election rules.
- Deterministic seeded chaos tests using in-process transport + fake clock.
- Linearizability checker against the demo KV workload.
- Chaos harnesses for all three partition styles: process kill/restart, in-process packet-drop proxy, iptables network partition.

**Project quality**

- README explains: what's implemented, what's intentionally skipped, what would change for production.
- ADRs in `docs/adr/` for every non-obvious decision (matches toykv/toymq house style).
- Per-phase journal entries in `.journal/`.

Source: `PROJECT.md` §Active.

## Non-goals

The following are explicit non-goals for v1. Each is documented to prevent re-adding without an RFC (see Working Agreement 3 in `PROJECT.md`).

| Non-goal | Reason |
|---|---|
| gRPC / protobuf transport | Breaks the stdlib-only aesthetic shared with `toykv`/`toymq`. The `Transport` interface keeps the door open if needed later. |
| Snapshots / log compaction | Meaningful complexity, deferred to a future milestone. ToyRaft v1 logs grow unbounded; `Snapshot`/`Restore` are stubbed for forward-compat. |
| Cluster membership changes (joint consensus / single-server changes) | Out of scope for v1; cluster is fixed at startup. |
| Production-grade auth / TLS between nodes | Matches the "not production" stance of toykv/toymq v1s. |
| Sharing a wire protocol with `toymq`/`toykv` (`toywire`) | Interesting future work, but pre-binding it now creates coupling without value. |
| gRPC streaming for log shipping | Basic AppendEntries batching in v1 is enough. |
| Multi-Raft / shard support | Single Raft group only. |
| ReadIndex / lease reads | v1 ships leader-only reads (no read index). Linearisable reads via `ReadIndex` are explicit v2 work. |

Source: `PROJECT.md` §Out of Scope (verbatim) + `REQUIREMENTS.md` v2 buckets (`SNAP-*`, `MEMB-*`, `PERF-*`, `PROD-*`).

## Success criteria

ToyRaft v1 succeeds when **a consumer can `import` ToyRaft, plug in a `StateMachine`, and get a working replicated cluster whose invariants hold under partition/kill/restart chaos.**

The four Raft safety invariants that must hold (Ongaro/Ousterhout 2014 §5.4.3):

1. **Election safety** — at most one leader per term.
2. **Leader completeness** — a committed entry is present on every future leader.
3. **Log matching** — if two logs contain an entry at the same index with the same term, all preceding entries are identical.
4. **State-machine safety** — if a server has applied an entry at a given index, no other server applies a different entry at the same index.

**v1 acceptance gate (Phase 12 — `feature/linearizability`):** a Porcupine-style linearizability checker, run against the reference KV state machine under seeded chaos, produces zero violations across `N = 3, 5, 7` cluster sizes. This is the load-bearing correctness signal; manual demo scripts are the floor, not the ceiling.

Source: `PROJECT.md` §Core Value + `REQUIREMENTS.md` §Active "Testing (layered)" + `ROADMAP.md` Phase 12.

## Consumer integration target

ToyRaft's v1 win condition is being consumed by two downstream services:

- **`toykv-cluster`** — `toykv` v2 with Raft-replicated KV state. State machine = the existing `toykv` store; transport = HTTP/JSON.
- **`toymq-cluster`** — `toymq` v2 with Raft-replicated log. State machine = WAL append + offset advance.

**Known cross-project concern:** `toymq`'s fsync-on-PUB durability story interacts non-trivially with Raft's commit semantics. A PUB acknowledged to a client before the Raft commit completes would weaken `toymq`'s durability contract; serialising on Raft commit may regress its publish latency. This interaction is flagged here for the future `toymq-cluster` RFC (Working Agreement 3).

Source: `PROJECT.md` §Context, "Consumer integration target".

---

## Cross-references

- High-level architecture: `docs/HLD.md`
- Sequence flows: `docs/FLOWS.md`
- Interface contracts: `docs/LLD.md` (lands later in this phase)
- Wire format: `docs/WIRE.md` (lands later in this phase)
- v1 scope lock + per-non-goal rationale: `docs/rfc/0001-v1-scope-and-non-goals.md`
- Working agreements (ADR / RFC / journal / branching / commits): `docs/PROCESS.md` (lands later in this phase)
