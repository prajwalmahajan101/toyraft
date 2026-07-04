# 0021 — Publish observability daemon-side, leaving the raft core untouched

**Status:** Accepted
**Date:** 2026-07-04
**Scope:** `cmd/toyraftd`, `pkg/raft/status.go`

## Context

Phase 14 closes the runtime-observability gaps (OBS-04 / SC2): the six `raft.*`
counters at `/debug/vars`, structured RPC/role logs (SC1), and per-node
`/status` JSON (SC3). ToyRaft's single-mutex raft core is a frozen, stdlib-only
state machine (ADR-0004): every mutation flows through one lock and the package
imports no third-party code. Threading a `Config.Metrics` hook through the core —
the obvious way to source term counts, commit/apply lag and RPC counters — would
puncture that mutex discipline (a metrics callback firing under `n.mu`, or a
second lock to protect the counters) and drag an `expvar` import into `pkg/raft`,
breaking the "core is a pure library" invariant. OBS-04 also mandates stdlib
`expvar` specifically (NOT the Prometheus stack the sibling `toymq` uses).

## Decision

We publish ALL observability DAEMON-SIDE, in `cmd/toyraftd`, so the raft core
stays byte-for-byte untouched:

- The six counters are registered with `expvar` in the daemon, with byte-exact
  names `raft.terms`, `raft.elections`, `raft.rpc.sent`, `raft.rpc.received`,
  `raft.commit_lag`, `raft.apply_lag`.
- `raft.rpc.sent` / `raft.rpc.received` are counted at the TRANSPORT SEAMS on the
  daemon's `asyncTransport` wrapper: `Send` increments sent at enqueue (so a
  dropped-full-queue message still counts as an attempted send, matching the
  fire-and-forget contract); `Register` wraps the registered step callback in a
  counting closure that increments received before delegating to `node.Step`.
  `expvar` therefore never enters `pkg/raft`.
- `raft.commit_lag` / `raft.apply_lag` are read-time `expvar.Func` gauges computed
  from a fresh `node.Status()` snapshot on each scrape. To make `commit_lag`
  computable, `raft.Status` gains a `LastLogIndex` field (populated under the
  existing copy-under-lock snapshot) — the ONLY core change, and a purely additive
  read-only field.
- `raft.terms` / `raft.elections` are DERIVED by a daemon poll ticker that reads
  `node.Status()` every 200 ms: each observed `Term` increase advances `terms` by
  the delta, and each observed transition into the `Candidate` role bumps
  `elections`. This is a deliberate poll-based approximation ("close enough" for
  an educational demo) that trades exactness for zero core coupling.
- `/status` is UN-GATED: `handleStatus` no longer calls `leaderGate`, so every
  node returns its own status snapshot as JSON (followers no longer 307-redirect).
  This is strictly better for leader discovery — a follower's `leader_hint` points
  at the leader. The `/kv/*` handlers KEEP `leaderGate` (writes and leader-only
  reads stay leader-routed).

## Consequences

**Positive**
- The single-mutex core (ADR-0004) and its stdlib-only ethos are preserved — no
  metrics hook, no lock, no `expvar` import in `pkg/raft`.
- Operators get `/debug/vars` counters, per-node `/status`, and structured
  RPC/role logs with zero new dependencies.
- The transport-seam counting is exact for RPCs actually sent/received.

**Negative**
- `raft.terms` / `raft.elections` are approximate: a term jump between two polls
  still counts the full delta, but a candidacy that resolves inside one 200 ms
  poll interval may be missed. Acceptable for a demo; not a production SLI.
- `commit_lag` / `apply_lag` are computed per scrape (one extra `Status()` call
  each), not continuously tracked.

**Follow-ups**
- A future exact metrics path would require a core-level hook — explicitly
  rejected here to protect ADR-0004; revisit only if approximate counters prove
  insufficient.
- Cross-reference ADR-0004 (single-mutex core) for the invariant this decision
  protects.
