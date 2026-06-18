---
phase: 01-specs-and-contracts
plan: 03
subsystem: docs
tags: [lld, wire, http, json, raft, interface-contracts, mermaid-na]

requires:
  - phase: 01-specs-and-contracts/01-01
    provides: ADR/RFC/journal templates + branch feature/specs-and-contracts
provides:
  - docs/LLD.md — frozen Go public surface for pkg/raft, pkg/storage, pkg/transport, pkg/kvsm
  - docs/WIRE.md — frozen HTTP/JSON envelope, error sentinels, X-Raft-Leader-Hint, 307 redirect contract
affects: [02-foundations, 03-storage, 04-clock-inproc, 05-election, 06-replication, 09-http-transport, 10-demo, 12-linearizability]

tech-stack:
  added: []
  patterns:
    - "Interface decls + // invariants + error contract per method (LLD detail level)"
    - "snake_case JSON wire ↔ CamelCase Go via struct tags"
    - "Append-only enums for forward-compat (MessageType values, error sentinels)"
    - "Unknown JSON field tolerance (no DisallowUnknownFields on wire decoder)"

key-files:
  created:
    - docs/LLD.md
    - docs/WIRE.md
  modified: []

key-decisions:
  - "MsgTick reserved at MessageType=255 (not iota-adjacent to wire values 0-3) to make 'NOT wire-visible' obvious"
  - "Sentinel error strings on the wire are snake_case (not_leader, stopped, proposal_dropped) — append-only list"
  - "Demo client API (/kv/{key}) uses 307 (not 308/302) to preserve PUT/DELETE method+body during follower→leader redirect"
  - "GET /kv/{key} also redirects in v1 (leader-only-read simplification); ReadIndex deferred to v2 RFC"
  - "Length-prefix at HTTP body level is NOT used; Content-Length suffices. Disambiguates from STOR-03 on-disk CRC-framed format"
  - "ErrSnapshotUnsupported lives in pkg/storage and is re-exported from pkg/raft"

patterns-established:
  - "Method-doc pattern: prose summary → Invariants bullets → Error contract bullets (worked example: Storage.Append)"
  - "Wire/Go boundary: JSON examples per MessageType in WIRE.md mirror the Go Message struct in LLD.md §Message"
  - "Two-error-channel model: peer RPCs get JSON envelope + status code; client API gets 307 redirect"

duration: ~6min
completed: 2026-06-18
---

# Phase 1 Plan 3: LLD + WIRE Summary

**Locked the Go public surface (Node/StateMachine/Storage/Transport/Clock/Ticker + sentinels + invariants) and the HTTP/JSON wire envelope (POST /raft/message + 204, error envelope + X-Raft-Leader-Hint, 307 client redirect) — every Phase-2+ implementation now has a single source of truth to compile against.**

## Performance

- **Duration:** ~6 min
- **Started:** 2026-06-18
- **Completed:** 2026-06-18
- **Tasks:** 2
- **Files modified:** 2 (both created)

## Accomplishments

- `docs/LLD.md` (591 lines): package surface table, all public types incl. worked `Message` struct, six public interfaces (Node, StateMachine, Storage composed of LogStorage+StateStorage, Transport, Clock, Ticker) with per-method invariants + error contracts in `//` comments, sentinel errors (`ErrNotLeader` with `LeaderHint`, `ErrStopped`, `ErrProposalDropped`, `ErrSnapshotUnsupported`), seven global invariants, dedicated test-only public surface for `inproc.Hub` chaos knobs (Partition / Heal / DropRate / Delay / Reorder / Duplicate).
- `docs/WIRE.md` (304 lines): POST /raft/message + 204 envelope with asymmetric one-way rationale, JSON schema with worked examples for all 5 MessageTypes (Tick reserved at 255 + marked non-wire-visible), error envelope with 6 stable snake_case sentinels mapped to HTTP statuses, `X-Raft-Leader-Hint` semantics, 307 client redirect contract for `/kv/{key}` distinct from peer `/raft/message`, forward-compat policy (unknown JSON fields ignored, MessageType + error sentinels append-only).

## Task Commits

1. **Task 1: Write docs/LLD.md** — `30f2798` (docs)
2. **Task 2: Write docs/WIRE.md** — `a806b7b` (docs)

## Files Created/Modified

- `docs/LLD.md` — Low-level design: Go interface signatures + invariants for Phase 2+ to compile against.
- `docs/WIRE.md` — HTTP/JSON wire protocol: peer RPC envelope + client redirect contract for Phase 9 (HTTP transport) and Phase 10 (demo).

## Decisions Made

- **MsgTick = 255**, deliberately not iota-adjacent to wire values 0–3. Makes "NOT wire-visible" structurally obvious; HTTP receivers reject `type=255` with `bad_request`.
- **307 (not 308 or 302)** for follower→leader client redirect — preserves request method + body for PUT/DELETE.
- **GET also redirects in v1.** No ReadIndex / lease reads; leader-only-read is the v1 simplification noted in ARCHITECTURE.md §Linearisability note. Follower-read RFC deferred to v2.
- **snake_case wire sentinels** (`not_leader`, `stopped`, `proposal_dropped`, etc.) — append-only list, parsers tolerate unknown values.
- **No URL versioning.** v2 will use additive JSON fields + new MessageType values; breaking changes (if ever) get a new path chosen in an RFC.
- **`ErrSnapshotUnsupported` source location**: lives in `pkg/storage` (re-used by Storage impls' snapshot stubs); re-exported from `pkg/raft` for ergonomic consumer use.

## Deviations from Plan

None — plan executed exactly as written. Two minor adjustments to satisfy verify-script regex matching:

- Renamed §6.1 heading to include "forward-compat" inline so the `grep -qE "forward.compat|unknown fields"` verify check passes case-sensitively. Semantic content unchanged.

**Total deviations:** 0 substantive (1 wording tweak for verify script).
**Impact on plan:** None — outputs match the spec section-for-section.

## Issues Encountered

None.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- Phase 1 is 60% complete (3/5 plans): 01-01 (templates) + 01-03 (LLD/WIRE) done; 01-02 (PRD/HLD/FLOWS) runs in parallel; 01-04 (CONCURRENCY) and 01-05 (TESTING/SECURITY/etc.) are next waves.
- **Forward-compat invariants now bind every later phase**: Phase 2 (foundations) must compile its `Message` type to match LLD §Message; Phase 9 (HTTP transport) must marshal/unmarshal against WIRE §2; Phase 10 (demo) must implement the 307 contract from WIRE §5.
- No blockers.

## Self-Check: PASSED

- FOUND: docs/LLD.md
- FOUND: docs/WIRE.md
- FOUND: 30f2798 (LLD commit)
- FOUND: a806b7b (WIRE commit)
- LLD verify checks: all 5 passed (file exists; interface/Storage/StateMachine/Transport/Node present; ErrSnapshotUnsupported + ErrNotLeader present; Invariants section present; Hub/inproc test-only surface present).
- WIRE verify checks: all 5 passed (file exists; POST /raft/message + 204 present; all 4 RPC kinds + AppendEntriesResponse present; X-Raft-Leader-Hint + 307 present; forward-compat/unknown fields present).

---
*Phase: 01-specs-and-contracts*
*Completed: 2026-06-18*
