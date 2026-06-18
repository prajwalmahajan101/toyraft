# ToyRaft — Project State

**Initialised:** 2026-06-18
**Updated:** 2026-06-18 — completed Phase 1 Plan 2 (PRD/HLD/FLOWS) and Plan 3 (LLD/WIRE) on parallel waves

## Project Reference

- **Project doc:** `.planning/PROJECT.md`
- **Requirements:** `.planning/REQUIREMENTS.md` (116 v1 requirements)
- **Roadmap:** `.planning/ROADMAP.md` (14 phases)
- **Research:** `.planning/research/SUMMARY.md` (+ STACK / FEATURES / ARCHITECTURE / PITFALLS)
- **Core value:** A correct, understandable Raft implementation that can be embedded as a library by other Go services to add leader election + log replication on top of any deterministic state machine.
- **Current focus:** Phase 1 — Specs & Contracts.

## Current Position

- **Phase:** 1 (Specs & Contracts) — In progress
- **Plan:** 3/5 complete; next plans: `01-04-PLAN.md` (CONCURRENCY), `01-05-PLAN.md` (TESTING/SECURITY/GLOSSARY/CONTRIBUTING/RELEASE_PLAN/PROCESS + PR template)
- **Status:** Plan 01-02 (PRD/HLD/FLOWS) + Plan 01-03 (LLD/WIRE) both complete
- **Branch:** `feature/specs-and-contracts`
- **Progress:** [██████░░░░] 60%

## Performance Metrics

- Phases complete: 0/14
- Plans complete: 3/5 (in Phase 1)
- Requirements satisfied: 0/116 (Plan 01-01 partial: DOC-14, PROC-04, PROC-05, DOC-13 template-only; Plan 01-02: DOC-01, DOC-02, DOC-08; Plan 01-03: DOC-03, DOC-04 spec-only — implementations land Phase 2+)
- ADRs written: 1/15 (target) — ADR-0000 meta
- Journal entries: 0/14

### Per-plan metrics

| Phase | Plan | Duration | Tasks | Files |
|-------|------|----------|-------|-------|
| 01 | 01 | ~2 min | 3 | 5 |
| 01 | 02 | ~4 min | 3 | 3 |
| 01 | 03 | ~6 min | 2 | 2 |

## Accumulated Context

### Decisions (locked in PROJECT.md + SUMMARY.md)
- Library/SDK shape with `StateMachine`, `Storage`, `Transport` interfaces.
- Stdlib-only consensus core; test-only deps allowed (Porcupine, optional gopter).
- `Storage` interface in v1; ship both `memory` and `file` impls.
- Hand-rolled `Clock` + inproc `Hub` (no benbjohnson/clock, no clockwork).
- HTTP/JSON wire format v1; gRPC out of scope.
- Static membership; no joint consensus.
- Snapshot interface methods stubbed in v1 (forward-compat with v2).
- Linearizability via Porcupine is the v1 acceptance gate (Phase 12).
- Upfront `docs/{PRD,HLD,LLD,WIRE,CONCURRENCY,TESTING,SECURITY}.md` committed before any code (Phase 1) — matches toykv/toymq house style.

### Decisions (from execution)
- **01-01:** Adopt Michael Nygard ADR form verbatim from sibling toymq house style.
- **01-01:** Defer `docs/adr/README.md` index to Phase 2 (alongside ADR-0001) per research open question #1.
- **01-01:** Created `feature/specs-and-contracts` branch (per Working Agreement 6) before committing Phase 1 artifacts.
- **01-02:** Adopt Mermaid (not PlantUML or ASCII) for all sequence + state diagrams — GitHub renders natively since Feb 2022, no toolchain needed.
- **01-02:** Render Out-of-Scope as a single non-goals table in PRD (DRY — not bullets-then-table).
- **01-02:** Fast-rollback wire fields (ConflictTerm/ConflictIndex) ship in v1 Message schema; leader-side optimisation deferred to a future ADR (per REPL-04 slow probe).
- **01-02:** Phase 1 docs carry Status/Date/Scope header mirroring the ADR template — treats specs as first-class governance artifacts.
- **01-03:** Reserve `MsgTick = 255` (not iota-adjacent to wire values 0–3) so "NOT wire-visible" is structurally obvious; HTTP receivers reject `type=255` as `bad_request`.
- **01-03:** Use `307 Temporary Redirect` (not 302/308) for follower→leader client redirect to preserve method+body for PUT/DELETE.
- **01-03:** GET also redirects in v1 (leader-only-read simplification); ReadIndex / follower reads deferred to v2 RFC.
- **01-03:** Wire error sentinels are snake_case strings (`not_leader`, `stopped`, `proposal_dropped`, …) — append-only list; parsers tolerate unknown values.
- **01-03:** No URL versioning. v2 uses additive JSON fields + new MessageType values; breaking changes (if any) get a new path chosen in an RFC.
- **01-03:** `ErrSnapshotUnsupported` lives in `pkg/storage`, re-exported from `pkg/raft` for ergonomic consumer use.

### Todos
- (none captured yet)

### Blockers
- (none)

## Session Continuity

- **Last session:** 2026-06-18 — completed `01-02-PLAN.md` (PRD/HLD/FLOWS) and `01-03-PLAN.md` (LLD/WIRE) on parallel wave 2.
- **Stopped at:** Completed 01-specs-and-contracts/01-03-PLAN.md
- **Next action:** Execute `01-04-PLAN.md` (CONCURRENCY) then `01-05-PLAN.md` (TESTING/SECURITY/GLOSSARY/CONTRIBUTING/RELEASE_PLAN/PROCESS + PR template).
- **Resume hint:** Specs landed on `feature/specs-and-contracts`: ADR/RFC/journal/PR templates (01-01); `docs/PRD.md` + `docs/HLD.md` + `docs/FLOWS.md` (01-02); `docs/LLD.md` + `docs/WIRE.md` (01-03). LLD locks the Go public surface — Phase 2+ must compile against it. WIRE locks `POST /raft/message` + 204 + JSON schema for 5 RPC kinds + 307 client redirect for `/kv/{key}`. CONCURRENCY (01-04) should cross-reference LLD §Node + §Storage for the goroutine/lock model and reference REPL-09 fsync-before-RPC invariant.
