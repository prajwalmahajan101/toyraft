# ToyRaft — Project State

**Initialised:** 2026-06-18
**Updated:** 2026-06-18 — completed Phase 1 Plan 2 (PRD, HLD, FLOWS with Mermaid sequence diagrams)

## Project Reference

- **Project doc:** `.planning/PROJECT.md`
- **Requirements:** `.planning/REQUIREMENTS.md` (116 v1 requirements)
- **Roadmap:** `.planning/ROADMAP.md` (14 phases)
- **Research:** `.planning/research/SUMMARY.md` (+ STACK / FEATURES / ARCHITECTURE / PITFALLS)
- **Core value:** A correct, understandable Raft implementation that can be embedded as a library by other Go services to add leader election + log replication on top of any deterministic state machine.
- **Current focus:** Phase 1 — Specs & Contracts.

## Current Position

- **Phase:** 1 (Specs & Contracts) — In progress
- **Plan:** 2/5 complete; next plan: `01-03-PLAN.md`
- **Status:** Plan 01-02 complete (PRD + HLD + FLOWS shipped)
- **Branch:** `feature/specs-and-contracts`
- **Progress:** [████░░░░░░] 40%

## Performance Metrics

- Phases complete: 0/14
- Plans complete: 2/5 (in Phase 1)
- Requirements satisfied: 0/116 (Plan 01-01 partial: DOC-14, PROC-04, PROC-05, DOC-13 template-only; Plan 01-02: DOC-01, DOC-02, DOC-08)
- ADRs written: 1/15 (target) — ADR-0000 meta
- Journal entries: 0/14

### Per-plan metrics

| Phase | Plan | Duration | Tasks | Files |
|-------|------|----------|-------|-------|
| 01 | 01 | ~2 min | 3 | 5 |
| 01 | 02 | ~4 min | 3 | 3 |

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

### Todos
- (none captured yet)

### Blockers
- (none)

## Session Continuity

- **Last session:** 2026-06-18T14:42:24.798Z
- **Stopped at:** Completed 01-specs-and-contracts/01-02-PLAN.md
- **Next action:** Execute `01-03-PLAN.md` (Contract docs: LLD + WIRE).
- **Resume hint:** PRD/HLD/FLOWS now exist under `docs/`. Next plan locks the API contract (LLD interface signatures + invariants) and the HTTP/JSON wire envelope (WIRE) — follows atomic-commit order steps 8–9 in `.planning/phases/01-specs-and-contracts/01-RESEARCH.md §Atomic Commit Ordering`. HLD forward-references LLD/WIRE; drift will be a review-blocking finding. Branch `feature/specs-and-contracts` is active.
