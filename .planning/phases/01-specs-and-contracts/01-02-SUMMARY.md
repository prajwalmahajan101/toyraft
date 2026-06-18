---
phase: 01-specs-and-contracts
plan: 02
subsystem: docs
tags: [prd, hld, flows, mermaid, architecture, raft]

# Dependency graph
requires:
  - phase: 01-specs-and-contracts/01
    provides: ADR/RFC/journal/PR templates (process infrastructure that PRD/HLD/FLOWS reference)
provides:
  - docs/PRD.md (DOC-01: consumer-facing product contract)
  - docs/HLD.md (DOC-02: component map, role FSM, build-order rationale, directory layout)
  - docs/FLOWS.md (DOC-08: Mermaid sequence diagrams for election, client write, heartbeat/catch-up)
affects: [01-03 LLD/WIRE, 01-04 CONCURRENCY/TESTING/SECURITY, 01-05 PROCESS/RFC, every implementation phase 2-14]

# Tech tracking
tech-stack:
  added: [Mermaid sequenceDiagram + stateDiagram-v2 (GitHub-native rendering)]
  patterns: [docs source from .planning/ verbatim; cross-references with forward refs to later-arriving docs; Out-of-Scope as table not bullets (DRY); each doc has Status/Date/Scope header matching ADR house style]

key-files:
  created:
    - docs/PRD.md
    - docs/HLD.md
    - docs/FLOWS.md
  modified: []

key-decisions:
  - "Use Mermaid (not PlantUML or ASCII) for sequence diagrams - renders natively on GitHub since Feb 2022, no toolchain required"
  - "Render role FSM as Mermaid stateDiagram-v2 in HLD (consistent with FLOWS choice)"
  - "Out-of-Scope shipped as a single non-goals table in PRD (DRY - not duplicated as bullets-then-table)"
  - "FLOWS documents fast-rollback wire fields (ConflictTerm/Index) in v1 but defers the leader-side optimisation to a future ADR (per REPL-04 slow-probe v1)"
  - "Each Phase 1 doc carries a Status/Date/Scope header mirroring the ADR template, treating specs as first-class governance artifacts"

patterns-established:
  - "Doc header pattern: Status / Date / Scope (mirrors ADR template) - signals specs are governance, not prose"
  - "Source attribution: each section cites its .planning/ origin so drift is detectable"
  - "Cross-reference footer: every doc ends with a Cross-references section listing sibling docs (including forward refs)"
  - "Failure-mode addendum per flow: each Mermaid sequence diagram in FLOWS is followed by a 2-4 bullet 'what goes wrong when X' list"

# Metrics
duration: ~4 min
completed: 2026-06-18
---

# Phase 1 Plan 2: Product & High-Level Design Summary

**PRD locks the consumer-facing contract; HLD captures the core/plug-in/driver axis and 14-phase build chain; FLOWS ships three Mermaid sequence diagrams (election, client write, heartbeat/catch-up) with per-flow failure modes.**

## Performance

- **Duration:** ~4 min
- **Started:** 2026-06-18T14:37:24Z
- **Completed:** 2026-06-18T14:41:20Z
- **Tasks:** 3
- **Files created:** 3

## Accomplishments

- Consumer-facing product contract (`docs/PRD.md`) — What it is / isn't / Scope / Non-goals (table) / Success criteria (4 Raft invariants + Phase 12 linearizability gate) / Consumer integration targets (`toykv-cluster`, `toymq-cluster`) with the fsync-on-PUB cross-project flag.
- Architectural decomposition (`docs/HLD.md`) — full component map, Mermaid `stateDiagram-v2` for the role FSM, ASCII three-layer axis diagram, build-order rationale tracing the `1→2→…→7, 8∥9, 10→11→12` hard chain, and the verbatim directory layout from `ARCHITECTURE.md`.
- Three load-bearing sequence flows (`docs/FLOWS.md`) — election (with HardState fsync ordering invariant), client write (with current-term commit rule), heartbeat / catch-up (slow probe v1, fast-rollback wire fields shipped but optimisation deferred). Each diagram has a failure-mode addendum.

## Task Commits

1. **Task 1: Write docs/PRD.md** — `72b31d7` (docs)
2. **Task 2: Write docs/HLD.md** — `f4ab7d2` (docs)
3. **Task 3: Write docs/FLOWS.md with Mermaid sequence diagrams** — `cb2e87e` (docs)

## Files Created/Modified

- `docs/PRD.md` — Consumer-facing product contract; sources `PROJECT.md` §Active + Out-of-Scope verbatim.
- `docs/HLD.md` — Component map, role FSM (Mermaid `stateDiagram-v2`), build-order rationale, three-layer plug-in/core/driver axis, directory layout.
- `docs/FLOWS.md` — Three Mermaid `sequenceDiagram` blocks (election, client write, heartbeat/catch-up) with per-flow failure-mode addenda.

## Decisions Made

- **Mermaid over PlantUML/ASCII for diagrams.** GitHub renders Mermaid natively (since Feb 2022); no toolchain or server required. Applies to both `FLOWS.md` sequence diagrams and the `HLD.md` role FSM.
- **Out-of-Scope as a single table in PRD.** Avoided the DRY trap of bullets-then-table; the table form is the source of truth.
- **Fast-rollback wire fields ship in v1 but the leader-side optimisation does not.** Per `REQUIREMENTS.md` REPL-04. Documenting both in `FLOWS.md` keeps the wire format forward-compatible while keeping v1 minimal; the optimisation is flagged for a future ADR.
- **Each Phase 1 doc carries Status/Date/Scope header.** Treats specs as first-class governance artifacts (mirrors ADR template), making "spec staleness" visible at a glance.
- **Forward references are explicit.** PRD/HLD/FLOWS link to LLD/WIRE/CONCURRENCY/etc. that haven't landed yet; the cross-reference footer marks these as "lands later in this phase". Drift detectable post-merge.

## Deviations from Plan

None - plan executed exactly as written. All three tasks shipped in the documented atomic-commit order (PRD → HLD → FLOWS).

## Issues Encountered

None.

## User Setup Required

None - documentation-only plan, no external service configuration required.

## Self-Check: PASSED

- `docs/PRD.md` — FOUND (commit `72b31d7`)
- `docs/HLD.md` — FOUND (commit `f4ab7d2`)
- `docs/FLOWS.md` — FOUND (commit `cb2e87e`, contains 3 Mermaid sequenceDiagram blocks verified)
- Verification commands from PLAN ran clean for all three tasks.

## Next Phase Readiness

- **Plan 01-03 (Contract docs: LLD, WIRE)** can start immediately. PRD/HLD/FLOWS establish the consumer-facing surface and the architectural axes that LLD will lock at the signature level.
- HLD explicitly forward-references LLD for interface signatures and WIRE for the HTTP/JSON envelope — drift between LLD/WIRE and HLD will be a review-blocking finding (Working Agreement 4).
- FLOWS's "fast-rollback deferred to ADR" note becomes a TODO for the first implementation-phase ADR sweep.

---
*Phase: 01-specs-and-contracts*
*Plan: 02*
*Completed: 2026-06-18*
