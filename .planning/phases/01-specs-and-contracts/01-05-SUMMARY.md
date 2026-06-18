---
phase: 01-specs-and-contracts
plan: 05
subsystem: docs
tags: [glossary, contributing, release, process, rfc, semver, goreleaser, conventional-commits]

# Dependency graph
requires:
  - phase: 01-specs-and-contracts/01-01
    provides: ADR + RFC + journal + PR templates (RFC 0001 uses docs/rfc/TEMPLATE.md; CONTRIBUTING references .github/pull_request_template.md)
  - phase: 01-specs-and-contracts/01-02
    provides: PRD + HLD (scope framing reused by RFC 0001 and GLOSSARY cross-refs)
  - phase: 01-specs-and-contracts/01-03
    provides: LLD + WIRE (GLOSSARY project-term definitions point at these specs)
provides:
  - docs/GLOSSARY.md — Raft + project vocabulary (DOC-09)
  - docs/CONTRIBUTING.md — build/test/lint + branch model + commit cheatsheet + skill bindings (DOC-10)
  - docs/RELEASE_PLAN.md — semver-via-tags for library, GoReleaser for demo, release gates, tag namespace split (DOC-11, QUAL-07, QUAL-08)
  - docs/PROCESS.md — codified 6 Working Agreements + skill bindings + enforcement (DOC-12)
  - docs/rfc/0001-v1-scope-and-non-goals.md — Accepted seed RFC locking v1 contract (DOC-13)
affects: [02-foundations, 03-storage-interface, all later phases (PROCESS + CONTRIBUTING govern every PR), 14-release (RELEASE_PLAN gates), any v2 RFC (must cite RFC 0001 to widen scope)]

# Tech tracking
tech-stack:
  added: [GoReleaser v2 policy (referenced for Phase 14), golangci-lint v2 policy, commitlint policy, semver tag policy]
  patterns:
    - "Two-prefix release tagging — library `v<…>` + demo `toyraftd/v<…>` keeps Go module proxy from notifying library consumers about binary releases"
    - "Mechanics vs semantics split — CONTRIBUTING owns commands/branches/checklists; PROCESS owns ADR/RFC/journal meanings. Cross-link, don't duplicate"
    - "Substantive RFC decision rule — explicit binary test list (touches public symbol? wire schema? out-of-scope table?) so reviewers can adjudicate without re-litigating each time"
    - "Locked-scope RFC pattern — RFC 0001 is itself the scope lock; widening v1 requires a new RFC superseding it"

key-files:
  created:
    - docs/GLOSSARY.md
    - docs/CONTRIBUTING.md
    - docs/RELEASE_PLAN.md
    - docs/PROCESS.md
    - docs/rfc/0001-v1-scope-and-non-goals.md
  modified: []

key-decisions:
  - "RFC 0001 marked Accepted (not Draft) at creation — it IS the lock, not a proposal under discussion"
  - "Library tags and demo tags partitioned by prefix (v<…> vs toyraftd/v<…>) to prevent Go module proxy from cross-notifying consumers"
  - "RELEASE_PLAN documents Makefile targets as forward references — Makefile itself lands Phase 14, but the surface is fixed now so docs don't churn"
  - "Substantive RFC test articulated as a binary checklist (public symbol / wire / invariant / scope-table) rather than judgement-call language"
  - "Fast-rollback wire fields stay in v1 Message schema; only the leader-side optimisation is deferred (matches 01-02 decision recorded for ConflictTerm/ConflictIndex)"
  - "PROCESS over CONTRIBUTING in case of disagreement — PROCESS is the authoritative semantics doc; CONTRIBUTING is mechanics only"

patterns-established:
  - "Doc cross-reference policy — every spec references companion docs by relative path so reviewers can navigate without a TOC"
  - "Spec doc header convention — Status / Date / Scope mirrors ADR shape, treating specs as first-class governance artefacts (continues 01-02 precedent)"
  - "Out-of-scope items always link to a v2 bucket — prevents 'lost feature' anxiety; nothing is dropped, only deferred"

# Metrics
duration: 5min
completed: 2026-06-18
---

# Phase 1 Plan 5: GLOSSARY / CONTRIBUTING / RELEASE_PLAN / PROCESS / RFC 0001 Summary

**Closed Phase 1 with the meta-docs and the seed RFC that locks v1 scope, codifying the six Working Agreements and partitioning library vs binary release tag namespaces**

## Performance

- **Duration:** ~5 min
- **Started:** 2026-06-18T14:46:20Z
- **Completed:** 2026-06-18T14:51:35Z
- **Tasks:** 3 (5 atomic commits)
- **Files modified:** 5 created, 0 modified

## Accomplishments

- `docs/GLOSSARY.md` — full Raft vocabulary (roles, terms, safety properties, mechanisms), Figure 7/8 explanatory references, ToyRaft-specific terms (Hub, Driver, ApplyChannel, LeaderHint, HardState, Entry.Data, toyraftd/toyraftctl).
- `docs/CONTRIBUTING.md` — build/test/lint command table with Makefile-target ↔ direct-`go` equivalences, all 14 locked branch names per phase, conventional-commit scope cheatsheet (`raft`, `storage`, `transport`, `kvsm`, `toyraftd`, `toyraftctl`, `chaos`, `lin`, `ci`, `adr`, `rfc`, `journal`), PR checklist preview.
- `docs/RELEASE_PLAN.md` — library released via plain `v<…>` Git tags (no GoReleaser), demo binary released via GoReleaser v2 under `toyraftd/v<…>` tag prefix on a `{linux,macOS}×{amd64,arm64}` matrix, release gates (lint + race + chaos + linearizability + journal + CHANGELOG).
- `docs/PROCESS.md` — six Working Agreements codified with skill bindings (`architecture-decision-records`, `code_assist:journal`), substantive-RFC decision rule, enforcement table (PR template + commitlint CI + `make hooks`).
- `docs/rfc/0001-v1-scope-and-non-goals.md` — Accepted seed RFC. Every out-of-scope item linked to its v2 bucket (SNAP-*, MEMB-*, PERF-*, PROD-*). Prior art cites the etcd v1 → v3 wire-incompatible scope-creep lesson and the three mitigations ToyRaft adopts (snapshot stubs, reserved wire fields, JSON forward-compat rule).

## Task Commits

Each task was committed atomically:

1. **Task 1a: GLOSSARY** — `6d74bc5` (docs)
2. **Task 1b: CONTRIBUTING** — `57e1bb6` (docs)
3. **Task 2a: RELEASE_PLAN** — `e973824` (docs)
4. **Task 2b: PROCESS** — `3949c90` (docs)
5. **Task 3: RFC 0001 v1 scope and non-goals** — `daae315` (docs(rfc))

_Note: Tasks 1 and 2 each produced two atomic commits per the plan's explicit two-commit instruction; Task 3 produced one._

## Files Created/Modified

- `docs/GLOSSARY.md` — Shared Raft + project vocabulary, single source of truth for terminology.
- `docs/CONTRIBUTING.md` — Contributor workflow: build/test/lint commands, branch model, conventional-commit cheatsheet, PR checklist preview.
- `docs/RELEASE_PLAN.md` — Library semver-via-tags + demo binary GoReleaser policy with two-prefix tag namespace split.
- `docs/PROCESS.md` — Six Working Agreements + skill bindings + enforcement; authoritative companion to CONTRIBUTING.
- `docs/rfc/0001-v1-scope-and-non-goals.md` — Accepted seed RFC locking the v1 contract.

## Decisions Made

- **RFC 0001 status = Accepted from the start.** It is the lock, not a proposal. Treating it as Draft would imply discussion is open.
- **Two-prefix release tags.** `v<…>` for library (Go module proxy sees these); `toyraftd/v<…>` for binary (proxy ignores these). Prevents binary releases from looking like library version bumps in `pkg.go.dev` / Dependabot.
- **RELEASE_PLAN documents `make <target>` plus direct `go` equivalents.** Makefile lands Phase 14, but the contract is fixed now so docs don't churn when it arrives.
- **Substantive-RFC test is a binary checklist, not a judgement call.** Five concrete conditions (public symbol changes, wire schema changes, documented-invariant changes, v2→v1 promotion, Out-of-Scope table changes) make reviewer adjudication mechanical.
- **PROCESS is authoritative over CONTRIBUTING for semantics.** CONTRIBUTING is mechanics only; if the two ever disagree, PROCESS wins.
- **Fast-rollback wire fields ship in v1; only leader-side use is deferred.** Reaffirms the 01-02 decision and threads it through RFC 0001's PERF-01 rationale so the rule is recorded in the scope-lock doc too.

## Deviations from Plan

None - plan executed exactly as written.

The plan was unusually well-specified (research doc had outlines for every doc section), so all five files were produced from the plan + research material without auto-fixes, blocking issues, or architectural questions.

## Issues Encountered

None. Plan 01-04 was running in parallel on the same branch and interleaved commits between mine (no conflict — disjoint file sets per the orchestrator's note).

## User Setup Required

None - documentation-only phase. No environment variables, no external services.

## Next Phase Readiness

- Phase 1 closes with this plan. All 12 docs + ADR/RFC/journal/PR templates land on `feature/specs-and-contracts` before any `feat:` commit — DOC-15 ordering invariant satisfied.
- Phase 2 (`feature/foundations`) can start: `FOUND-01..FOUND-05` (Entry, Message, Role, Term, Index, NodeID, HardState public types + in-memory Log + invariants).
- Phase 2's first action: open `feature/foundations` branch off `main` after this branch merges, and produce an ADR for whatever shape the `Log` type lands in (likely "single-mutex policy" — ADR-0001 in the queue).
- The PR closing Phase 1 needs a `.journal/M1.md` entry per PROC-02. That entry is the responsibility of the phase-close orchestrator, not this plan.

## Self-Check: PASSED

All five planned files exist at expected paths. All five task commits found in `git log --all` (6d74bc5, 57e1bb6, e973824, 3949c90, daae315). SUMMARY.md present.

---
*Phase: 01-specs-and-contracts*
*Plan: 05*
*Completed: 2026-06-18*
