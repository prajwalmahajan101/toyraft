# ToyRaft — Project State

**Initialised:** 2026-06-18
**Updated:** 2026-06-18 — completed Phase 1 Plans 4 and 5 (parallel wave 3); Phase 1 complete

## Project Reference

- **Project doc:** `.planning/PROJECT.md`
- **Requirements:** `.planning/REQUIREMENTS.md` (116 v1 requirements)
- **Roadmap:** `.planning/ROADMAP.md` (14 phases)
- **Research:** `.planning/research/SUMMARY.md` (+ STACK / FEATURES / ARCHITECTURE / PITFALLS)
- **Core value:** A correct, understandable Raft implementation that can be embedded as a library by other Go services to add leader election + log replication on top of any deterministic state machine.
- **Current focus:** Phase 1 — Specs & Contracts.

## Current Position

- **Phase:** 1 (Specs & Contracts) — Complete
- **Plan:** 5/5 complete; ready to close Phase 1 with `.journal/M1.md` and merge `feature/specs-and-contracts`
- **Status:** All 5 Phase 1 plans complete
- **Branch:** `feature/specs-and-contracts`
- **Progress:** [██████████] 100%
- **Next phase:** Phase 2 — Foundations (`feature/foundations`), starts with FOUND-01..FOUND-05

## Performance Metrics

- Phases complete: 1/14 (Phase 1 — Specs & Contracts)
- Plans complete: 5/5 (in Phase 1)
- Requirements satisfied: 0/116 implementations; spec-only: Phase 1 lands DOC-01..DOC-15 + PROC-04 + PROC-05 (Plan 01-05: DOC-09, DOC-10, DOC-11, DOC-12, DOC-13 second half) — implementations land Phase 2+
- ADRs written: 1/15 (target) — ADR-0000 meta
- Journal entries: 0/14

### Per-plan metrics

| Phase | Plan | Duration | Tasks | Files |
|-------|------|----------|-------|-------|
| 01 | 01 | ~2 min | 3 | 5 |
| 01 | 02 | ~4 min | 3 | 3 |
| 01 | 03 | ~6 min | 2 | 2 |
| 01 | 04 | ~6 min | 3 | 3 |
| 01 | 05 | ~5 min | 3 | 5 |

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
- **01-05:** RFC 0001 marked Accepted from creation — it IS the scope lock, not a proposal under discussion.
- **01-05:** Library tags (`v<…>`) and demo binary tags (`toyraftd/v<…>`) partitioned by prefix so the Go module proxy does not cross-notify library consumers about binary releases.
- **01-05:** RELEASE_PLAN documents `make <target>` plus direct `go` equivalents; Makefile itself lands Phase 14 but the surface is fixed now.
- **01-05:** Substantive-RFC test expressed as a binary five-item checklist (public symbol / wire schema / documented invariant / v2→v1 promotion / Out-of-Scope table edit) to make reviewer adjudication mechanical.
- **01-05:** PROCESS is authoritative for governance semantics; CONTRIBUTING is mechanics only. When they disagree, PROCESS wins.

### Todos
- (none captured yet)

### Blockers
- (none)

## Session Continuity

- **Last session:** 2026-06-18T14:53:43.457Z
- **Stopped at:** Completed 01-specs-and-contracts/01-05-PLAN.md — Phase 1 complete
- **Next action:** Write `.journal/M1.md` (PROC-02), then open PR to merge `feature/specs-and-contracts` → `main`. After merge, open `feature/foundations` branch off `main` and start Phase 2 (FOUND-01..FOUND-05).
- **Resume hint:** Phase 1 closed with all 12 specs + ADR/RFC/journal/PR templates committed on `feature/specs-and-contracts` before any `feat:` commit (DOC-15 invariant satisfied). Plan 01-05 added GLOSSARY (DOC-09), CONTRIBUTING (DOC-10), RELEASE_PLAN (DOC-11), PROCESS (DOC-12), RFC 0001 (DOC-13 second half). Phase 2 starts with the in-memory `Log` type — likely first ADR-0001 will codify the single-mutex policy. RFC 0001 locks v1 scope: any later plan that wants to widen it must write a superseding RFC.
