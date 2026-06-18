# ToyRaft — Project State

**Initialised:** 2026-06-18
**Updated:** 2026-06-18 — completed Phase 1.1 Plan 01 (Go module skeleton + .golangci.yml + ADR-0003)

## Project Reference

- **Project doc:** `.planning/PROJECT.md`
- **Requirements:** `.planning/REQUIREMENTS.md` (116 v1 requirements)
- **Roadmap:** `.planning/ROADMAP.md` (14 phases)
- **Research:** `.planning/research/SUMMARY.md` (+ STACK / FEATURES / ARCHITECTURE / PITFALLS)
- **Core value:** A correct, understandable Raft implementation that can be embedded as a library by other Go services to add leader election + log replication on top of any deterministic state machine.
- **Current focus:** Phase 1 — Specs & Contracts.

## Current Position

- **Phase:** 1.1 (CI Bootstrap) — In progress (1/4 plans complete)
- **Plan:** 01.1-02 next (CI workflow + commitlint + build + ADR-0002)
- **Status:** Phase 1.1 Plan 01 landed Go module skeleton + golangci-lint v2 config + ADR-0003 on `feature/ci-bootstrap`. Two atomic commits: 3caf495 (chore go) and e823cf6 (ci golangci-lint).
- **Branch:** `feature/ci-bootstrap`
- **Progress:** Phase 1.1 [██░░░░░░░░] 25% (1/4 plans)
- **Next phase:** Finish Phase 1.1 (plans 02–04), open PR to merge `feature/ci-bootstrap` → `main`, then Phase 2 (`feature/foundations`).

## Performance Metrics

- Phases complete: 1/15 (Phase 1 — Specs & Contracts; Phase 1.1 INSERTED for CI before any Go code)
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
| 01.1 | 01 | ~6 min | 3 | 4 |

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
- **01-04:** Lock hierarchy is nominal (single mutex) but documented as state → log (in-state) → storage handle to forbid future fine-grained splits without an ADR superseding ADR-0001.
- **01-04:** `applyCh` is unbuffered + sole-sender-closes-via-sync.Once after drain; `inboundCh` is NEVER closed (transport-close-first ordering prevents C-5 send-on-closed panic).
- **01-04:** Hub's five chaos knobs (reorder/duplicate/drop/delay/partition) are public surface in `pkg/transport/inproc` so external consumers can author chaos tests against ToyRaft.
- **01-04:** Failed seeds get committed as regression tests under `test/chaos/seeds/<test-name>.txt` — failed-once seeds run forever; matches CHAOS-07 "flake = P0 bug" policy.
- **01-04:** Three required linearizability CI scenarios (steady-state / leader-churn / packet-loss) each across ≥3 seeds (LIN-04); scripted Figure 7 + Figure 8 always run (LIN-05).
- **01-04:** SECURITY NOT-in-scope list is **closed**: any security feature not on it is also not in scope without an RFC.
- **01-04:** Per-message-fsync vs cluster-durability composition documented in SECURITY (not just CONCURRENCY) — consumers MUST wait for `StateMachine.Apply` before acking writes (D-4 ToyMQ-integration contract).
- **01-04:** Reverse-proxy + mTLS-terminator pattern noted as escape hatch but explicitly out of scope — proxy-induced bugs are not ToyRaft bugs.
- **01-05:** RFC 0001 marked Accepted from creation — it IS the scope lock, not a proposal under discussion.
- **01-05:** Library tags (`v<…>`) and demo binary tags (`toyraftd/v<…>`) partitioned by prefix so the Go module proxy does not cross-notify library consumers about binary releases.
- **01-05:** RELEASE_PLAN documents `make <target>` plus direct `go` equivalents; Makefile itself lands Phase 14 but the surface is fixed now.
- **01-05:** Substantive-RFC test expressed as a binary five-item checklist (public symbol / wire schema / documented invariant / v2→v1 promotion / Out-of-Scope table edit) to make reviewer adjudication mechanical.
- **01-05:** PROCESS is authoritative for governance semantics; CONTRIBUTING is mechanics only. When they disagree, PROCESS wins.
- **01.1-01:** Pin `go` directive to `1.25` (lowest matrix entry), not the host toolchain default, so the 1.25.x CI matrix cell does not refuse the module.
- **01.1-01:** Stub `doc.go` with `package toyraft` lands now to retire Pitfall 2 (empty-tree `go test ./...` exit 1) before plan 02's CI workflow runs.
- **01.1-01:** `.golangci.yml` uses v2 schema with `linters.default: none` + explicit 7-linter enable list (errcheck, govet, ineffassign, staticcheck, unused, misspell, revive) — no silent upgrades on linter-set churn.
- **01.1-01:** `gosec` deliberately excluded — false-positive rate too high for `math/rand`-with-seeds + unbuffered I/O patterns; ADR-0003 documents the opt-out; Phase 14 may revisit.
- **01.1-01:** `gosimple` dropped from enable list because v2 merged it into `staticcheck` (intent preserved). ADR-0003 records the merge so future readers do not re-add it.
- **01.1-01:** Config schema corrected from the planner's v1-style (`linters-settings` top-level, `issues.exclude-rules`) to v2 layout (`linters.settings`, `linters.exclusions.rules`) per `golangci-lint config verify`.
- **01.1-01:** Config + its justifying ADR ship in one atomic commit (`e823cf6`) — house-style: the why next to the what.

### Todos
- (none captured yet)

### Blockers
- (none)

## Session Continuity

- **Last session:** 2026-06-18T15:47:42.205Z
- **Stopped at:** Completed 01.1-ci-bootstrap/01.1-01-PLAN.md — go module skeleton + .golangci.yml + ADR-0003 landed
- **Next action:** Execute plan 01.1-02 (CI workflow `.github/workflows/ci.yml` with lint/test/commitlint/build jobs + ADR-0002 bringing CI forward).
- **Resume hint:** On branch `feature/ci-bootstrap` (off `main`). Plan 01.1-01 landed `go.mod` (`go 1.25`), `doc.go` (stub), `.golangci.yml` (v2 schema, 7 linters), and `docs/adr/0003-golangci-lint-config.md`. Two commits: `3caf495` (chore go) and `e823cf6` (ci golangci-lint). `go test ./...` exits 0; `golangci-lint config verify` exits 0; `golangci-lint run` exits 0. Plan 02 will land the GitHub Actions workflow and ADR-0002; that workflow's `lint` job runs `config verify` per Pitfall 6, the `test` job exercises the `go.mod` skeleton landed here.
