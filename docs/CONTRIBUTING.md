# Contributing to ToyRaft

**Status:** Accepted
**Date:** 2026-06-18
**Scope:** project-wide contributor workflow

ToyRaft inherits the [toykv][toykv] / [toymq][toymq] house style:
stdlib-only consensus core, ADR per non-obvious decision, layered tests,
conventional commits, one branch per phase. This document covers
**mechanics** (build/test/lint/PR); the **semantics** of ADRs, RFCs, and
journal entries live in [`docs/PROCESS.md`](./PROCESS.md). Cross-link,
don't duplicate.

[toykv]: https://github.com/prajwalmahajan101/toykv
[toymq]: https://github.com/prajwalmahajan101/toymq

## Build, Test, Lint

ToyRaft's tooling is driven through `make`. The **`hooks`** target ships
in Phase 1.1 (CI bootstrap — see ADR-0002). The remaining build/test/
chaos/demo targets land in later phases; during Phases 2–13 they are
equivalent to direct `go` invocations listed below.

| Target            | What it does                                                                            | Direct equivalent                            |
| ----------------- | --------------------------------------------------------------------------------------- | -------------------------------------------- |
| `make build`      | Builds the library + `cmd/toyraftd` + `cmd/toyraftctl`                                  | `go build ./...`                             |
| `make test`       | Runs unit + integration tests, no race detector                                         | `go test ./...`                              |
| `make test-race`  | Runs the full test suite with the race detector (QUAL-02)                               | `go test -race ./...`                        |
| `make chaos`      | Runs the seeded chaos suite (CHAOS-01)                                                  | `go test -tags=chaos ./test/chaos/...`       |
| `make lint`       | Runs golangci-lint v2.x with the project's `.golangci.yml` (QUAL-01)                    | `golangci-lint run ./...`                    |
| `make hooks`      | Rebinds `core.hooksPath` to `.githooks/` (commit-msg regex + pre-commit gofmt/vet) (QUAL-10, PROC-08) | `git config core.hooksPath .githooks`        |
| `make demo`       | Boots a 3-node `toyraftd` cluster on `localhost:9001..9003` (DEMO-06)                   | `./scripts/demo.sh`                          |

**Run before every PR:** `make lint` clean + `make test-race` green.
Chaos tests are required for any change touching consensus core,
storage, or transport.

## Branch Model

- **One branch per phase.** Open a branch off `main` named
  `feature/<descriptive-slug>` — never with a phase number in the
  branch name (PROC-07).
- **No direct commits to `main`.** Every phase merges back via PR after
  the verifier passes and the journal entry + any required ADR(s) land.
- **Branch names are locked at roadmap time** so the planner can refer
  to them by name. The 14 reserved names:

  | Phase | Branch                              |
  | ----- | ----------------------------------- |
  | 1     | `feature/specs-and-contracts`       |
  | 2     | `feature/foundations`               |
  | 3     | `feature/storage-interface`         |
  | 4     | `feature/test-infra`                |
  | 5     | `feature/election`                  |
  | 6     | `feature/replication`               |
  | 7     | `feature/library-api`               |
  | 8     | `feature/storage-file`              |
  | 9     | `feature/http-transport`            |
  | 10    | `feature/reference-demo`            |
  | 11    | `feature/chaos-suite`               |
  | 12    | `feature/linearizability`           |
  | 13    | `feature/netns-chaos`               |
  | 14    | `feature/release`                   |

  See `.planning/REQUIREMENTS.md` PROC-07 for the canonical list.

## Conventional Commits

All commits follow the [Conventional Commits][cc] spec (PROC-08).
Subject line ≤72 chars, no trailing period, imperative mood.

[cc]: https://www.conventionalcommits.org/en/v1.0.0/

**Type prefixes:** `feat | fix | refactor | docs | test | chore`.

**Scope cheatsheet** — pick the narrowest scope that names what
changed. Scopes are reserved (one per subsystem); add a scope only when
it disambiguates.

| Scope           | When to use                                       | Example                                                       |
| --------------- | ------------------------------------------------- | ------------------------------------------------------------- |
| `(adr)`         | Adding or amending an ADR                         | `docs(adr): add ADR-0007 single-mutex policy`                 |
| `(rfc)`         | Adding or amending an RFC                         | `docs(rfc): add RFC 0003 fast-rollback`                       |
| `(journal)`     | Phase journal entry                               | `docs(journal): add M5 journal`                               |
| `(raft)`        | Consensus core (`pkg/raft`)                       | `feat(raft): implement RequestVote`                           |
| `(storage)`     | Storage interface or impls (`pkg/storage/*`)      | `feat(storage): add file impl with fsync ordering`            |
| `(transport)`   | Transport interface or impls (`pkg/transport/*`)  | `feat(transport): add inproc Hub partition knob`              |
| `(kvsm)`        | Reference KV state machine (`pkg/kvsm`)           | `feat(kvsm): add DELETE op`                                   |
| `(toyraftd)`    | Demo server binary (`cmd/toyraftd`)               | `feat(toyraftd): wire HTTP redirect on ErrNotLeader`          |
| `(toyraftctl)`  | Demo CLI binary (`cmd/toyraftctl`)                | `feat(toyraftctl): add status subcommand`                     |
| `(chaos)`       | Chaos test harnesses (`test/chaos/`)              | `test(chaos): add Figure 8 seeded scenario`                   |
| `(lin)`         | Linearizability suite                             | `test(lin): add leader-churn scenario`                        |
| `(ci)`          | CI config                                         | `chore(ci): add macOS race matrix`                            |

Examples without scope are valid too: `docs: add CONTRIBUTING workflow
guide`, `refactor: extract maybeStepDown helper`.

`commitlint` is enforced both in CI and via the commit-msg git hook
installed by `make hooks`.

## ADR / RFC / Journal Expectations

Three kinds of governance docs accompany code changes. The semantics —
when to write each, how they relate — live in
[`docs/PROCESS.md`](./PROCESS.md). The mechanics:

- **ADRs** (`docs/adr/NNNN-<slug>.md`) — One per architecture decision.
  Drafted via the **`architecture-decision-records`** skill. The PR that
  depends on an ADR links it in the description (enforced by the PR
  template).
- **RFCs** (`docs/rfc/NNNN-<slug>.md`) — One per substantive proposal
  (public API shape, contract changes, scope expansion). Drafted from
  `docs/rfc/TEMPLATE.md`. Promoted to an ADR after acceptance.
- **Journal entries** (`.journal/M{n}.md`) — One per completed phase.
  Drafted via the **`code_assist:journal`** skill from
  `.journal/TEMPLATE.md`. Committed before the phase is marked complete
  in `STATE.md` (PROC-02).

See `docs/PROCESS.md` for the full working agreements.

## Pull Request Checklist

The PR template at
[`.github/pull_request_template.md`](../.github/pull_request_template.md)
is enforced — reviewers will reject PRs that skip the checklist. The
template requires confirming:

- Conventional Commit subject (≤72 chars, no trailing period)
- No spec/code drift — if behaviour diverges from `docs/`, an ADR
  justifies it or the spec is updated in this PR
- RFC was discussed first if the PR changes public API shape, contract,
  or v1 scope
- ADR is included if the PR makes an architectural decision
- Journal entry is included if the PR closes a phase
- `make lint` clean, `make test-race` green
- Branch follows `feature/<descriptive-slug>` (no phase numbers)

Reviewers should treat any unchecked box as a hold. **No spec/code
drift** is the heaviest item: it implements Working Agreement 4 (specs
are source of truth) and is the single most common failure mode for a
documentation-heavy project.

## Getting Started

1. Read `docs/PRD.md`, `docs/HLD.md`, and `docs/PROCESS.md` end to end —
   the project is intentionally specs-first.
2. Skim `docs/GLOSSARY.md` so terminology lands the same way for
   everyone.
3. Pick a phase from `.planning/ROADMAP.md` that depends only on phases
   already complete. Open the branch named in the table above.
4. Read the relevant existing code and **match its patterns** rather
   than inventing new ones. When a pattern is missing, add an ADR.
5. Test in this order: unit → integration → seeded chaos →
   linearizability. Each layer proves something the previous can't.
6. Open the PR. Fill out every box in the checklist. Link any ADR /
   RFC the change depends on.

Welcome aboard.
