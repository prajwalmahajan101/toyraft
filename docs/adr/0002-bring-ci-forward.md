# ADR-0002 — Bring CI forward to Phase 1.1

**Status:** Accepted
**Date:** 2026-06-18
**Scope:** project-wide (CI / quality gates)

## Context

`ROADMAP.md` originally placed all CI infrastructure in Phase 14 (QUAL-04
— "documented quality gates" plus GoReleaser publish). Sibling projects
[toykv] and [toymq] both shipped CI from their first code-bearing phase
and never regretted it: every `feat:` commit lands on green from day one.

Deferring CI to Phase 14 would let untested `feat:` commits accumulate
from Phase 2 (foundations) through Phase 13 (release prep) — 11 phases of
unenforced quality. Two specific requirements belong at the gate, not at
the end:

- **PROC-08** (every commit Conventional-Commit-clean and CI-green) is
  unenforceable without a `commitlint` job present from the first PR.
- **QUAL-02** (cross-platform race-test on Go 1.25 + 1.26 × Ubuntu +
  macOS) catches platform-specific data races at introduction, not after
  a year of accumulated change.

The constraint that makes bringing CI forward cheap right now: **there
is zero Go code in the repo today** (Phase 1.1 plan 01 landed only
`go.mod` + a stub `doc.go`). The workflow can be landed and pass by
construction: `gofmt -l .` returns empty, `go vet ./...` returns 0
because the package list is empty/stub, `go test -race ./...` returns 0
("no test files" is a pass), `golangci-lint config verify` exits 0 on a
valid v2 config, cross-compile of a stub package succeeds for all four
targets. The same workflow exercises real code from Phase 2 onward
without modification.

This ADR pairs with [ADR-0003] (linter set), which lands the
`.golangci.yml` consumed by the `lint` job introduced here.

## Decision

We bring the four-job CI workflow forward to Phase 1.1. The workflow
lives at `.github/workflows/ci.yml` and defines four jobs:

- **lint** — `golangci-lint` v2 via `golangci/golangci-lint-action@v6`
  with `install-mode: goinstall` (Go 1.26 source requires this; the
  prebuilt v6 binary ships compiled with Go 1.24 and cannot parse 1.26
  packages). A `golangci-lint config verify` step runs first to catch
  `.golangci.yml` typos on the near-empty tree.
- **test** — matrix `{ubuntu-latest, macos-latest} × {1.25.x, 1.26.x}` =
  four cells, each running `gofmt -l .` (fails on non-empty output),
  `go vet ./...`, and `go test -race -timeout 5m ./...`. `fail-fast:
  false` so a failure in one cell still surfaces results from the others.
- **commitlint** — `wagoid/commitlint-github-action@v6` on
  `pull_request` events only (push to `main` is post-merge; the gate
  already fired). `fetch-depth: 0` so the PR commit range is walked
  correctly across force-pushes and merge commits.
- **build (cross-compile)** — single runner, env-var loop over
  `linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64` invoking
  `GOOS=<os> GOARCH=<arch> go build ./...` for each (see
  *Cross-compile-in-single-job* below for the rationale).

Phase 14 (QUAL-04) retains the **residual** quality work: prove
`golangci-lint run` clean against the full Phase 2–13 codebase,
land observability counters (`raft_*` metrics), wire GoReleaser into a
tag-driven `release.yml`, and audit branch-protection compliance after
twelve phases of drift.

### Branch protection on `main`

Branch-protection rules cannot be expressed in repo YAML (no rule file
exists in `.github/`) — they live in **Repo Settings → Branches** and
must be applied manually after the workflow's first successful run
(see Pitfall 4 in 01.1-RESEARCH.md). Required settings:

- Require a pull request before merging.
- Require ≥1 approving review **(self-merge allowed for the solo
  maintainer — toggle off once a second committer joins)**.
- Require **all CI jobs listed below** to pass before merging.
- Require linear history (no merge commits on `main`).
- Require conversation resolution before merging.
- Restrict who can push to matching branches (maintainer only).

The settings get captured for replay in `.journal/M1.1.md` (plan 04)
alongside the PR URL of the first green run.

### Required-check names

Branch protection requires picking check names from a dropdown that
only lists checks GitHub has already observed. These are the **seven
required-check names** that `.github/workflows/ci.yml` produces — pick
exactly these in the UI, verbatim, including spaces and parentheses:

- `lint`
- `test (ubuntu-latest / go 1.25.x)`
- `test (ubuntu-latest / go 1.26.x)`
- `test (macos-latest / go 1.25.x)`
- `test (macos-latest / go 1.26.x)`
- `commitlint`
- `build (cross-compile)`

These names come from the `name:` fields in `.github/workflows/ci.yml`
(top-level for `lint` / `commitlint` / `build`, and the matrix-templated
`name: test (${{ matrix.os }} / go ${{ matrix.go }})` for the four `test`
cells). **If a `name:` field in the workflow ever changes, this list and
the branch-protection UI selection must change in the same commit.**

### Cross-compile-in-single-job (ToyRaft delta from siblings)

Neither sibling does explicit `arm64` cross-compile in CI. ToyRaft does,
because the library's chaos suite + lockstep tick scheduler expose
ordering pitfalls that historically differ between `amd64` and `arm64`
memory models, and we want compile-time coverage of both before runtime
coverage shows up in Phase 11.

We deliberately use a **single runner with a `GOOS`/`GOARCH` env-var
loop** instead of an Actions `strategy.matrix`:

- Cross-compile is a compile-only check (no tests run on the foreign
  arch — we cannot, without QEMU). One runner finishes all four targets
  in seconds; four runners would each spend ~30s on setup/checkout/cache.
- A `strategy.matrix` here would 4× the runner-minute cost for identical
  coverage, plus quadruple the entries in the required-check dropdown
  (four `build (linux-amd64)`-style names instead of one `build
  (cross-compile)`), making branch protection noisier.
- Future Phase 14 GoReleaser will replace this with a proper publishing
  matrix; until then, the env loop is sufficient for the "compiles on
  every supported target" gate the ROADMAP requires.

## Consequences

**Positive**

- Every `feat:` commit from Phase 2 onward is gated by the full four-job
  CI workflow before it can merge to `main`.
- Sibling-project parity: contributors moving between toykv / toymq /
  toyraft find the same workflow shape, the same job names, the same
  Conventional-Commits rules. The trilogy is navigable as one project.
- Cross-platform / cross-arch / lint-config issues surface at
  introduction, not at Phase 14 in a flood.
- `golangci-lint config verify` catches `.golangci.yml` typos now, on
  the empty tree, instead of at the worst possible time later.
- PROC-08 ("every commit Conventional-Commit-clean and CI-green")
  becomes structurally enforceable.

**Negative**

- A new phase (1.1) had to be inserted into the ROADMAP. We preserved
  the existing 1–14 numbering by using the `.1` decimal scheme rather
  than renumbering downstream phases.
- Phase 14 scope shrinks — the ROADMAP §Phase 14 entry must reflect
  that core CI is no longer landing there. This documentation update
  rides along with plan 04 of Phase 1.1.
- The workflow's first PR has nothing real to lint or test. Plan 04's
  draft PR includes a deliberately-malformed commit to prove the
  `commitlint` job actually rejects bad subjects (ROADMAP §SC11).

**Follow-ups**

- [ADR-0003] records the `.golangci.yml` linter set the `lint` job
  depends on (already landed in plan 01 of this phase).
- Phase 14 audits whether branch-protection settings still match this
  ADR after twelve phases of evolution, and lands the GoReleaser
  publish workflow on tag pushes.
- `.journal/M1.1.md` (plan 04) records the exact manual branch-protection
  toggles for replay if the repo ever has to be reconstructed.
- A future ADR may unify the trilogy on a single shared workflow
  template (vendored via reusable GitHub Actions); deferred until at
  least one sibling adopts ToyRaft's `commitlint` job.

## Usage

`.github/workflows/ci.yml` is the canonical reference; this ADR is the
*why*. When updating either:

- Renaming a job (`name:` field) requires updating the **Required-check
  names** list above and re-selecting in GitHub Settings → Branches in
  the **same** PR — otherwise branch protection silently stops
  enforcing the renamed check.
- Adding a job (e.g. a future `chaos-smoke` in Phase 11) requires a new
  required-check entry in this ADR (or a superseding ADR) plus a UI
  update.
- Removing a job requires a superseding ADR explaining why the gate is
  no longer needed.

See also `docs/PROCESS.md` for the branch-protection settings table
(landed in plan 03 of this phase) and `.planning/phases/01.1-ci-bootstrap-lint-test-race-commitlint-matrix-build/01.1-RESEARCH.md`
for the full pattern / pitfall analysis behind every choice above.

[toykv]: https://github.com/prajwalmahajan101/toykv
[toymq]: https://github.com/prajwalmahajan101/toymq
[ADR-0003]: ./0003-golangci-lint-config.md
