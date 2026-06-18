# ADR-0003 — golangci-lint configuration

**Status:** Accepted
**Date:** 2026-06-18
**Scope:** `.golangci.yml`

## Context

[golangci-lint] v2 ships with a default-enabled set (errcheck, govet,
ineffassign, staticcheck, unused). The v1→v2 transition (2024) changed
this set, restructured the YAML schema (`linters-settings` moved under
`linters.settings`, `issues.exclude-rules` moved under
`linters.exclusions.rules`), and folded the standalone `gosimple` linter
into `staticcheck`. We need an explicit linter list so future linter-set
changes are reviewed through ADRs, not silently adopted via a tool
upgrade.

Sibling projects [toymq] and [toykv] ship no `.golangci.yml` and rely on
the upstream defaults. ToyRaft diverges here because PROC-08 (every
commit Conventional-Commit-clean and CI-green) and QUAL-04 (documented
quality gates) demand a stable, ADR-tracked linter set. The trilogy may
later unify on a single config; that supersedes this ADR.

This ADR is paired with [ADR-0002] (bring CI forward to Phase 1.1), which
introduces the `lint` job that consumes this config.

## Decision

`.golangci.yml` uses the v2 schema (`version: "2"`) and explicitly
enables the following seven linters via `linters.default: none` +
`linters.enable`:

- `errcheck` — unchecked errors
- `govet` — `go vet`'s rule set
- `ineffassign` — assignments whose value is never read
- `staticcheck` — bug detection + style; **subsumes the old `gosimple`
  linter in v2**, so we do not (and cannot) enable `gosimple` separately
- `unused` — unused identifiers
- `misspell` — common English spelling mistakes (especially in doc comments)
- `revive` — naming + style, configured with rules `var-naming`,
  `error-return`, `unexported-return`, `blank-imports`

`gosec` is **deliberately excluded.** Its false-positive rate is too high
for a teaching project that uses `math/rand` with explicit seeds (for
reproducible chaos), unbuffered I/O patterns by design, and `os.Exit`
in the `toyraftd` CLI. Re-evaluating in Phase 14 is allowed (see
Follow-ups).

`_test.go` files are exempt from `errcheck` via
`linters.exclusions.rules`, because test helpers routinely ignore
`Close()` errors deliberately.

Lint timeout is set to 5 minutes via `run.timeout`.

## Consequences

**Positive**

- Linter set is stable across `golangci-lint` upgrades; the next minor
  release cannot silently widen or narrow what CI enforces.
- Future additions/removals are ADR-tracked rather than appearing as
  surprise PR failures.
- `misspell` catches doc typos cheaply; with Phase 1's twelve spec
  documents this pays for itself immediately.
- `revive` enforces Go naming conventions that downstream chaos tests
  rely on (exported-name discoverability).
- `linters.default: none` + explicit `enable` list means upgrading
  `golangci-lint` never silently turns linters on.

**Negative**

- ToyRaft diverges from toykv/toymq (which use defaults). The
  divergence is documented here; a future trilogy-wide ADR may unify.
- `gosimple` is no longer listed in the config because v2 absorbed it
  into `staticcheck`. Readers familiar with the v1 set may wonder where
  it went — this ADR is the canonical answer.
- `gosec` opt-out is a real trade: a future ToyRaft consumer running
  `gosec` independently may find issues CI never flagged. Acceptable
  because ToyRaft is library code, not a security-critical service.

**Follow-ups**

- Phase 14 audits whether to enable `gocritic`, `errorlint`, or
  `gocyclo` once the codebase is large enough for them to add signal.
  That audit lands as a new ADR superseding the relevant Decision lines
  here (do not amend in place).
- If `revive` rule churn becomes painful, pin a fixed `revive` config
  file checked into the repo and reference it from `.golangci.yml`.
- If trilogy-wide unification happens, that ADR supersedes this one.

## Usage

CI consumes this file via `golangci/golangci-lint-action@v6` plus an
explicit `golangci-lint config verify` step (defends against silent
config typos on the near-empty Phase 1.1 tree — see [Pitfall 6] in
01.1-RESEARCH.md). Local developers run `golangci-lint run` directly;
the `make hooks` install (landed in plan 02 of this phase) wires up a
`pre-commit` hook that runs `gofmt` + `go vet`, deferring the heavier
`golangci-lint run` to CI.

[golangci-lint]: https://golangci-lint.run
[toymq]: https://github.com/prajwalmahajan101/toymq
[toykv]: https://github.com/prajwalmahajan101/toykv
[ADR-0002]: ./0002-bring-ci-forward.md
[Pitfall 6]: ../../.planning/phases/01.1-ci-bootstrap-lint-test-race-commitlint-matrix-build/01.1-RESEARCH.md
