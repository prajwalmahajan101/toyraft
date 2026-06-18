# ToyRaft Process

**Status:** Accepted
**Date:** 2026-06-18
**Scope:** project-wide working agreements + enforcement

This document codifies the six Working Agreements from
`.planning/PROJECT.md` and binds each to its enforcement mechanism
(skill, CI hook, PR template). When the rules in `PROJECT.md` and this
document disagree, this document — version-controlled in the repo — is
authoritative.

**Companion docs:**

- [`docs/CONTRIBUTING.md`](./CONTRIBUTING.md) owns the **mechanics**:
  build/test/lint commands, branch naming table, conventional-commit
  scope cheatsheet, PR checklist preview.
- This document owns the **semantics**: when to write an ADR vs. an
  RFC, when a journal entry is required, what "drift" means.

Cross-link, don't duplicate.

## Working Agreement 1 — One ADR per architecture decision

Every non-obvious design choice that constrains future work lands in
`docs/adr/NNNN-<slug>.md` **before** the PR that depends on it merges
(PROC-01).

- **Template:** `docs/adr/TEMPLATE.md` (Michael Nygård form — Context /
  Decision / Consequences with Status / Date / Scope header).
- **Status transitions** are append-only: an accepted ADR is never
  edited; it is **superseded** by a new ADR that references it.
- **Numbering:** `NNNN` is a zero-padded 4-digit serial, assigned at PR
  open time (not at draft time) to avoid collisions across parallel
  PRs. The meta-ADR `docs/adr/0000-record-architecture-decisions.md`
  documents this rule.
- **Skill binding:** drafted via the **`architecture-decision-records`**
  skill.
- **PR template enforcement:** every PR that depends on an ADR links
  it under "Linked specs / ADRs / RFCs"; reviewers reject unchecked
  boxes.
- **Phase requirement:** at least one ADR per implementation phase
  (Phases 2–14) — PROC-06. Verified at phase close by
  `ls docs/adr/` referencing the phase.

**What counts as "architecturally significant":**

- Locks a public interface shape or contract.
- Picks one of several viable implementation strategies (e.g.
  single-mutex vs lock-per-field).
- Introduces a dependency, a build-tag, or a process boundary.
- Defines an invariant that other code must preserve.

**What does not need an ADR** (judgement call — when in doubt, write
one): typo fixes, refactors that preserve behaviour, internal helper
extraction, test additions that don't change a contract.

## Working Agreement 2 — One journal entry per phase

Every completed phase commits an entry to `.journal/M{n}.md` derived
from `.journal/TEMPLATE.md` **before** the phase is marked complete
in `STATE.md` (PROC-02).

- **Template:** `.journal/TEMPLATE.md` (PROC-04).
- **Skill binding:** drafted via the **`code_assist:journal`** skill.
- **Contents:** Goals (from `ROADMAP.md`), What shipped (file paths),
  ADRs introduced, Decisions made (non-ADR), Risks retired, **What
  surprised us** (load-bearing creative section — matches toykv/toymq
  house style), Follow-ups, Seeds for next phase.
- **Commit format:** `docs(journal): add M{n} journal`.
- **Required by:** PROC-02; enforced by the PR template ("If this PR
  closes a phase: `.journal/M{n}.md` is included").

The journal is the load-bearing artefact for inter-phase context
handoff. A phase that ships without one is incomplete by definition.

## Working Agreement 3 — One RFC per substantive proposal

Any change that touches **public API shape**, alters a **Phase-1
contract**, **expands v1 scope**, or **promotes a v2 requirement to
v1** starts as an RFC in `docs/rfc/NNNN-<slug>.md` and is discussed
**before** becoming an ADR (PROC-03).

- **Template:** `docs/rfc/TEMPLATE.md`.
- **Lifecycle:** `Draft → Under Review → Accepted | Rejected`. An
  Accepted RFC is promoted to an ADR (the RFC stays as the record of
  the discussion; the ADR is the record of the decision).
- **Numbering:** sequential, separate namespace from ADRs
  (`docs/rfc/0001-…`, `0002-…`, …). `0001` is the seed RFC locking v1
  scope.

**"Substantive" decision rule.** A proposal is substantive if **any**
of these is true:

- It changes a public symbol's signature, name, or type.
- It changes the on-wire JSON schema for `Message` or any HTTP
  endpoint contract documented in `docs/WIRE.md`.
- It alters an invariant documented in `docs/CONCURRENCY.md`,
  `docs/LLD.md`, or `docs/WIRE.md`.
- It pulls a `SNAP-*`, `MEMB-*`, `PERF-*`, or `PROD-*` v2 requirement
  into v1 scope.
- It modifies the Out-of-Scope table in `.planning/REQUIREMENTS.md` or
  `.planning/PROJECT.md`.

Anything else (internal refactor, perf improvement that preserves
contracts, new test, doc fix) skips RFC and goes straight to ADR (if
architectural) or just a PR (if not).

**RFC vs ADR — short version:**

- **RFC** = proposal under discussion. May be rejected.
- **ADR** = decision made. Append-only history of what we chose and
  why.

## Working Agreement 4 — Specs are source of truth

When code diverges from `docs/PRD.md`, `docs/HLD.md`, `docs/LLD.md`,
`docs/WIRE.md`, `docs/CONCURRENCY.md`, `docs/TESTING.md`, or
`docs/SECURITY.md`, exactly one of two things must happen in the same
PR:

1. **The code is wrong** — fix the code to match the spec.
2. **The spec is stale** — write an ADR (Working Agreement 1)
   justifying the change, then update the spec.

Drift without an ADR is a **review-blocking finding**. This is the
single most common failure mode for a documentation-heavy project, and
the PR checklist has a dedicated line for it:

> [ ] No spec/code drift — if behaviour diverges from `docs/`, either
> an ADR justifies it or the spec is updated in this PR.

Reviewers run a `git diff main -- docs/` mental check for any
non-trivial PR. If `docs/` is unchanged and the PR touches a contract,
that's the prompt.

## Working Agreement 5 — Conventional commits

All commits follow [Conventional Commits 1.0.0][cc] (PROC-08). The
full scope cheatsheet lives in
[`docs/CONTRIBUTING.md`](./CONTRIBUTING.md#conventional-commits) — this
document captures the rule, not the table.

[cc]: https://www.conventionalcommits.org/en/v1.0.0/

- Subject line ≤72 chars, no trailing period, imperative mood.
- Type prefix mandatory: `feat | fix | refactor | docs | test | chore`.
- Scope optional but encouraged.
- Body and footer optional; use `BREAKING CHANGE:` footer for breaking
  changes (will trigger major-bump at release time).

**Enforcement:**

- **CI:** `commitlint` runs on every PR; failures block merge.
- **Local:** `make hooks` installs a `commit-msg` git hook that runs
  the same `commitlint` config locally so failures are caught before
  push (QUAL-10).

## Working Agreement 6 — One branch per phase

Every phase opens its own branch off `main` named
`feature/<descriptive-slug>` — **never** with a phase number in the
branch name (PROC-07). Phase merges back via PR after the verifier
passes and the journal entry + any required ADR(s) land. No direct
commits to `main`.

The locked branch-name table is in
[`docs/CONTRIBUTING.md`](./CONTRIBUTING.md#branch-model). This document
holds the rule, not the table.

## Enforcement

The agreements above are enforced by three mechanisms — none of which
is "we'll remember":

| Mechanism | What it enforces | Where it lives |
| --------- | ---------------- | -------------- |
| **PR template** | ADR/RFC linkage, journal-per-phase, no spec drift, branch naming, conventional-commit subject | `.github/pull_request_template.md` (DOC-15 / PROC-05) |
| **commitlint CI** | Conventional Commits subject format | `.github/workflows/ci.yml` + `commitlint.config.js` (PROC-08, Phase 1.1) |
| **`make hooks`** | Pre-commit `gofmt + go vet`; commit-msg Conventional Commits regex | `Makefile` `hooks` target + `.githooks/` (QUAL-10, Phase 1.1) |

The PR template ships in Phase 1 (DOC-15 / PROC-05). The CI workflow,
commitlint config, and local `make hooks` installer all ship in
**Phase 1.1** (CI bootstrap) — see [ADR-0002](./adr/0002-bring-ci-forward.md)
for the rationale on bringing CI forward from Phase 14, and that ADR for
the canonical list of required-check job names enforced on `main`.

## Skill Bindings (summary)

| Workflow | Skill |
| -------- | ----- |
| Drafting / maintaining ADRs | `architecture-decision-records` |
| Drafting phase journal entries | `code_assist:journal` |

Both are global skills available to any agent contributing to the
project. They encode the templates above and the project's house
style so the output is consistent across contributors.

## Cross-References

- **PROJECT-level vision:** `.planning/PROJECT.md` §Working Agreements.
- **Mechanics:** `docs/CONTRIBUTING.md`.
- **ADR template:** `docs/adr/TEMPLATE.md`.
- **RFC template:** `docs/rfc/TEMPLATE.md`.
- **Journal template:** `.journal/TEMPLATE.md`.
- **PR template:** `.github/pull_request_template.md`.
- **Requirements traceability:** `.planning/REQUIREMENTS.md` §Process
  Rules (PROC-01..PROC-08).
