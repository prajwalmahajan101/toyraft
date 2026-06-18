# 0000 — Record architecture decisions

**Status:** Accepted
**Date:** 2026-06-18
**Scope:** project-wide

## Context

ToyRaft inherits the toykv/toymq house style: every non-obvious decision
that constrains future work is recorded as an ADR. We need to formalise
the practice itself before any other ADR can cite it. Michael Nygård's
2011 essay defines the pattern we adopt, and the canonical form lives at
`docs/adr/TEMPLATE.md`.

## Decision

We will record architecturally significant decisions in
`docs/adr/NNNN-<slug>.md` using the template at `docs/adr/TEMPLATE.md`.

- NNNN is a zero-padded 4-digit serial, assigned at PR open time (not at
  draft time) to avoid collisions across parallel branches.
- Status transitions are append-only: an ADR is never edited after
  Accepted; it is superseded by a new ADR that references it.
- Every PR that depends on an ADR links it in the PR description; the
  PR template (`.github/pull_request_template.md`) enforces this.
- ADRs are drafted via the `architecture-decision-records` skill.

## Consequences

**Positive**
- Future contributors can reconstruct the "why" behind any decision.
- Forces explicit tradeoff documentation before merge.
- Aligns with toykv/toymq, simplifying trilogy navigation.

**Negative**
- Small per-decision overhead (~30 min to draft).

**Follow-ups**
- Phase 2+ produces at least one ADR per phase (PROC-06).
- 15 ADRs targeted for v1 (QUAL-04); seed list lives in
  `.planning/research/SUMMARY.md` §9.
- A `docs/adr/README.md` index will be added in Phase 2 alongside
  ADR-0001 (deferred per `01-RESEARCH.md` open question #1).
