# 0012 — Reconcile the local pkg/raft.Storage subset against pkg/storage.Storage

**Status:** Accepted
**Date:** 2026-06-29
**Scope:** `pkg/raft` (`config.go` local `Storage` interface), `pkg/storage`

## Context

Plan 05-01 introduced a **local** `Storage` interface declared inside
`pkg/raft` (`config.go`) — the minimal `LogStorage` + `StateStorage` subset
the internal `*node` actually consumes — to defeat the `pkg/raft <->
pkg/storage` import cycle. `pkg/storage` already imports `pkg/raft` for the
shared `Entry` / `HardState` / `Index` / `Term` types (ADR-0005 freeze), so
`pkg/raft` **cannot** import the canonical `pkg/storage.Storage` directly.
`pkg/storage.Storage` structurally satisfies the local interface, so
consumer code is unchanged and passes any concrete backend (memory, file)
without a cast.

This structural duplication has been a **known, tracked divergence** since
Phase 5 (LLD §3 "Phase 7 will reconcile this via an ADR"; `.journal/M5.md`).
Phase 6 (ADR-0011) made the local subset carry the **replicated log** —
not just HardState — in lockstep with `n.log`, which raised the stakes:
the log now writes *through* the local interface, so the duplication is no
longer cosmetic. ADR-0011 explicitly **deferred** the reconciliation to
this Phase-7 ADR; STATE.md decisions 05-06 / 06-04 / 06-06 carry the same
flag forward. Phase 7 must resolve it because the public `raft.New`
constructor exposes `Config.Storage Storage` to consumers, and the
Phase-4 recorder-shim `Propose` path (retired in Phase 7 in favour of the
real `ProposeToLeader`) depended on the same seam.

RESEARCH.md (R-4) framed two options:

- **(a)** Move the shared Raft types (`Entry` / `HardState` / `Index` /
  `Term`) to a leaf package that both `pkg/raft` and `pkg/storage` import,
  then have both reference one canonical `Storage` interface. The "correct
  long game" — eliminates the duplication outright.
- **(b)** Declare the local subset interface as the **canonical Raft-side
  view** and document the `pkg/storage.Storage` structural duplication
  explicitly, rather than moving types.

References: RESEARCH R-4, ADR-0005 (Storage interface freeze), ADR-0011
(log-storage mirror), LLD §3 (local-subset note + Phase-7-reconciliation
flag), STATE.md decisions 05-06 / 06-04 / 06-06, `.journal/M5.md`.

## Decision

We adopt **option (b)**: the local `pkg/raft.Storage` subset is the
**canonical Raft-side view** of storage, and the `pkg/storage.Storage`
structural duplication is a **ratified, documented v1 decision** — not a
deferred TODO.

Concretely:

1. `Config.Storage` references the **local** `pkg/raft.Storage` interface.
   This is invisible to consumers: structural typing lets any
   `pkg/storage` backend satisfy it, which is exactly what lets the Phase-7
   `New` constructor accept any backend without a cast.
2. The two interfaces MUST stay signature-compatible. The LLD-drift gate
   (`scripts/check-lld-drift`) flags any divergence between the documented
   surface and the code, so the duplication **cannot silently rot**.
3. We do **not** move `Entry` / `HardState` / `Index` / `Term` to a leaf
   package in v1. Option (a) is the better long-term shape but is a large
   refactor spanning `pkg/storage`, `pkg/storage/memory`,
   `pkg/storage/storagetest`, `pkg/transport/inproc`, and
   `internal/raftest`; it risks the phase for **no v1 functional gain**.

## Consequences

**Positive**

- **Zero type-move churn this phase.** The smallest possible blast radius:
  no edits to `pkg/storage` and its dependents, so Phase 7 stays focused on
  the public API surface (Node, driver, apply channel).
- **The duplication is now decided in writing**, closing the Phase-5/6
  carry-forward flag. Reviewers reading `config.go` see a ratified choice,
  not an open question.
- **Drift cannot accumulate.** `scripts/check-lld-drift` is the standing
  guard: any signature divergence between the local subset and
  `pkg/storage.Storage` surfaces as a failing gate.
- The public constructor accepts any `pkg/storage` backend without a cast,
  thanks to structural typing against the canonical local view.

**Negative**

- The `pkg/raft.Storage` vs `pkg/storage.Storage` duplication **persists**
  into v1: two declarations of the same conceptual interface, kept in sync
  by convention + the drift gate rather than by the compiler.
- A reader must understand the import-cycle rationale (ADR-0005) to see why
  the duplication exists — extra cognitive load versus a single canonical
  interface.

**Follow-ups**

- The **v2 revisit door stays open**: option (a) (leaf package for the
  shared Raft types) is the intended long-term resolution and should be
  reconsidered if/when the Storage interfaces grow or a third importer
  appears.
- Supersedes the Phase-5/6 deferral: ADR-0011's "DEFER ... to Phase 7's
  ADR" is now discharged by this document.
