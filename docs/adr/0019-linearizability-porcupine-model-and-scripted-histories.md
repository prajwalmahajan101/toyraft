# 0019 — Linearizability verification: Porcupine KV-register model and scripted histories

**Status:** Accepted
**Date:** 2026-07-04
**Scope:** `test/linearizability/`

## Context

Phase 12 is ToyRaft's v1 correctness gate: replay operation histories through
`github.com/anishathalye/porcupine` against a model of the reference KV state
machine and assert linearizability, including encodings of the Raft paper's
Figure 7 (log matching under leader change) and Figure 8 (the current-term
commit rule). See LIN-01..LIN-05 for the phase requirements.

Two forces shape the decision:

1. **The Phase-11 recorded histories are trivially linearizable.** Both chaos
   suites dump `[]raftest.HistoryEvent` to `test/artifacts/chaos/<seed>/history.json`
   (ADR-0018 §5, shared recorder), but those dumps are single-client
   (`ClientID=0`) and non-overlapping. Porcupine returns `Ok` on such a history
   even if Raft were broken — a checker pointed at them would ship a gate that
   stays green regardless of correctness (RESEARCH Pitfall 1; the vacuity is
   demonstrable per matrix_test.go:200-206). A non-trivial gate needs
   concurrent, multi-client, overlapping histories.

2. **The model must faithfully mirror the reference state machine.** The oracle
   is only as good as its model: it must reproduce `pkg/kvsm` semantics exactly,
   or it will either reject legal histories or rubber-stamp illegal ones. The
   Figure-8 encoding in particular must hinge on ADR-0010 (the current-term
   commit rule — the safety property that keeps a committed value visible across
   leader churn).

Relevant prior art: ADR-0010 (current-term commit / Figure-8 safety), ADR-0018
(shared `HistoryEvent` recorder + the `[]raftest.HistoryEvent` ⇄
`porcupine.Operation` seam), and the v1.0.3 pin already carried in
`internal/raftest/history.go`.

## Decision

1. **Test dependency and version pin.** We use
   `github.com/anishathalye/porcupine` v1.0.3 (pinned) as the linearizability
   checker and HTML visualizer. It does **not** leak into `./pkg/...` — only
   `internal/raftest` and `test/linearizability` import it
   (`go list -deps ./pkg/... | grep porcupine` is clean). We do **not** upgrade
   to v1.2.0: its non-test API is identical for our use and the
   `raftest.HistoryEvent` shape is pinned 1:1 to v1.0.3's `Operation`.

2. **The KV-register model mirrors `pkg/kvsm` exactly.** The
   `porcupine.Model` `Step` reproduces kvsm semantics inline: `set` is an
   unconditional write (state becomes the value), `del` collapses to the empty
   state (absent), and `get` is legal iff the observed value equals the current
   state. `Init` returns `""`, so `del` and never-set collapse to the same state
   (mirroring `kvsm.Apply`'s `delete(map,key)` and `Get`'s `(nil,false)` for
   both). We **forbid empty-value sets** so per-key state is a plain `string`
   (`""` == absent); scenarios must never `set` an empty value. We **partition
   by key** so an N-key history becomes N independent per-key checks. `model.go`
   imports only `fmt` + `porcupine` — kvsm is a semantic reference, not a runtime
   call. We use `CheckOperationsVerbose` (not the bool `CheckOperations`) so a
   violation yields the `LinearizationInfo` that `VisualizePath` needs.

3. **The corpus is scripted concurrent histories — Option A, not Option B.**
   The linearizability corpus is **hand-authored** concurrent, multi-client
   read+write histories (steady-state, leader-churn, packet-loss, Figure 7,
   Figure 8), each with ≥2 distinct `ClientId`s and ≥1 overlapping operation on a
   shared key — mirroring porcupine's own `TestRegisterModel`. We do **not**
   re-plumb the chaos harness for deterministic multi-client recording
   (Option B): the existing single-client dumps are trivially linearizable, and
   scripting keeps Phase-11's byte-determinism story intact while making SC2/SC3
   fast and deterministic. Recording-based multi-client concurrency is
   **deferred**. Since Porcupine has no notion of leaders, "leader churn" is
   modeled purely as later operations issued by distinct `ClientId`s.

4. **Fault injection is safe by construction.** SC4 asserts that a **crafted**
   non-linearizable Figure-8 history (a committed write of "c8" followed by a
   strictly-later read observing the pre-write absent value — a lost committed
   write) returns `porcupine.Illegal`, and that `VisualizePath` writes
   `test/artifacts/linearizability/<seed>.html`. The test passes **because** the
   history is illegal (mirroring porcupine's own `visualization_test.go`); no bug
   is introduced into `pkg/raft`. Artifacts land under the gitignored
   `test/artifacts/` and are never committed.

5. **CI wiring introduces no new required check.** The linearizability check
   runs as an explicit terminal step in the existing `test` matrix job — no
   retry, no `continue-on-error`, no conditional — so no new required-check name
   is introduced and ADR-0002 + branch protection are unchanged. The general
   `go test -race` step excludes `/test/(chaos|linearizability)/` so the suite
   runs once per matrix cell.

## Consequences

**Positive**
- A genuinely diagnostic correctness gate: the model is proven non-vacuous
  (it rejects a crafted illegal history), so a broken Raft would fail it.
- Deterministic and fast: scripted histories are tens of ops, so the run is
  byte-deterministic and never times out (`Unknown` is treated as failure).
- Visualization on violation: `CheckOperationsVerbose` + `VisualizePath` emit an
  HTML diagram of any counterexample.
- No branch-protection churn: reusing the `test` job keeps ADR-0002's enumerated
  required-check names stable.
- v1 acceptance: ToyRaft credibly implements Raft — Figure 7/8 histories pass
  and the current-term commit rule (ADR-0010) is exercised end-to-end.

**Negative**
- Scripted histories exercise the **model** and the client-visible contract, not
  the live Raft engine per run; recording-based concurrency is deferred.
- The Figure 7/8 register encodings are a modeling choice (client-visible
  register histories), not a mechanical replay of engine logs.

**Follow-ups**
- Optional real-dump smoke consumer of `test/artifacts/chaos/<seed>/history.json`
  via `adapter.go`'s `FromHistory` (kept as the normalization path).
- Phase 13 network-partition chaos (`iptables` / `netns`) can feed richer
  recorded histories once Option B is revisited.
