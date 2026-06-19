# RFC 0002 — Pull Log fuzz coverage forward to Phase 2

**Status:** Proposed
**Author:** project owner
**Date:** 2026-06-20
**Tracking issue:** N/A (phase-2 internal RFC; tracked via
`.planning/phases/02-foundations/02-06-PLAN.md`)

## Summary

This RFC pulls fuzz-test coverage for the in-memory `pkg/raft.Log`
surface forward from Phase 11 (chaos suite) and Phase 12
(linearizability) to Phase 2 (foundations). The fuzz layer is bounded
strictly to the six FOUND-02 methods on `*Log` — `Append`,
`TruncateSuffix`, `Term`, `Match`, `LastIndex`, `LastTerm` — and
contains no Storage, Transport, Node, or multi-node interaction. The
phases that own broader fuzzing concerns (Phase 9 `FuzzMessageParse`,
Phase 11 chaos suite, Phase 12 linearizability) keep their charters
unchanged; this RFC only pulls forward the slice of fuzz coverage that
becomes cheap and high-leverage the moment the Log surface lands in
plan 02-03.

## Motivation

The `pkg/raft.Log` surface is load-bearing for every subsequent phase.
Phase 5 (elections) reads `LastIndex`/`LastTerm` for the up-to-date
check; Phase 6 (replication) calls `Match`/`Term`/`Append`/
`TruncateSuffix` on every AppendEntries receive path. A latent bug in
any of the six methods does not surface until a multi-node chaos run
discovers it — at which point triage is dramatically more expensive
because the failure is intertwined with timing, partitions, and
state-machine application.

Table-driven negative tests (the four §4 invariant violations exercised
in `log_invariants_test.go` under `-tags raftdebug`) cover the failure
cases a human test-author already imagined. They do not, and cannot,
enumerate the long tail of `(Term, Index)` sequences a fuzzer reaches
in a few seconds.

**Concrete cases the table tests miss but fuzzing reaches:**

- Truncate-then-append boundary conditions where the truncation point
  is exactly `LastIndex()+1` (idempotent no-op) versus exactly
  `LastIndex()` (single-entry truncate) versus `0` (full wipe).
- `Match(idx, term)` lookups against indices that were just truncated
  and re-appended with a different term — the per-entry term-compare
  must be the new term, not the old.
- `Term(0)` sentinel consistency under arbitrary append patterns
  (FOUND-05 zero-value safety).

**Sibling-project parity.** `toykv` ships fuzz tests on its
key-encoding layer at the equivalent maturity point in its phase plan,
and the lesson recorded in its journal is unambiguous: every Log-level
fuzz bug surfaced cheaply at the unit layer would have been an hour of
chaos-suite triage if deferred.

**Cost is small.** The Log surface is pure: no goroutines, no I/O, no
clocks. Fuzz throughput is therefore high, corpus stays tiny (each
crafted seed is <100 bytes), and CI runtime grows by 30 seconds on
pull-request runs only.

## Detailed design

### Surface (in scope)

The fuzz layer targets exactly these methods on `*pkg/raft.Log`:

- `Append(entries ...Entry)`
- `TruncateSuffix(from Index)`
- `Term(idx Index) (Term, error)`
- `Match(prevIdx Index, prevTerm Term) bool`
- `LastIndex() Index`
- `LastTerm() Term`

No other type is constructed or invoked inside the fuzz harness.
`Storage`, `Transport`, `Node`, and the FakeClock are deliberately
unreachable; the fuzz `_test.go` file lives in `package raft` and
imports nothing from any sibling package.

### Surface (out of scope)

The following remain in their original phases and are NOT pulled
forward by this RFC:

- **Phase 9 `FuzzMessageParse`** — fuzzing of the wire-format JSON
  envelope for `Message`. Ships in Phase 9 because it requires the
  Transport surface to exist first.
- **Phase 11 chaos suite** — multi-node fuzzing over message histories
  driven by `Hub` knobs (reorder, duplicate, drop, delay, partition).
  Ships in Phase 11 because it depends on the full Node + Transport
  stack.
- **Phase 12 linearizability** — Porcupine-driven history fuzzing for
  the linearizability acceptance gate. Ships in Phase 12 because it
  depends on the KV reference state machine and the linearizability
  harness.

This boundary is enforced by the location of the fuzz file
(`pkg/raft/log_fuzz_test.go`, `package raft`) and by the absence of
any cross-package imports.

### Fuzz targets

Two top-level `Fuzz*` functions ship in this RFC:

1. **`FuzzLogAppendTruncate`** — interprets the fuzzer's `[]byte` as a
   script of operations (`op := b % 2`). `op == 0` appends one entry
   with a monotonically derived `(Term, Index)`; `op == 1` truncates
   the suffix from a random in-range index. After the script runs, the
   harness asserts `LastIndex()` equals the running expected last
   index and `LastTerm()` equals the running expected last term. The
   pre-log sentinel (`Match(0, 0)` and `Term(0)`) is re-asserted on
   every run.

2. **`FuzzLogMatch`** — builds a random log respecting the §4
   monotonic-term invariant, then for every in-range `idx` asserts
   `Match(idx, Term(idx)) == true` and `Match(idx, wrongTerm) ==
   false`. Out-of-range and sentinel cases are asserted explicitly.

Both functions are property-checking on **valid** input sequences. The
existing `log_invariants_test.go` continues to own §4 invariant
violation negative tests under `-tags raftdebug`.

### Corpus seeds

Hand-crafted seeds under `pkg/raft/testdata/fuzz/Fuzz*/` cover:

- the empty log (zero-value safety, FOUND-05)
- a single appended entry
- a multi-entry batch
- truncate-then-append

Each seed file uses Go's `go test fuzz v1` format. Crucially, every
seed under `testdata/fuzz/` runs as a regular table-test subtest under
default `go test ./pkg/raft` (no `-fuzz` flag required). This means the
seeds add coverage even on CI runs that skip the `-fuzz=...` step
(currently push-to-`main`).

### CI gating policy

The fuzz step runs only on `pull_request` events, via
`if: github.event_name == 'pull_request'` on the new CI step. Pushes
to `main` are exempt to keep main-branch CI fast (no fuzz wall-clock
on the post-merge path). The step lives **inside the existing `test`
job**, so the required-status-check contexts set live via `gh api PUT`
in Phase 1.1 (`lint`, four `test (...)` matrix entries, `commitlint`,
`build (cross-compile)`) remain unchanged. The fuzz step's exit code
becomes part of the `test` job's overall pass/fail.

`-fuzztime=30s` is the per-target CI budget; longer fuzz runs are
left to local invocation by maintainers.

### Crash-handling convention

If Go's fuzzer discovers a crashing input during a PR run, it
automatically writes the input to `pkg/raft/testdata/fuzz/<FuzzName>/`.
The convention — borrowed from how Phase 11/12 will treat their own
corpora — is: **the crashing seed MUST be committed in the same PR as
a regression artefact**, along with the production-code fix. This
matches the `test/chaos/seeds/` policy in `docs/TESTING.md` §4.

## Drawbacks

- **CI runtime grows by ~30 s per PR.** Acceptable: PR throughput in a
  solo project is low, and the 30-second budget is bounded.
- **Corpus files clutter the repo.** Acceptable: each is <100 bytes;
  the directory is a few KB total at any reasonable point in v1.
- **Scope is pulled out of Phase 11/12.** Only the Log-surface slice
  is pulled; chaos proper and linearizability proper remain in their
  phases. The RFC makes this distinction explicit so a future reader
  doesn't infer that Phase 11/12 lost their fuzz mandate.
- **Maintenance.** Any change to the six in-scope `Log` methods must
  re-evaluate whether the harness still exercises the new contract.

## Rationale and alternatives

**Why now (Phase 2) and not Phase 11?** Bugs are exponentially cheaper
to triage closer to the surface they live on. A latent `Log.Match` bug
that surfaces in a Phase-11 chaos run intermixes with timing,
partitions, and FSM application — pulling it apart can take hours.
The same bug under a 30-second fuzz run in Phase 2 prints a
counterexample. The mutation-test sanity check in plan 02-06's task 2
confirms this empirically.

**Alternative: defer fuzzing entirely to Phase 11.** Rejected. The
cost saving is illusory — bugs found cheaply now are not free later;
they are several orders of magnitude more expensive.

**Alternative: fuzz a wider surface (e.g., `types.go` Message
encoding) at Phase 2.** Rejected. Message encoding is not exercised
yet at Phase 2 (no Transport), and Phase 9 already owns
`FuzzMessageParse` per ROADMAP SC5. Pulling that forward would
genuinely widen scope.

**Alternative: skip CI gating, run fuzz locally only.** Rejected. A
fuzz target without CI enforcement is exposure theatre — the first
contributor who skips the local run breaks the chain.

**Alternative: run fuzz on every push (including main).** Rejected.
Adds 30 s to every post-merge CI run with zero correctness benefit
(the gate already fired on the PR). The `if:` gate is precise.

## Prior art

- **Go fuzzing built-in.** `go test -fuzz=` shipped in Go 1.18; the
  ecosystem convention is exactly the file-layout / seed format used
  here.
- **`toykv` precedent.** `toykv` pulled fuzz coverage onto its
  key-encoding layer at the foundations-equivalent phase, with the
  same justification: catch invariant violations on a pure data
  structure before they propagate into multi-component testing.
- **etcd `raft` package.** Upstream etcd ships fuzz targets against
  its log package (`raft/log_unstable.go`) — the same layering
  decision, validated at scale.

## Unresolved questions

None at this RFC's scope. Open questions that belong to other phases:

- Whether the Phase 11 chaos suite will reuse this RFC's corpus as a
  starting set (deferred to Phase 11 RFC).
- Whether nightly fuzz with a larger `-fuzztime` budget should be
  added (deferred to Phase 11; the 30-second PR budget is sufficient
  for v1 release readiness).

## Future possibilities

- **Extend fuzz coverage to Storage (in-memory)** once Phase 3's
  `Storage` interface stabilises — a separate RFC if proposed.
- **Reuse the corpus directory layout** for Phase 9
  `FuzzMessageParse` and Phase 11 chaos seeds. The
  `testdata/fuzz/<FuzzName>/seed*` convention is the established Go
  pattern.
- **Surface crashing seeds in PR review** via the CI artefact upload
  policy established in `docs/TESTING.md` §4 (chaos seeds).

## References

- `.planning/phases/02-foundations/02-03-PLAN.md` — defines the Log
  surface fuzzed by this RFC.
- `.planning/phases/02-foundations/02-06-PLAN.md` — execution plan
  this RFC accompanies.
- `docs/TESTING.md` §6 (Fuzz targets) and the new §Fuzz layer
  subsection — implementation of this policy.
- `docs/LLD.md` §2.1 — Log invariants.
- `docs/rfc/0001-v1-scope-and-non-goals.md` — v1 scope lock; this RFC
  does not widen v1 scope.
- ROADMAP Phase 9 SC5 (`FuzzMessageParse`), Phase 11 (chaos suite),
  Phase 12 (linearizability) — the phases whose charters remain
  unchanged.
