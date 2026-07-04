# 0018 — Chaos suite: PGID group kill, positive oracles, and the determinism boundary

**Status:** Accepted
**Date:** 2026-07-04
**Scope:** `test/chaos/` (`test/chaos/inproc/`, `test/chaos/processkill/`), `internal/raftest` (`HistoryEvent` recorder)

## Context

Phase 11 adds a seeded chaos suite in two halves (RESEARCH §Summary):

- an **in-process matrix** (`test/chaos/inproc`) sweeping all five failure knobs
  — drop / delay / reorder / duplicate / partition — over the deterministic
  inproc `Hub`, and
- a **process-kill harness** (`test/chaos/processkill`) that boots real
  `toyraftd` subprocesses, kills the leader, and asserts recovery.

Two design forces drove the load-bearing decisions this ADR ratifies:

1. **Killing a leader deterministically from a Go test, and proving no leak.**
   A `toyraftd` leader may fork helpers or leave a listener bound; killing only
   the immediate child (`cmd.Process.Kill()`) can orphan the group. Go's
   `syscall.Kill` with a negative PID is the POSIX group-signal primitive, but it
   sits next to the well-known `golang/go#53199` negative-PID coercion footgun, so
   the lifecycle (spawn + kill + leak gate) needs to be pinned explicitly.
   (CHAOS-02/03/05/06.)

2. **Oracles that assert real progress, not the absence of panics.** "No error
   was raised" is a vacuous chaos oracle — a cluster that silently stops
   committing raises nothing. The suite needs *positive* liveness assertions.
   (CHAOS-07.)

Relevant prior art: ADR-0007 (split-seed chaos determinism), ADR-0017
(synchronous delivery-drain seam — the harness change that made SC1 byte-
determinism reachable at all), and ADR-0002 (CI required-check names).

## Decision

1. **PGID subprocess lifecycle.** We spawn each `toyraftd` with
   `SysProcAttr{Setpgid: true}`, so the child's process-group id equals its pid,
   and we kill the leader via `syscall.Kill(-pgid, syscall.SIGKILL)` — the
   **negative pgid** signals the whole group, reaping any helpers the daemon
   spawned. We do **not** use `cmd.Process.Kill()` (which signals the immediate
   child only). The harness carries `//go:build linux || darwin`; `Setpgid` and
   negative-pgid signalling are POSIX, and both CI matrix OSes (ubuntu + macos)
   are POSIX.

2. **Leak gate = process ABSENCE, not port-free.** After a kill + `cmd.Wait`, we
   assert the child is gone via `ps -o pgid= -p <pid>` as the **primary, portable
   (Linux + macOS)** gate; `lsof -i :<port>` is a **secondary** best-effort port
   check (skipped when `lsof` is absent). Process-absence is the reliable signal
   because a killed listener's port can linger in `TIME_WAIT` — port-still-bound
   does not mean process-alive. Anchoring on process-absence also keeps
   correctness independent of the `go#53199` coercion edge. Leaks are reported via
   `t.Errorf` (never `t.Fatalf` inside cleanup); teardown order is
   kill-live-groups → `cmd.Wait` (reap) → leak-assert; data dirs are
   `t.TempDir()` (auto-removed).

3. **Positive oracles.** Chaos runs assert **≥N committed writes AND ≥M leader
   changes** (parameterised table fields), never merely "no errors raised". The
   committed-write oracle reads the *committed-index delta* — the inproc matrix
   reads `Cluster.CommitIndex`; the process-kill harness reads the `/status`
   `commit_index` seam on a **majority** of survivors (not read-back alone, since
   a 307-followed GET can be served from the leader's applied KVSM without a
   durable commit signal). The continuous invariants
   `AtMostOneLeaderPerTerm`, `LogMatching`, and `NoCommittedEntryLost` run every
   tick.

4. **Determinism boundary — inproc-only.** The inproc matrix is
   **byte-deterministic** from a single `-seed` flag (seeding the election RNG and
   all five Hub chaos sub-RNGs, ADR-0007), enforced by
   `TestSeededMatrix_SameSeedIdenticalTrace`. The process-kill harness uses real
   wall-clock (`clock.NewReal()`) and is **intentionally NOT** byte-deterministic:
   only positive/qualitative oracles apply there. SC1 determinism is an
   inproc-only property by design.

5. **Shared history recorder for Phase 12.** Both suites emit
   `[]raftest.HistoryEvent` (pinned 1:1 to `porcupine.Operation` v1.0.3) to
   `test/artifacts/chaos/<seed>/history.json`. Phase 12's Porcupine
   linearizability checker consumes exactly this shape. The `test/artifacts/`
   tree is gitignored (anchored at the module root via a `go.mod` walk, so the
   `go test` CWD=package-dir never escapes the rooted ignore entry).

6. **CI wiring stays inside the existing job.** The chaos suite runs as a
   terminal step in the existing `test` matrix job — no retry, no
   `continue-on-error` — so no new required-check name is introduced and ADR-0002
   / branch protection are unchanged.

## Consequences

**Positive**
- Leak-safe group kill: the whole daemon subtree dies, proven absent by `ps`.
- Cheap silent-divergence detection: the ≥N/≥M oracles catch a cluster that
  stops making progress, which "no panic" would miss.
- Phase 12 is unblocked — the `HistoryEvent` artifacts are its direct input.
- No branch-protection churn: the CI step reuses the `test` job's required check.

**Negative**
- The process-kill harness is POSIX-only (`linux || darwin`); a Windows CI cell
  could not run it.
- Process-kill histories are wall-clock and non-deterministic, so only
  positive/qualitative oracles apply there — no byte-for-byte replay.
- The `ps`/`lsof` leak gate depends on those tools existing on the runner —
  mitigated: both are present on the ubuntu + macos GitHub images, and `lsof`
  absence degrades to a skip rather than a failure.

**Follow-ups**
- Phase 12: the Porcupine linearizability consumer over the dumped histories.
- Phase 13: `iptables` / `netns` network-partition chaos (CHAOS-04, Linux-only).

## Usage

- Deterministic replay / regression triage: run the inproc matrix with a fixed
  `-seed`; the same seed reproduces a byte-identical `history.json`.
- Real crash chaos: run `test/chaos/processkill` on a POSIX host; treat any
  leaked process reported by the `ps` gate as a bug, not a flake.
- Phase 12 input: read `test/artifacts/chaos/<seed>/history.json` as
  `[]raftest.HistoryEvent` — do not change the shape without updating this ADR
  and the porcupine pin.
