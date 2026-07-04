# Architecture Decision Records — Index

This is the one-line index of every ADR in `docs/adr/`. Bodies live in the
numbered files; this index exists per the project convention (and for trilogy
parity with toykv's `docs/adr` index). Each entry links the file and gives a
one-line summary. Order is by number.

> **On the 0001 gap:** ADR numbering skips from **0000 → 0002**. ADR-0001 is
> **intentionally unused / reserved** — the number was never assigned to a
> decision. Existing ADRs are **NOT renumbered** to fill the gap, because their
> numbers are referenced from journals (`.journal/M*.md`), other ADRs, and
> commit messages; renumbering would break those cross-references. Treat 0001 as
> a permanent hole in the sequence.

## Index

- [ADR-0000](0000-record-architecture-decisions.md) — Record architecture decisions (the meta-ADR establishing the Michael Nygård practice).
- **ADR-0001 — intentionally unused / reserved** (numbering skipped 0000 → 0002; existing ADRs are NOT renumbered to preserve cross-references).
- [ADR-0002](0002-bring-ci-forward.md) — Bring CI forward to Phase 1.1 (lint + race-test + commitlint + build gates before consensus code).
- [ADR-0003](0003-golangci-lint-config.md) — golangci-lint configuration (linter set + config schema).
- [ADR-0004](0004-single-mutex-state-machine.md) — A single mutex guards the Raft state machine.
- [ADR-0005](0005-storage-interface-freeze.md) — Storage interface freeze (Phase 3): interface home, Snapshot/Restore placement, sentinel cleanup.
- [ADR-0006](0006-fakeclock-determinism-model.md) — Deterministic FakeClock model (panic-on-stuck synchronous-fire).
- [ADR-0007](0007-inproc-hub-chaos-seed-splitting.md) — inproc Hub chaos: seed-splitting and single dispatcher.
- [ADR-0008](0008-step-event-loop-and-ready.md) — `step()` event loop and `Ready()` drain pattern.
- [ADR-0009](0009-per-node-rng-mixing.md) — Per-node RNG mixing for election timeouts.
- [ADR-0010](0010-current-term-commit-rule.md) — Current-term commit rule (Raft Figure 8 fix).
- [ADR-0011](0011-log-storage-mirror.md) — Mirror the replicated log into the local Storage subset.
- [ADR-0012](0012-storage-interface-reconciliation.md) — Reconcile the local `pkg/raft.Storage` subset against `pkg/storage.Storage`.
- [ADR-0013](0013-storage-file-on-disk-format.md) — `storage/file` on-disk format + in-process fault seam.
- [ADR-0014](0014-inproc-transport-reconciliation.md) — inproc transport reconciliation (additive `Hub.Transport` wrapper).
- [ADR-0015](0015-http-transport-config-address-book.md) — HTTP transport owns its peer-URL address book.
- [ADR-0016](0016-reference-demo-daemon-architecture.md) — Reference demo daemon: two-listener model + non-blocking peer sends.
- [ADR-0017](0017-synchronous-delivery-drain-seam.md) — Synchronous delivery-drain seam for byte-deterministic chaos traces.
- [ADR-0018](0018-chaos-suite-pgid-kill-and-oracles.md) — Chaos suite: PGID group kill, positive oracles, and the determinism boundary.
- [ADR-0019](0019-linearizability-porcupine-model-and-scripted-histories.md) — Linearizability verification: Porcupine KV-register model and scripted histories.
- [ADR-0020](0020-netns-iptables-chaos-and-dedicated-ci-job.md) — netns/iptables partition chaos and a dedicated CI job.
- [ADR-0021](0021-observability-published-daemon-side.md) — Observability published daemon-side: expvar `raft.*` counters + un-gated `/status` + structured RPC/role logs.
- [ADR-0022](0022-goreleaser-dual-binary-release.md) — GoReleaser release build shipping `toyraftd` + `toyraftctl` across `{linux, macOS} × {amd64, arm64}`; library ships via plain semver tags.
