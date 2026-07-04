# 0020 — netns/iptables partition chaos and a dedicated CI job

**Status:** Accepted
**Date:** 2026-07-04
**Scope:** `test/chaos/netns/`, `.github/workflows/ci.yml`

## Context

Phase 13 retires risk **D-1 (loopback masking)**. The Phase-11 chaos halves —
the in-process matrix and the process-kill harness — both run their clusters
over `127.0.0.1`, where the kernel's loopback path cannot exhibit a *real*
network partition: a `DROP` on loopback is not the same failure mode as a peer
that becomes unreachable across an L3 hop (a real dial times out, a real read
blocks until the transport `SendTimeout`, and reconnection must re-establish a
TCP session). CHAOS-04 asks for a genuine partition test.

Two prior ADRs frame the decisions here:

- **ADR-0018** (chaos suite: PGID group kill, positive oracles, determinism
  boundary) — its explicit forward-reference names this phase ("Phase 13:
  `iptables` / `netns` network-partition chaos (CHAOS-04, Linux-only)"). This
  ADR reuses ADR-0018's PGID discipline, its positive-oracle rule (assert
  progress, never "no panic"), and its process-absence leak-gate pattern, now
  re-expressed for namespaces.
- **ADR-0002** (bring CI forward) — its **Required-check names** list is the
  authoritative enumeration of CI checks, and its *Usage* section states that
  **adding a job requires a new required-check entry in the ADR (or a superseding
  ADR) plus a branch-protection update**. Phase 13 adds a job, so it pays that
  cost in full.

The netns chaos harness (`test/chaos/netns/harness.go`) and its scenario
(`test/chaos/netns/netns_test.go`) landed in plans 13-01/13-02; this ADR
ratifies the load-bearing decisions those files encode and the CI wiring that
plan 13-03 adds.

## Decision

1. **Subprocess `ip netns exec` over in-process `setns`.** We drive network
   namespaces from `ip netns exec` **subprocesses**, never in-process
   `setns`/`CLONE_NEWNET` from the Go test runtime. A network namespace is a
   per-OS-thread attribute, and the Go scheduler moves goroutines across OS
   threads freely, so an in-process namespace switch leaks namespace state onto
   the thread pool (the goroutine↔thread↔namespace footgun). `ip netns exec
   <ns> toyraftd …` lands the daemon in its namespace at exec time; the test
   process itself never switches. Every topology mutation is an `ip`/`iptables`
   argv through a checked `run()` (fatal) / `runQuiet()` (best-effort) helper.
   Zero new Go dependencies (no `vishvananda/netlink`).

2. **N=3 separate-namespace + veth + bridge topology, partitioned by per-ns
   `iptables -j DROP`.** We run **N=3** nodes, each in its **own** network
   namespace with its **own** veth pair into a shared root-namespace Linux
   bridge and a real non-loopback `10.<octet>.0.1{0,1,2}/24` IP — a genuine L3
   hop between nodes. We partition exactly two nodes by installing
   per-namespace `iptables -A INPUT/OUTPUT -s/-d <peerip> -j DROP` rules
   **inside the two victim namespaces** (never a rule in the root/bridge
   namespace), cutting a single link while the third node bridges a surviving
   quorum. Namespace, bridge, veth, and subnet-octet names are randomized
   per-run from the pid for idempotency against a crashed prior run.

3. **`ip netns del` auto-teardown + namespace-absence leak gate (SC1).** `ip
   netns del <ns>` destroys a namespace and the kernel auto-reaps its veth end
   and its per-namespace iptables rules; the root-namespace bridge is deleted
   explicitly with `ip link del`. Teardown order is: kill daemon PGIDs →
   `cmd.Wait` → `ip netns del` each namespace → `ip link del` the bridge. The
   leak gate then asserts namespace **absence** via `ip netns list` (the netns
   analog of the process-kill suite's `ps` process-absence gate). All cleanup
   failures are reported with `t.Errorf`, never `t.Fatalf` (ADR-0018
   cleanup discipline).

4. **`linux && netns` build tag + `runtime.GOOS`/sudo skip (SC2).** The package
   is `//go:build linux && netns` — invisible to `go test ./...` and
   uncompilable on macOS even with `-tags=netns`. A three-layer runtime guard
   (`runtime.GOOS != "linux"`, `os.Geteuid() != 0 && !sudoAvailable()` where
   `sudoAvailable()` runs `sudo -n true`, and `testing.Short()`) makes a
   Linux-without-root box **skip** rather than fail. "Skipped, not failed" holds
   in every invocation that cannot actually run the partition.

5. **A DEDICATED `netns` CI job (Option A) + the SC3-wording reading.** The
   netns chaos runs as a **dedicated** `netns` job on `ubuntu-latest`
   (linux-amd64), without the race detector, terminal (no re-run loop, no
   failure-tolerating modifier, no conditional guard). We read SC3's "linux-amd64
   matrix entry only" as satisfied by a dedicated `ubuntu-latest` job — it runs
   on exactly one linux-amd64 runner and never on macOS — rather than a guarded
   step inside the `test` matrix (which would have to skip the macOS cells and
   the non-1.26 linux cell anyway). RESEARCH flagged this exact "dedicated job
   vs. literal matrix entry" tension; we resolve it to Option A and record the
   reinterpretation in the ci.yml step comment, here, and in `.journal/M13.md`.
   Unlike the Phase-11/12 chaos steps that reused the `test` job, this adds the
   **8th** required-check name `netns` — enumerated in ADR-0002's amended
   required-check list and enforced by updating branch protection via `gh api`.

## Consequences

**Positive**

- **D-1 retired.** A real L3 `DROP` across distinct namespace addresses
  exercises dial/timeout/reconnect that a loopback partition cannot reproduce.
- **Zero new Go dependencies** — the harness shells `ip`/`iptables` rather than
  linking `netlink`.
- **Single-command teardown.** `ip netns del` auto-reaps the veth end and the
  per-namespace iptables rules; only the root bridge needs explicit removal.
- **Loud capability probe.** The CI job probes `iptables --version` +
  `ip netns add/del` before the test, so a runner capability gap fails at a
  clear step, not deep in the harness.

**Negative**

- **linux-amd64-only.** The job runs on exactly one runner class; other OSes
  skip by design (SC2).
- **Requires root / passwordless sudo.** Developer boxes without it **skip**
  rather than run the partition.
- **Adds one required check + a branch-protection update** — the ADR-0002 cost
  of a new job, paid here rather than deferred.
- **The "dedicated job = matrix entry" reading is a deliberate interpretation**
  of SC3's wording, recorded explicitly so it is signed off, not silent.

**Follow-ups**

- Phase 14 (observability, ADR backfill, README, GoReleaser release).
- A future ADR could parameterize N (currently fixed at 3, the minimum clean
  odd majority) if a larger partition topology is wanted.

## Usage

- Run the partition on a root / passwordless-sudo Linux host:
  `sudo -E env "PATH=$PATH" go test -tags=netns -timeout 10m ./test/chaos/netns`.
  After a run, `ip netns list` shows no `tr-<pid>-nX` namespaces (the SC1 leak
  gate).
- The package is invisible without the tag: `go test ./...` never compiles it,
  and macOS reports it excluded even with `-tags=netns`.
- Do not switch namespaces in-process; keep every mutation an `ip`/`iptables`
  subprocess (Decision 1). Do not install partition rules in the root/bridge
  namespace (Decision 2).
- If the CI job's `name:` (`netns`) ever changes, update ADR-0002's
  required-check list and re-run the branch-protection `gh api` update in the
  same PR (see `.journal/M13.md` for the replay).
