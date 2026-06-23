# 0009 — Per-Node RNG Mixing for Election Timeouts

**Status:** Accepted
**Date:** 2026-06-23
**Scope:** `pkg/raft` (`newNodeRNG`, election timeout draw)

## Context

ROADMAP SC2 mandates that every Raft node draws its randomised
election timeout from a per-node `*math/rand/v2.Rand` seeded from
`Config.Seed` (and, when `Config.Seed == 0`, from
`FNV(nodeID) ^ time.Now().UnixNano()`). Two nodes constructed with
the same `Config.Seed` but different `NodeID`s MUST produce divergent
timeout sequences, otherwise all nodes time out together and split-
vote convergence is impossible to study under chaos.

RESEARCH Pitfall P1-4 separately forbids the global `math/rand`
source: tests racing on it produce scheduler-dependent draws that
defeat SC3-style byte-identical-trace replay.

The election timer is on the hot path — `tickFollowerLocked` runs on
every `MsgTick`, and `resetElectionTimeoutLocked` re-draws every
term. Whatever mixing function we pick to turn
`(Config.Seed, NodeID)` into per-node RNG state runs on that hot
path. ADR-0007 already established the seed-splitting pattern for
the inproc Hub's chaos knobs, but ADR-0007 splits seeds ONCE at hub
construction; its SHA-256 choice was justified by setup-time
amortisation, not hot-path applicability.

This ADR locks the mixing function for the election timer (and any
future per-node `*rand.Rand` that 05-02 / Phase 6 / Phase 7 needs).

References: ROADMAP §SC2, ELEC-02, RESEARCH §Pitfalls P1-4,
`docs/adr/0007-inproc-hub-chaos-seed-splitting.md`,
`docs/adr/0008-step-event-loop-and-ready.md`,
`pkg/raft/rng.go::newNodeRNG`.

## Decision

We will:

1. **Hash `NodeID` via FNV-1a 64.** `nodeHash = fnv.New64a().Write([]byte(NodeID)).Sum64()`.
   FNV-1a is the canonical fast non-cryptographic hash for short
   strings; its avalanche on short inputs is sufficient to give two
   distinct `NodeID`s well-separated seeds.
2. **XOR-fold the user seed.** `seed = nodeHash ^ uint64(Config.Seed)`
   when `Config.Seed != 0`; `seed = nodeHash ^ uint64(time.Now().UnixNano())`
   when `Config.Seed == 0`.
3. **Expand via splitmix64.** `lo, hi = splitmix64(seed)`. PCG's two-
   word state benefits from inputs with good bit-mixing; a tiny seed
   fed directly to `NewPCG` yields a tiny state and a measurably
   weaker first stretch of output. splitmix64 (Vigna) is the
   canonical fix and is what `math/rand/v2.NewPCG`'s reference
   implementation recommends.
4. **Construct via `math/rand/v2.New(rand/v2.NewPCG(hi, lo))`.**
   `math/rand/v2`, NOT v1 — P1-4 forbids the global source and v2's
   per-instance API is reproducible across architectures.
5. **Treat the mixing function as ABI.** Renaming `splitmix64` or
   changing the FNV constant re-keys every committed test seed. A
   future change requires a superseding ADR and a sweep of every
   committed seed in the test corpus.

## Consequences

**Positive**

- **SC2 satisfied.** `TestRNGPerNodeSameSeedDifferentNodeIDs` asserts
  ≥95/100 differing draws for `(seed=42, "node-a") vs (seed=42, "node-b")`,
  and `TestRNGZeroSeedDifferentNodeIDsDiffer` asserts the same for
  the unseeded branch.
- **Reproducibility holds for chaos replay.** `TestRNGReproducibleSameSeedSameNodeID`
  asserts byte-identical first-100 draws across two independent
  constructions with the same inputs. Phase 11 chaos replay relies
  on this.
- **No crypto dependency on the election-timer hot path.** FNV-1a +
  splitmix64 are pure ALU; no SHA round costs.
- **Per-node RNG defeats P1-4.** Tests can run `t.Parallel()` over
  Cluster harnesses without scheduler-dependent draws.

**Negative**

- **FNV-1a is not cryptographic.** A malicious caller crafting
  `NodeID`s could theoretically force collisions and align two
  nodes' timeout streams. Acceptable: `Config.Seed` is local-trust-
  only (test/operator input); there is no adversarial threat model
  here.
- **Mixing function is ABI.** Tag-string-style commitment — any
  change to the FNV constant, splitmix64 multipliers, or PCG-word
  order re-keys every committed test seed. The forwarding hash chain
  protects us against accidental drift; deliberate change requires
  this ADR's supersession.
- **Zero-seed branch is non-deterministic.** That is intentional
  (SC2 phrases it as a feature for production), but means
  `cfgSeed == 0` tests cannot assert exact draw values — only that
  `*Rand` is non-nil and does not panic. `rng_test.go` documents this.

**Follow-ups**

- Phase 6's heartbeat jitter (if added) draws from this same
  `n.rng`; the ABI commitment extends.
- Phase 11 chaos replay tests pin a small set of `(Config.Seed,
  NodeID)` pairs in the test corpus; renames there are guarded by
  the wall-clock-ban + LLD-drift gates established in Phase 4.

## Alternatives Considered

- **SHA-256 of `(Seed || NodeID)`.** ADR-0007's choice for hub seed
  splitting. REJECTED for the election-timer hot path: a SHA round
  per `Tick` is 10–100× the cost of FNV-1a + splitmix64. ADR-0007
  amortises the cost because it splits seeds ONCE at hub
  construction. See `docs/adr/0007-inproc-hub-chaos-seed-splitting.md`
  §Alternatives Considered for the symmetric note.
- **`crypto/rand` for the unseeded case.** REJECTED: violates SC2's
  exact formula (`FNV(NodeID) ^ time.Now().UnixNano()`) and makes
  replay-with-recorded-seed impossible.
- **`math/rand` (v1) global source.** REJECTED: P1-4 forbids it;
  scheduler-dependent draws would defeat SC3 replay.
- **XOR seed and `NodeID` bytes directly (no hash).** REJECTED: short
  `NodeID`s ("n1", "n2") share a prefix and only the trailing byte
  differs — a direct XOR aligns most bits and produces nearly
  identical PCG states. FNV's avalanche fixes this.
- **`maphash` from stdlib.** Tempting but `maphash.Hash` is seeded
  per-process (`maphash.MakeSeed`) and therefore non-reproducible
  across runs — defeats SC2's reproducibility corollary. REJECTED.

## References

- ROADMAP §SC2 (`.planning/ROADMAP.md`)
- RESEARCH §Pitfalls P1-4 (`.planning/research/PITFALLS.md`)
- ELEC-02 (frozen interfaces, Phase 2)
- ADR-0007 — inproc Hub chaos seed splitting (contrast: setup vs hot path)
- ADR-0008 — step() event loop and Ready
- splitmix64 reference (Vigna): <https://prng.di.unimi.it/splitmix64.c>
- `math/rand/v2` API: <https://pkg.go.dev/math/rand/v2>
- `pkg/raft/rng.go::newNodeRNG`
- `pkg/raft/rng_test.go` — SC2 + reproducibility tests
