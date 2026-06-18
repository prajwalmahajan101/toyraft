# ToyRaft Release Plan

**Status:** Accepted
**Date:** 2026-06-18
**Scope:** versioning, tagging, release gating

ToyRaft ships **two artefacts** with deliberately different release
mechanisms:

1. **The library** (`github.com/prajwalmahajan101/toyraft`) — consumed
   by `toykv-cluster`, `toymq-cluster`, and any other Go service via
   `go get`. Released through plain Git semver tags. No binary artefact
   for the library itself (QUAL-08).
2. **The demo binary** (`cmd/toyraftd` + `cmd/toyraftctl`) —
   distributed as pre-built binaries for users who want to run the
   reference cluster without a Go toolchain. Released through
   [GoReleaser v2][goreleaser] (QUAL-07).

The two mechanisms keep library consumers from being notified about
binary-only releases and vice versa, by partitioning the tag namespace.

[goreleaser]: https://goreleaser.com/

## Versioning

ToyRaft follows [Semantic Versioning 2.0.0][semver].

- `v<major>.<minor>.<patch>` — both library and binary.
- Pre-1.0 (`v0.x.y`): minor-bump = breaking allowed, patch-bump =
  non-breaking. We treat the public API as "ratifying" during the v0
  series; once consumers start depending on a release in production we
  cut `v1.0.0`.
- 1.0+: standard semver — `MAJOR` for breaking changes, `MINOR` for
  additive, `PATCH` for fixes.
- Pre-release suffixes use SemVer's hyphen-separated form: `v0.3.0-rc.1`.

[semver]: https://semver.org/spec/v2.0.0.html

## Tag Conventions

Two separate tag prefixes to keep library and binary release notes from
crossing wires:

| Artefact      | Tag format                          | Example                  |
| ------------- | ----------------------------------- | ------------------------ |
| Library       | `v<major>.<minor>.<patch>`          | `v0.1.0`, `v0.2.0-rc.1`  |
| Demo binary   | `toyraftd/v<major>.<minor>.<patch>` | `toyraftd/v0.1.0`        |

**Why two prefixes:** Go's module proxy treats any `v<…>` tag at the
repo root as a library release and notifies consumers. If we used the
same prefix for the binary, every binary release would falsely look
like a library release in `pkg.go.dev`, in `go list -m -u`, and in
Dependabot. The separate `toyraftd/v<…>` prefix is invisible to the
module proxy.

Library and demo versions evolve independently — a binary-only patch
(e.g. flag rename in `toyraftd`) does not bump the library version.

## Library Release Process

The library has no binary artefact. The release is the tag.

1. Open a release PR from `feature/release` (or a hotfix branch) into
   `main`. PR body summarises changes; CHANGELOG updated.
2. CI must be green on the QUAL-02 / QUAL-03 matrix: `{linux, macOS} ×
   {race, no-race}`.
3. The chaos suite (CHAOS-01) must pass.
4. The linearizability suite (LIN-04) must pass on ≥3 distinct seeded
   scenarios: steady-state, leader churn, packet loss.
5. A journal entry for the release version exists in `.journal/`
   (PROC-02 applied to Phase 14).
6. Merge the PR. Tag `main` with `vX.Y.Z`.
7. Push the tag. Done — `go get
   github.com/prajwalmahajan101/toyraft@vX.Y.Z` works.

No GoReleaser invocation for the library tag. The release is the tag
itself.

## Demo Binary Release Process

`toyraftd` and `toyraftctl` ship as pre-built binaries via GoReleaser
v2.

**Build matrix:** `{linux, macOS} × {amd64, arm64}` — four binaries
per artefact × two artefacts = eight files per release.

1. Steps 1–5 of the library release process all apply (CI green, chaos
   green, linearizability green, journal entry exists, CHANGELOG
   updated).
2. Tag `main` with `toyraftd/vX.Y.Z`.
3. The `release` GitHub Actions workflow detects the `toyraftd/`
   prefix, runs `goreleaser release --clean`, and publishes a GitHub
   Release with the eight binaries + their checksums.
4. The release notes are derived from the conventional-commit history
   between the previous `toyraftd/v…` tag and the current one.

The library tags (`v<…>`) **do not** trigger the GoReleaser workflow,
even though they are also git tags on `main`. The workflow gates on
the tag prefix.

## Release Gates

Every release — library or binary — passes the same gates:

| Gate | Source | Verification |
| ---- | ------ | ------------ |
| Lint clean | QUAL-01 | `make lint` (golangci-lint v2.x) |
| Race-free tests | QUAL-02 | `make test-race` in CI |
| Cross-platform CI | QUAL-03 | `{linux, macOS} × {race, no-race}` matrix |
| Chaos suite passed | CHAOS-01, CHAOS-07 | `make chaos`; no flakes (flake = P0 bug) |
| Linearizability green | LIN-04 | Porcupine check on ≥3 seeded scenarios |
| Journal entry exists | PROC-02 | `.journal/M14.md` (or equivalent for the release version) is committed |
| CHANGELOG updated | (Phase 14) | `CHANGELOG.md` has an entry for the release |

A release with any red gate is aborted; we fix the underlying problem
and cut a new pre-release (`vX.Y.Z-rc.N+1`). We do **not** override
gates "just for this release."

## CHANGELOG

`CHANGELOG.md` ships in **Phase 14** alongside the first tagged
release. It is not part of Phase 1's deliverable set — this document
references it forward so the structure is captured, but the file
itself doesn't exist yet.

Format: [Keep a Changelog][kac]. Section per release; sub-sections
grouped as `Added | Changed | Deprecated | Removed | Fixed | Security`.
Entries derived from the conventional-commit history.

[kac]: https://keepachangelog.com/en/1.1.0/

## Open Items (deferred)

- **Library + binary in a single tag?** Tempting to collapse to one
  release. Deferred: keeps the door open for `toyraftctl` to ship
  independently of the consensus core if its CLI surface evolves
  faster.
- **Multi-module repo?** If `cmd/toyraftd` grows into its own Go
  module with its own go.mod, the tagging story changes (`toyraftd/`
  becomes the submodule path, which Go natively understands). v1
  keeps a single module.
- **Signing release artefacts.** GoReleaser supports cosign / minisign;
  evaluated in Phase 14 alongside the first release.
