# 0022 — GoReleaser ships both toyraftd and toyraftctl

**Status:** Accepted
**Date:** 2026-07-04
**Scope:** `.goreleaser.yml`, `Makefile` (release-snapshot / release-check)

## Context

Phase 14 delivers the release INFRA (SC8 / QUAL-07): a GoReleaser config so
`goreleaser release --snapshot` produces platform binaries for
{linux, macOS} × {amd64, arm64}. Two forces shape the exact shape of that
config:

1. **What to ship.** SC8's literal wording names only the `toyraftd`
   daemon binary. But ToyRaft's reference experience is two commands — the
   daemon (`cmd/toyraftd`) and the CLI (`cmd/toyraftctl`) that drives it (the
   `make demo` smoke path, the README Quickstart, and the 307-follow client
   flows all use `toyraftctl`). The sibling trilogy repos (toykv, toymq) both
   ship their CLIs alongside their daemons; shipping only `toyraftd` would
   force a user to `go install` the CLI separately, breaking that parity and
   the out-of-the-box demo.

2. **What NOT to ship.** The library package
   `github.com/prajwalmahajan101/toyraft` is consumed as a Go module, not run
   as a binary. QUAL-08 fixes its distribution path at plain git semver tags —
   a GoReleaser "build" for a library makes no sense.

Version provenance: Plan 14-01 added package-level `version`, `commit`, `date`
vars to `cmd/toyraftd/main.go` (default `dev`/`none`/`unknown`, printed by
`toyraftd -version`) specifically so a release stamp can bind via
`-ldflags -X`. `cmd/toyraftctl` has no such vars.

GoReleaser v2 schema constraint: the darwin→macOS archive rename must use a
`name_template` conditional; the `archives.replacements` key was REMOVED in
v2 and `goreleaser check` (the SC8 config gate) fails on it.

## Decision

We will ship **both** reference binaries from GoReleaser:

- **Two builds** in `.goreleaser.yml` — `id: toyraftd` (`./cmd/toyraftd`) and
  `id: toyraftctl` (`./cmd/toyraftctl`) — each targeting
  `goos: [linux, darwin] × goarch: [amd64, arm64]` (8 binaries total),
  `CGO_ENABLED=0`, `-trimpath`.
- `toyraftd`'s ldflags stamp `-X main.version={{.Version}}
  -X main.commit={{.Commit}} -X main.date={{.Date}}` onto the Plan 14-01 vars
  (plus `-s -w`). `toyraftctl` declares no such vars, so its ldflags are
  `-s -w` only.
- **One archive per os/arch** bundling BOTH binaries plus `README.md`,
  `LICENSE`, `docs/SECURITY.md`. The darwin OS renders as `macos` in the
  archive name via a `name_template` conditional
  (`{{- if eq .Os "darwin" }}macos{{- else }}{{ .Os }}{{- end }}`) — **never**
  the removed `archives.replacements` key.
- **No GoReleaser build for the library package** — QUAL-08 is unchanged: the
  library releases via plain git semver tags only.

This is a **deliberate, user-approved deviation from SC8's "toyraftd only"
wording** (locked in `.planning/phases/14-.../14-CONTEXT.md`, decision 3). The
deviation adds the CLI; it does not drop the daemon or alter the platform
matrix.

`make release-snapshot` (`goreleaser release --snapshot --clean`) is the local
SC8 verification path; `make release-check` (`goreleaser check`) is the config
gate.

## Consequences

**Positive**
- Users get both the daemon and the CLI from a single release archive —
  the `make demo` / README Quickstart experience works from a downloaded
  tarball with no extra `go install`. Trilogy parity with toykv/toymq.
- `toyraftd` binaries carry a real build stamp (`toyraftd -version`), giving
  run-time provenance for issue reports.
- The v2 `name_template` conditional keeps `goreleaser check` green (no
  deprecated keys), so the SC8 config gate can run in CI.

**Negative**
- Two builds double the compile time of a release run (~8 binaries) and the
  archive size carries both commands even for users who want only one.
- A future third command would need a third build block (the config is
  explicit per-binary, not globbed).

**Follow-ups**
- Actual tag-cutting (`v1.0.0-rc.1`, `v1.0.0`) is a POST-roadmap release
  action, not Phase 14 — this ADR only ratifies the INFRA.
- ADR-0022 is referenced from the M14 phase journal (Plan 14-05) and listed
  in `docs/adr/INDEX.md`.
- If `toyraftctl` later grows its own version vars, extend its ldflags to
  stamp them (symmetric with `toyraftd`).
