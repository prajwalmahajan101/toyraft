# Task: ToyRaft v1.0.0-rc.2 — make pkg/transport/http externally constructible

## Why (context — this came from dogfooding toykv)
toykv (github.com/prajwalmahajan101/toykv) is embedding ToyRaft for its M19 cluster
milestone and hit a hard blocker: `pkg/transport/http` cannot be constructed by an
external module at v1.0.0-rc.1.

Root cause (verified):
- `pkg/transport/http/config.go:35` — `Clock clock.Clock` (type from `internal/clock`).
- `pkg/transport/http/config.go:67-69` — `Config.Validate()` HARD-REJECTS a nil Clock.
- `pkg/transport/http/transport.go:51-54` — `New` calls `Validate()` and stops; it does
  NOT default the clock.
- An external module cannot supply a Clock: there is no public clock constructor, and
  `internal/clock.Clock` can't be implemented externally (its methods return the
  un-nameable `internal/clock.Timer`/`Ticker`).
- Contrast `pkg/raft/config.go:134-136` — `applyDefaults()` already does
  `if c.Clock == nil { c.Clock = clock.NewReal() }`. That's exactly why an external
  caller CAN build `raft.Config` while leaving `Clock` unset (nil zero value needs no
  naming). The http transport just needs the same treatment.
- The http transport only ever calls `Clock.After()` (`client.go:92`, `server.go:121`),
  so `clock.NewReal()` fully satisfies its real usage.

This is a NON-BREAKING fix (type unchanged, nil now permitted instead of rejected) →
ships as v1.0.0-rc.2.

## Conventions to respect
- Never commit to `main`. A branch `fix/http-transport-nil-clock-default` may already
  exist (created from another session) — check `git branch`; use it or create it.
- Conventional commits (commitlint is enforced in CI).
- ADRs in `docs/adr/` (TEMPLATE.md, INDEX.md) — next number is **0023**.
- Journal entry in `.journal/` (TEMPLATE.md).
- `make verify` = lld-drift + check-no-time-now; both must stay green.

## The fix

### 1. pkg/transport/http/config.go
Add an unexported defaulter mirroring pkg/raft.Config:
```go
// applyDefaults fills a nil Clock with the real clock so external consumers —
// who cannot construct internal/clock — can build the transport by leaving
// Clock unset. Mirrors pkg/raft.Config.applyDefaults; called by New before Validate.
func (c *Config) applyDefaults() {
	if c.Clock == nil {
		c.Clock = clock.NewReal()
	}
}
```
Update the `Clock` field doc comment (currently "MUST be non-nil; use internal/clock.Real")
to: "Optional; a nil Clock defaults to the real clock in New (parity with raft.Config).
Injecting a Fake requires internal access and is for in-module tests only."
Leave the `Validate()` nil-check as-is (now unreachable via New; still guards direct callers).

### 2. pkg/transport/http/transport.go
In `New`, call `cfg.applyDefaults()` BEFORE `cfg.Validate()`. `cfg` is a value param, so
the defaulted clock flows into `NewServer(cfg)`/`newClient(cfg)`. Drop the "cfg.Clock MUST
be non-nil" line from New's doc comment. No new import (clock lives in config.go).
Note: check-no-time-now stays clean — the call is `clock.NewReal()`, not `time.Now()`.

### 3. Test — pkg/transport/http/*_test.go
Add a case: `New(Config{NodeID, ListenAddr, PeerURLs, ... /* Clock left unset */})` returns
no error and the transport works (a two-node Send/receive round-trip, or at minimum New ok +
Close clean under the existing goleak TestMain). This locks the external-constructibility fix
in. Existing tests (which pass explicit Real/Fake clocks) stay unchanged.

## Docs
- **ADR-0023** `docs/adr/0023-http-transport-defaults-nil-clock.md` (use docs/adr/TEMPLATE.md):
  Context = the toykv dogfood finding above; Decision = default nil Clock → Real in New, parity
  with raft.Config; Consequences = non-breaking / more-permissive, Fake injection stays
  in-module; Alternative rejected = extract a public `pkg/raft.Clock` interface (breaking
  field-type change → defer to the doc.go "phase-5" work / a future v1.1). Add a line to
  `docs/adr/INDEX.md`.
- **.journal/** entry (follow .journal/TEMPLATE.md), recording the toykv-driven finding + fix.
- **LLD golden**: the Clock field doc-comment change will drift `docs/lld-go-doc-golden.txt` —
  run `make lld-drift-update` and commit the regenerated golden (verify only the intended
  comment lines changed).

## Verify
- `make verify` clean (lld-drift + check-no-time-now).
- `go test -race ./pkg/transport/http/...` green (incl. the new nil-clock test).
- `go test -race ./...` (or the non-chaos matrix) green.
- `make lint` clean.

## Finalize the release
- Commit on the branch → PR → merge to `main` → tag `v1.0.0-rc.2` (plain library semver tag
  per RELEASE_PLAN.md; no GoReleaser for a library tag).

## Downstream (happens in toykv, not here)
toykv adds `replace github.com/prajwalmahajan101/toyraft => ../toyraft` to unblock M19.1
immediately, then after rc.2 is tagged drops the replace and bumps
`require ... v1.0.0-rc.2`.
