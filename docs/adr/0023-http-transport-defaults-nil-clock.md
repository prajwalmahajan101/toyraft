# 0023 — Default a nil HTTP-transport Clock to the real clock in New

**Status:** Accepted
**Date:** 2026-09-05
**Scope:** `pkg/transport/http`

## Context

Dogfooding surfaced a hard blocker. toykv (`github.com/prajwalmahajan101/toykv`),
an external module embedding ToyRaft for its M19 cluster milestone, could not
construct `pkg/transport/http` at v1.0.0-rc.1:

- `pkg/transport/http/config.go` types `Config.Clock` as `internal/clock.Clock`.
- `Config.Validate()` hard-rejects a nil `Clock`, and `New` calls `Validate()`
  without first defaulting it.
- An external caller cannot supply a `Clock`: there is no public constructor for
  `internal/clock`, and the `Clock` interface cannot be implemented outside the
  module because its methods return the un-nameable `internal/clock.Timer`/`Ticker`.

The Raft core already solved the identical problem: `pkg/raft.Config.applyDefaults()`
does `if c.Clock == nil { c.Clock = clock.NewReal() }`, which is exactly why an
external caller *can* build a `raft.Config` while leaving `Clock` unset — the nil
zero value needs no naming. The http transport only ever calls `Clock.After()`
(`client.go`, `server.go`), so `clock.NewReal()` fully satisfies its real usage.

## Decision

`New` calls `cfg.applyDefaults()` **before** `cfg.Validate()`. `applyDefaults`
defaults a nil `Config.Clock` to `clock.NewReal()`, mirroring
`pkg/raft.Config.applyDefaults`. `cfg` is a value parameter, so the defaulted clock
flows into `NewServer(cfg)`/`newClient(cfg)`. The `Validate()` nil-check is retained
(now unreachable via `New`, it still guards callers that invoke `Validate` directly).

## Consequences

**Positive**
- External modules can construct the HTTP transport by leaving `Clock` unset,
  matching the ergonomics of `raft.Config`. This unblocks toykv M19 without a
  `replace` directive after rc.2 is tagged.
- Non-breaking and strictly more permissive: the field type is unchanged and a
  previously-rejected input (nil) is now accepted with a sensible default.

**Negative**
- `Validate()`'s nil-Clock branch becomes dead on the `New` path, a minor
  redundancy kept deliberately as a guard for direct `Validate` callers.

**Follow-ups**
- Injecting a `Fake` clock still requires internal access and stays an in-module
  test concern.
- Rejected alternative: extract a public `pkg/raft.Clock` interface so external
  callers could inject any clock. That is a breaking field-type change; it is
  deferred to a future v1.1 (the `doc.go` "phase-5" work), not rc.2.
