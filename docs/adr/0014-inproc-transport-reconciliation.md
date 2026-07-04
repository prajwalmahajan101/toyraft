# 0014 — inproc transport reconciliation (additive Hub.Transport wrapper)

**Status:** Accepted
**Date:** 2026-07-04
**Scope:** `pkg/transport/inproc` (`transport.go`, `hub.go`), `pkg/transport/transporttest`

## Context

Phase 9 ships the production `raft.Transport` over HTTP and, as a prerequisite
(SC1/SC6, TRAN-06), must make the in-process `Hub` hand out a value satisfying
the frozen LLD §3 `raft.Transport` PUSH interface (`Send` / `Register(step)` /
`Close`). Two frozen artefacts DRIFT from the shipped code, and this ADR records
both explicitly so the divergence is traceable:

- **LLD §6 signature drift.** `docs/LLD.md:656` froze
  `func (h *Hub) Connect(nodeID) raft.Transport` and `func NewHub() *Hub`. The
  shipped code instead returns `*Endpoint` (`hub.go:97`) — a channel-PULL handle
  exposing `Send(ctx,msg)` / `Recv() <-chan Message` / `ID()`, NOT the
  `Register(step)` PUSH shape the interface requires. The code is drifted from
  its own frozen LLD signature.

- **Requirement-wording drift (TRAN-06 / Phase-9 SC6).** `.planning/REQUIREMENTS.md`
  TRAN-06 and the ROADMAP Phase-9 SC6 both literally read "`Hub.Connect(nodeID)`
  returns a `Transport`". Taken at face value this demands changing `Connect`'s
  return type — which this ADR declines (see Decision). So the requirement TEXT,
  not just the LLD signature, diverges from what ships.

The pull API is **load-bearing**. Grep-verified callers of `Connect`/`Recv`/
`*Endpoint`: `internal/raftest/cluster.go` (endpoints slice, `hub.Connect`,
`endpoint.Recv()`, plus its own `endpointTransport` pump adapter),
`pkg/transport/inproc/hub_test.go`, and the heavy `pkg/transport/inproc/chaos_test.go`
— the determinism backbone (ADR-0007). Changing `Connect`'s return type breaks
every one of them (~10 call sites in chaos alone) with no upside. A proven,
goleak-clean Register-pump adapter already exists in `internal/raftest/cluster.go`
(`endpointTransport`, RESEARCH Pattern 1): own ctx + `sync.WaitGroup` +
`sync.Once` Close, `Register` spawns a goroutine draining `Recv()` into `step`,
selecting on its own ctx (the Hub NEVER closes `Recv`, CONCURRENCY.md §5).

## Decision

We add an **ADDITIVE** `raft.Transport` wrapper rather than change `Connect`
(RESEARCH inproc-reconciliation Option 1b):

- New `hubTransport` type in `pkg/transport/inproc/transport.go` implements
  `raft.Transport` by wrapping an `*Endpoint` and COPYING the proven
  `endpointTransport` pump (own ctx/cancel/WaitGroup/Once; `Register` spawns one
  Recv→step pump; `Close` cancels and JOINS the pump — goleak-clean).
  Compile-asserted with `var _ raft.Transport = (*hubTransport)(nil)`.

- New method `func (h *Hub) Transport(id raft.NodeID) raft.Transport` calls the
  existing `h.Connect(id)` and wraps the returned `*Endpoint`. **`Connect`,
  `Endpoint`, and `Recv` are left byte-for-byte unchanged** — the entire
  chaos/hub/cluster suite keeps its pull API (zero blast radius).

- **Disconnect semantic = PUMP-STOP.** `hubTransport.Close()` cancels ONLY its
  own pump ctx: it stops inbound delivery to that node's `step`. The Hub and
  every other endpoint keep running; the Hub keeps its per-node inbound buffer
  and is NOT told to forget the node (no `Hub.Disconnect`). This mirrors the
  HTTP transport, where `Close` stops the listener/inbound handler but the
  process persists — "disconnect" means "this node stops receiving", not "the
  bus forgets this node". It satisfies SC6 ("closing one node's transport
  disconnects only that node") because closing A's wrapper leaves B delivering.

The shared `transporttest.RunConformance` (SC1) exercises this wrapper (C1-C9),
and the same suite is reused verbatim by the HTTP transport in wave 2.

## Consequences

**Positive**
- Zero changes to the load-bearing pull API — the chaos determinism backbone
  (ADR-0007) and raftest harness are untouched; the blast-radius guard
  `go test -race ./internal/raftest/...` stays green.
- One shared conformance suite proves inproc and HTTP satisfy the same contract.
- The pump is copied from an already-goleak-proven adapter, not invented.

**Negative**
- `pkg/transport/inproc` now has TWO ways to obtain a per-node handle:
  `Connect(id) *Endpoint` (pull, for the test harnesses) and
  `Transport(id) raft.Transport` (push, for the production node). Callers must
  pick the right one; documented on both methods.
- The frozen LLD §6 `Connect(nodeID) raft.Transport` signature and the TRAN-06 /
  Phase-9 SC6 requirement WORDING ("`Hub.Connect(nodeID)` returns a `Transport`")
  are now **advisory / superseded** by this ADR. Both drifts are named here so
  the requirement-text divergence is traceable, not silent.

**Follow-ups**
- A future documentation pass MAY reconcile the LLD §6 signature and the
  REQUIREMENTS TRAN-06 wording to describe `Hub.Transport(id)` as the Transport
  accessor and `Hub.Connect(id)` as the pull accessor.
- `internal/raftest`'s private `endpointTransport` MAY later be deduped against
  `hubTransport` (`hub.Transport(id)`) — cleanup, not required by this phase.
