# 0016 — Reference demo daemon: two-listener model + non-blocking peer sends

**Status:** Accepted
**Date:** 2026-07-04
**Scope:** `cmd/toyraftd`, `cmd/toyraftctl`, `pkg/kvsm`, `scripts/smoke.sh`

## Context

Phase 10 wires the library (`raft.Node`, Phase 7), the file storage (Phase 8),
and the HTTP transport (Phase 9) into a runnable cluster driven by `make demo`.
Standing up `cmd/toyraftd` surfaces four decisions the HLD/LLD/WIRE leave to the
demo layer:

1. **Client API vs consensus plane.** The Phase 9 `pkg/transport/http` server is
   frozen and serves ONLY `POST /raft/message` on the peer port (ADR-0015). The
   demo also needs a client-facing KV API (`GET/PUT/DELETE /kv/{key}`,
   `GET /status`, `/debug/pprof`, `/debug/vars`). Bolting KV routes onto the
   frozen transport server would break its contract and its address book.

2. **How a follower redirects a client to the leader.** WIRE §5 mandates a `307`
   + `Location` pointing at the leader's *client* URL. The transport's
   `PeerURLs` (ADR-0015) map NodeID → *peer* URL, not client URL. The daemon
   therefore needs a second address book.

3. **Reads.** `Node.Propose` returns `(Index, Term, error)` and discards the
   Apply result (`pkg/raft/node_public.go`), so a write is `Propose(op)` → `200`.
   A linearizable read needs a strategy; v1 has no read-index machinery.

4. **A frozen peer must not stall elections.** The Phase 7 driver calls
   `Transport.Send` synchronously inside the tick loop. Under the SC5 partition
   (`kill -STOP` the leader), a synchronous `Send` to the frozen peer blocks the
   whole tick up to `SendTimeout`. Observed at N=5: heartbeats desynchronised
   badly enough that the cluster churned terms (11→22→34) without settling — the
   SC5 gate was unreachable.

## Decision

**We will run two HTTP listeners per node.** The frozen Phase 9 transport server
stays on the peer port serving only `POST /raft/message`. A SEPARATE
`http.Server` this daemon owns serves the client API on the client port. The
daemon derives its own client bind from `-listen`; every other node's client URL
is derived from its peer `host:port` by the fixed convention
**`clientPort = peerPort + 2000`** (demo peer 7001 → client 9001). `-peers`
carries only peer addresses; `smoke.sh` and the README agree with this offset.

**We will build three collections from one `-peers` flag:** `raft.Config.Peers`
(includes self, odd-N — `Validate` hard-errors otherwise), the transport's
`PeerURLs` (EXCLUDES self, ADR-0015), and `clientURLs` (all nodes → client URL,
used to build the redirect `Location`). Redirect `Location` and
`X-Raft-Leader-Hint` come from `clientURLs[node.LeaderHint()]`; an empty hint
yields `503 no_leader_known`.

**Reads are leader-only.** `GET /kv/{key}` reads `kvsm.Get` directly on the
leader; a follower `307`-redirects GET exactly like PUT/DELETE (WIRE §5.2). This
is not fully linearizable (no read-index), which is acceptable and documented for
a v1 reference demo.

**The client API is served on `http.DefaultServeMux`** so the blank imports of
`net/http/pprof` and `expvar` register `/debug/pprof/*` and `/debug/vars` with no
extra wiring; KV routes are added to the same mux.

**Peer sends are wrapped non-blocking** (`cmd/toyraftd/asynctransport.go`): a
`raft.Transport` decorator with a per-peer buffered queue + drain goroutine.
`Send` enqueues and returns immediately; a full queue drops the message and the
next heartbeat is the retry. This stays within the frozen best-effort `Send`
contract. `SendTimeout` is tightened to 150 ms and `MaxAttempts` to 1 (the next
heartbeat, not this loop, is the real retry). `node.Stop()` closes the wrapper,
which stops the drain goroutines then closes the inner HTTP transport.

## Consequences

**Positive**
- The frozen Phase 9 transport is untouched; the demo composes it rather than
  modifying it. `pkg/raft` and `pkg/transport/http` source stay byte-identical
  (verified: `make lld-drift` OK, `go test -race ./pkg/raft/...` green).
- SC5 becomes reliable: N=3/5/7 re-elect within ~1.5 s under a STOP'd leader;
  `make demo` prints `SMOKE PASS`.
- `/debug/pprof` + `/debug/vars` come for free from stdlib blank imports.
- One `-peers` flag drives the whole cluster; the `+2000` convention keeps the
  demo memorable and scriptable.

**Negative**
- Reads are leader-only, not linearizable — a v1 limitation to revisit if the
  demo ever needs stale-read tolerance or a read-index path.
- `http.DefaultServeMux` globally couples the KV mux to pprof/expvar; fine for a
  single demo binary, but a library consumer would use a private mux.
- The async decorator + tightened timeouts live in `cmd/toyraftd`, so their
  benefit does not extend to other `Transport` consumers — by design (the demo
  owns its liveness tuning; the core stays policy-free).

**Follow-ups**
- Phase 14 (Observability) may promote the `/status` + `/debug/vars` counter set
  and revisit whether a read-index path is worth adding to the core.
- If a future phase needs non-blocking sends in the core driver rather than the
  demo wrapper, that is a separate ADR against `pkg/raft`.
