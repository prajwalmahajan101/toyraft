# 0015 — HTTP transport owns its peer-URL address book

**Status:** Accepted
**Date:** 2026-07-04
**Scope:** `pkg/transport/http` (Config, wire.go DTO)

## Context

Phase 9 adds `pkg/transport/http`, the node-to-node Raft transport that
marshals `raft.Message` to/from the byte-exact JSON contract frozen in
`docs/WIRE.md`. Standing it up surfaces two design questions the HLD/LLD do
not answer:

1. **Peer address resolution.** `raft.Config.Peers` is `[]raft.NodeID`
   (`pkg/raft/config.go`) — a bare membership list. There is NO
   `NodeID -> address` map anywhere in the core, and the core is deliberately
   URL-agnostic (it emits `Message` values addressed by `To NodeID` and hands
   them to a `Transport`; how a NodeID becomes a socket is the transport's
   concern). To send `POST /raft/message` (WIRE §1) the transport MUST resolve
   `msg.To` to a base URL. Something has to own that table.

2. **Where the JSON tags live.** `raft.Message` is a frozen public type with
   NO struct tags (`pkg/raft/types.go`). WIRE §2 mandates `snake_case` wire
   naming and standard-base64 `[]byte` encoding. Putting `json:"..."` tags on
   `raft.Message` would (a) pollute the frozen core type with a
   transport-specific concern and (b) couple every future JSON-shape change to
   a core-package edit.

The core stays free of both concerns; the transport must absorb them without
adding new public surface to `pkg/raft`.

## Decision

**`pkg/transport/http` owns its own `Config` address book.** We will define, in
`pkg/transport/http/config.go`:

```go
type Config struct {
    NodeID          raft.NodeID
    ListenAddr      string
    PeerURLs        map[raft.NodeID]string
    Clock           clock.Clock
    SendTimeout     time.Duration
    Backoff         BackoffConfig
    MaxBodyBytes    int64
    ShutdownTimeout time.Duration
}
```

`Send(msg)` resolves `PeerURLs[msg.To]` and POSTs to `<url>/raft/message`. An
unknown peer (no map entry) is an error the driver logs-and-drops — consistent
with WIRE §1's best-effort, retry-on-heartbeat send model. `raft.Config` is
NOT extended with a peer-URL map; the core stays URL-agnostic. `Config.Validate`
rejects a nil `Clock`, an empty `PeerURLs`, an empty `NodeID`, and a self-entry
in `PeerURLs`.

**All timers route through `clock.Clock`; there is no `time.Now()` in the
package.** `Config.Clock` is the sole time source for send timeouts and
backoff. `scripts/check-no-time-now.sh` is extended to cover
`pkg/transport/http` in plan 09-05.

**The JSON projection lives in a `wireMessage` DTO, NOT on `raft.Message`.**
`wire.go` defines an unexported `wireMessage` (and `wireEntry`) struct carrying
the `snake_case` `json` tags, with `toMessage`/`fromMessage` converters. The
frozen `raft.Message` stays tag-free. `Entry.Data []byte` uses `encoding/json`'s
default (standard base64) — deliberately NOT customized (WIRE §2). The decoder
runs in lenient mode (unknown JSON fields ignored, WIRE §6.1) and NEVER uses
`DisallowUnknownFields`.

## Consequences

**Positive**
- The frozen `raft.Message` and `raft.Config` are untouched — zero new
  exported core surface, no LLD-golden growth from this phase's wire layer.
- The transport owns exactly the two concerns the core delegates (address
  resolution + wire encoding); each is swappable/testable in isolation.
- The `wireMessage` DTO makes SC5 (`FuzzMessageParse`) and SC7
  (`WireConformance`) target a PURE decoder function with no network or
  handler in the loop.
- Forward-compat (WIRE §6.1) is structural: lenient unmarshal means v2 can add
  JSON fields without breaking v1 readers.

**Negative**
- Two shapes of the same data now exist (`raft.Message` and `wireMessage`),
  requiring keep-in-sync converters. Mitigated by `WireConformance`'s
  bidirectional golden round-trip, which fails loudly if a field is dropped.
- `Config.PeerURLs` is new public surface consumers (`cmd/toyraftd`, Phase 10)
  MUST populate; a missing/wrong entry is a runtime send failure, not a
  compile error.

**Follow-ups**
- 09-03 (server) and 09-04 (client) consume `statusForError` /
  `decodeMessage` and populate `Config`.
- 09-05 extends `scripts/check-no-time-now.sh` to guard `pkg/transport/http`.
- Phase 10 (`cmd/toyraftd`) constructs `Config.PeerURLs` from operator flags
  and reuses `PeerURLs[leaderID]` for the WIRE §5 client redirect `Location`.
