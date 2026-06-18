# WIRE — ToyRaft HTTP/JSON Protocol

**Status:** v1 frozen
**Scope:** node-to-node Raft RPC envelope + client-to-cluster redirect contract for the demo HTTP API.

> **Source of truth.** This document locks the on-the-wire shape that `pkg/transport/http` (Phase 9) MUST marshal/unmarshal against, and that `cmd/toyraftd` (Phase 10) MUST implement for client redirects.
>
> **Companion docs.** `docs/LLD.md` §[Message](./LLD.md#message-worked-struct) is the canonical Go shape this document projects to JSON. `docs/LLD.md` §[Sentinel errors](./LLD.md#4-sentinel-errors) defines the error sentinels referenced below.

---

## 1. Transport-level envelope

All node-to-node Raft RPCs use a single HTTP endpoint:

```
POST /raft/message
Host: <peer host>
Content-Type: application/json
Content-Length: <bytes>

<JSON body — see §2>
```

Success response:

```
HTTP/1.1 204 No Content
```

There is **no response body** on success and **no JSON response envelope** for the Raft message itself. The transport is one-way fire-and-forget: when a follower needs to send a `RequestVoteResponse` or `AppendEntriesResponse` back to the leader, it issues its OWN separate `POST /raft/message` to the leader's `/raft/message` endpoint, carrying the response Message as the body.

**Asymmetry rationale.** A synchronous request/response shape would couple the sender's goroutine to the receiver's processing time, defeating the "send is best-effort, retry on heartbeat" model that makes Raft tolerant of slow peers. Both directions of an RPC pair are independent `POST /raft/message` calls.

### Headers

| Header               | Direction          | Meaning                                                                                                                                            |
| -------------------- | ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Content-Type`       | request            | MUST be `application/json`. Other values: respond `415 Unsupported Media Type` with the error envelope (§3).                                       |
| `Content-Length`     | request            | Required. Streaming/chunked uploads MAY be rejected; transports SHOULD bound the body (e.g. 8 MiB) and respond `413 Payload Too Large` past it.    |
| `X-Raft-Leader-Hint` | response (4xx/5xx) | Set when the responder is not the leader; carries the believed leader NodeID. See §4.                                                              |

### HTTP status codes

| Status                 | Meaning                                                                                                |
| ---------------------- | ------------------------------------------------------------------------------------------------------ |
| `204 No Content`       | Message accepted by `Node.Step`. The Raft response (if any) arrives via a separate inbound POST.       |
| `400 Bad Request`      | JSON parse error, missing required field, unknown `MessageType`. Body: error envelope (§3).            |
| `404 Not Found`        | Path is not `/raft/message`. Body: empty.                                                              |
| `405 Method Not Allowed` | Not `POST`. Body: empty.                                                                             |
| `409 Conflict`         | `ErrNotLeader` returned by `Node.Step` on a leader-only message. Body: error envelope (§3) + `X-Raft-Leader-Hint`. |
| `413 Payload Too Large`| Body exceeded the transport's configured cap.                                                          |
| `415 Unsupported Media Type` | `Content-Type` was not `application/json`.                                                       |
| `503 Service Unavailable` | `ErrStopped` or `ErrProposalDropped` returned by `Node.Step`. Body: error envelope (§3).            |

---

## 2. JSON schema for `Message`

Field naming uses `snake_case` on the wire; the Go struct uses `CamelCase`. The Go marshaller MUST use `json:"snake_case"` struct tags to enforce the projection. Zero-valued fields MAY be omitted (`omitempty`); parsers MUST treat missing fields as zero values.

The full field set, mirroring `pkg/raft.Message`:

```jsonc
{
  "type": 0,               // MessageType: 0=RequestVote, 1=RequestVoteResponse,
                           //              2=AppendEntries, 3=AppendEntriesResponse
  "term": 7,
  "from": "node-1",
  "to":   "node-2",

  "last_log_index": 42,    // RequestVote
  "last_log_term":  6,     // RequestVote
  "vote_granted":  true,   // RequestVoteResponse

  "prev_log_index": 41,    // AppendEntries
  "prev_log_term":  6,     // AppendEntries
  "entries": [             // AppendEntries
    { "term": 7, "index": 42, "data": "aGVsbG8=" }  // data is base64-encoded
  ],
  "leader_commit": 40,     // AppendEntries
  "success":     true,     // AppendEntriesResponse
  "match_index": 42,       // AppendEntriesResponse

  "conflict_term":  0,     // AppendEntriesResponse (fast-rollback hint)
  "conflict_index": 0      // AppendEntriesResponse (fast-rollback hint)
}
```

`entries[].data` is a `[]byte` in Go, encoded as a **standard base64** string on the wire (Go `encoding/json` default). Empty data is the empty string `""`.

### 2.1 RequestVote (type=0)

Candidate → all other peers.

```json
{
  "type": 0,
  "term": 7,
  "from": "node-1",
  "to":   "node-2",
  "last_log_index": 42,
  "last_log_term":  6
}
```

### 2.2 RequestVoteResponse (type=1)

Voter → candidate.

```json
{
  "type": 1,
  "term": 7,
  "from": "node-2",
  "to":   "node-1",
  "vote_granted": true
}
```

### 2.3 AppendEntries (type=2)

Leader → follower. `entries` is empty for a pure heartbeat.

```json
{
  "type": 2,
  "term": 7,
  "from": "node-1",
  "to":   "node-2",
  "prev_log_index": 41,
  "prev_log_term":  6,
  "entries": [
    { "term": 7, "index": 42, "data": "UFVUIGZvbyBiYXI=" },
    { "term": 7, "index": 43, "data": "REVMIGZvbw==" }
  ],
  "leader_commit": 40
}
```

### 2.4 AppendEntriesResponse (type=3)

Follower → leader. On `success=true`, `match_index` is the follower's last matching index. On `success=false`, the `conflict_term` / `conflict_index` pair carries the fast-rollback hint (Raft §5.3 optimisation): leader uses them to jump `nextIndex[follower]` past the divergent suffix in one round trip.

Success example:

```json
{
  "type": 3,
  "term": 7,
  "from": "node-2",
  "to":   "node-1",
  "success": true,
  "match_index": 43
}
```

Failure (conflict) example:

```json
{
  "type": 3,
  "term": 7,
  "from": "node-2",
  "to":   "node-1",
  "success": false,
  "conflict_term": 5,
  "conflict_index": 38
}
```

### 2.5 Tick (type=255) — internal-only, NOT wire-visible

`MsgTick` (LLD `MessageType` value 255) drives the core state machine from the driver's tick loop. It MUST NEVER appear on a `POST /raft/message` body. Documented here only so a future maintainer reading `pkg/raft/message.go` knows why the value is reserved. HTTP receivers MUST reject `type=255` with `400 Bad Request` + error envelope `{"error":"bad_request"}`.

---

## 3. Error response envelope

For any non-`204` response carrying an application-level error, the body is:

```json
{
  "error": "<sentinel>",
  "leader_hint": "<NodeID or empty>"
}
```

The `error` field is one of the following stable string sentinels (kept lowercase, snake_case, and append-only):

| Sentinel             | Maps to Go error          | HTTP status |
| -------------------- | ------------------------- | ----------- |
| `not_leader`         | `*raft.ErrNotLeader`      | `409 Conflict` |
| `stopped`            | `raft.ErrStopped`         | `503 Service Unavailable` |
| `proposal_dropped`   | `raft.ErrProposalDropped` | `503 Service Unavailable` |
| `bad_request`        | JSON parse / validation   | `400 Bad Request` |
| `payload_too_large`  | body cap exceeded         | `413 Payload Too Large` |
| `unsupported_media`  | non-JSON `Content-Type`   | `415 Unsupported Media Type` |

`leader_hint` is set only when `error == "not_leader"` and the responder has a believed leader; otherwise it is `""`.

Parsers MUST tolerate unknown `error` sentinels gracefully (treat as a generic error; log raw value). The list is append-only across versions.

---

## 4. `X-Raft-Leader-Hint` response header

When a Raft node returns `*raft.ErrNotLeader` from `Node.Step` (e.g. a stale leader receives a misrouted client-originated message via a buggy proxy), the transport MUST set:

```
X-Raft-Leader-Hint: <NodeID>
```

on the HTTP response, AND populate `leader_hint` in the JSON error envelope (§3). The header is provided for clients that inspect headers without parsing the body (and for the 307 redirect path in §5, where the body is irrelevant).

- The header value is the verbatim `string(NodeID)` — no encoding wrapper.
- The header is OMITTED (not empty-string'd) when `LeaderHint == ""`.
- Sources: `REQUIREMENTS.md` TRAN-04.

---

## 5. Client-facing redirect contract (demo HTTP API)

The demo binary `cmd/toyraftd` exposes a client-facing KV API:

```
GET    /kv/{key}
PUT    /kv/{key}    body: <opaque value bytes>
DELETE /kv/{key}
```

These endpoints are **separate from `/raft/message`** and follow a different error model: a follower receiving a write MUST respond with an HTTP redirect, not an error envelope.

### 5.1 Follower handling of mutating client requests

When a follower receives `PUT /kv/{key}` or `DELETE /kv/{key}`:

```
HTTP/1.1 307 Temporary Redirect
Location: <leader URL>/kv/{key}
X-Raft-Leader-Hint: <leader NodeID>
Content-Length: 0
```

- Status MUST be `307` (preserves method and body; `301`/`302` would downgrade `PUT` to `GET`).
- `Location` is the leader's full URL for the SAME resource. The follower constructs it from its known `PeerAddrs[leaderID]` + the original request path.
- `X-Raft-Leader-Hint` carries the leader NodeID (operator-friendly diagnostic; same semantics as §4).
- Body is empty.

If the follower has no leader hint (`LeaderHint() == ""`), it responds:

```
HTTP/1.1 503 Service Unavailable
Content-Type: application/json

{"error": "no_leader_known", "leader_hint": ""}
```

The client SHOULD retry after a backoff.

### 5.2 GET handling

`GET /kv/{key}` in v1 is leader-only-read (no `ReadIndex`, no lease). Followers redirect `GET` with `307` exactly as for `PUT`/`DELETE`. This is the v1 simplification noted in `ARCHITECTURE.md` §Linearisability note; v2 may introduce follower reads via ReadIndex (separate RFC).

### 5.3 Distinction from `/raft/message`

| Endpoint         | Audience       | On not-leader        |
| ---------------- | -------------- | -------------------- |
| `/raft/message`  | peer nodes     | `409` + error envelope (§3); NO redirect |
| `/kv/{key}`      | external clients | `307` + `Location` + `X-Raft-Leader-Hint` |

The peer-RPC path does NOT redirect because Raft messages are addressed to a specific `to` NodeID; redirecting them would be a logic error. The client-facing path DOES redirect because it is the documented affordance for clients that don't track leadership themselves (DEMO-04).

---

## 6. Forward-compatibility policy

### 6.1 Unknown JSON fields are ignored (forward-compat)

Parsers MUST ignore unknown fields. This is the load-bearing forward-compat rule. This frees v2 to add fields (e.g. snapshot metadata, learner flags) without breaking v1 readers. Go implementations MUST NOT use `DisallowUnknownFields` on the wire decoder.

### 6.2 `MessageType` enum is append-only

Values `0` through `3` are frozen by this document. v2 may add new values (e.g. `4 = MsgInstallSnapshot`); v1 receivers will respond `400 Bad Request` with `error=bad_request` for unknown types, which v2 senders treat as a "peer doesn't support this RPC" signal.

### 6.3 Error sentinel list is append-only

New error sentinels MAY be added in future versions. Clients MUST tolerate unknown sentinels (treat as a generic error; preserve the raw string in logs).

### 6.4 HTTP-level framing

The HTTP `Content-Length` header is sufficient framing for the JSON body; there is NO additional length-prefix at the message level. The CRC + length-prefix framing in `STOR-03` is for the **on-disk** log format in `pkg/storage/file`, NOT the wire. Do not confuse the two.

### 6.5 Versioning

There is NO `v1` URL prefix. v2 will either (a) introduce new endpoints at new paths, or (b) extend the existing endpoint via additive JSON fields and new `MessageType` values. Breaking changes — if ever required — will live behind a new path (e.g. `/raft/v2/message`) chosen in an RFC.

---

## 7. Cross-references

- **Go shape:** `docs/LLD.md` §[Message](./LLD.md#message-worked-struct), §[Sentinel errors](./LLD.md#4-sentinel-errors).
- **Requirements:** TRAN-01..TRAN-06, DEMO-04 (`REQUIREMENTS.md`).
- **Source material:** `.planning/research/ARCHITECTURE.md` §Transport, `.planning/research/SUMMARY.md` §HTTP/JSON wire format v1.
