# ToyRaft — SECURITY

**Status:** Accepted
**Date:** 2026-06-18
**Scope:** project-wide threat model + non-goals; satisfies QUAL-09

## Purpose

ToyRaft is an **educational consensus library** with a 3-node reference
demo. This document declares the threat model under which it is
designed to operate, the security work that is explicitly **not in
scope**, and the operational guidance for anyone tempted to deploy it
outside that model.

Read this before deciding to expose any ToyRaft endpoint to a network
you do not control.

---

## 1. Threat model

**Single-tenant, localhost-only, single-trust-domain.**

The operator runs every ToyRaft node and every client inside a trusted
network boundary they control — typically a single host or a tightly
isolated private network (a `docker-compose` bridge, a kubernetes
namespace with NetworkPolicy lockdown, a developer's laptop).

### Trust assumptions (each is load-bearing — do not relax silently)

- Every node on the cluster's wire protocol (`POST /raft/message`) is
  benign and well-behaved. Nodes do not lie about their term, do not
  forge votes, do not inject malformed entries — they may be slow,
  partitioned, or crashed, but they are not adversarial.
- Every client on the demo endpoints (`/kv/{key}`, `/status`) is
  authorized to perform the operation it issues. There is no
  authentication, no authorization check, no rate limit, no audit log.
- The loopback interface (or the trusted private network the operator
  has configured) is itself trusted: no one is sniffing, tampering, or
  replaying traffic on it.
- The host filesystem under `Config.DataDir` is owned by the operator
  and not readable by other tenants. Raft state (log, `HardState`) is
  stored unencrypted at rest.

If any of those assumptions is false in your deployment, ToyRaft is
the wrong tool. Stop and pick a hardened production consensus library
(`etcd`, `hashicorp/raft` with the security wrappers their consumers
have built) instead.

---

## 2. What is NOT in scope

Listed verbatim from `.planning/REQUIREMENTS.md` §Out of Scope and
`.planning/PROJECT.md` §Constraints. **None of these will be added in
v1**, and changing that requires an RFC.

- **TLS / HTTPS** on `/raft/message` or `/kv/*`. The transport is
  plain HTTP/1.1 with JSON bodies.
- **mTLS** node-to-node authentication. There is no client-cert check;
  any host that can reach `/raft/message` is treated as a peer.
- **Node-to-node auth** by any other mechanism (shared-secret HMAC,
  signed envelopes, JWT). The wire format has no auth field; see
  `docs/WIRE.md` §Message schema.
- **Client auth on demo endpoints.** `/kv/{key}` accepts any request
  from any source. `/status`, `/debug/pprof`, `/debug/vars` are
  unauthenticated by design (DEMO-03).
- **Encrypted-at-rest log.** Log segments and `HardState` are written
  as plain bytes; anyone with read access to `Config.DataDir` sees the
  full Raft state.
- **Audit logging.** No record is kept of who issued which proposal.
  Structured logs include node-local events only.
- **Rate limiting.** The transport accepts as many `POST /raft/message`
  requests as the host can process; the demo serves as many
  `/kv/{key}` requests as it can.
- **Secrets management.** No support for loading peer addresses or
  bootstrap config from a secrets store; everything comes from a
  config file or flags.

This list is **closed**: an item not on this list is also not in scope
unless `REQUIREMENTS.md` adds it explicitly. The default for any
security feature is "out of scope until proven necessary by a real
v1.x deployment with an RFC."

---

## 3. Per-message `fsync` and consumer durability

ToyRaft is designed in the same house as **toymq** (the sibling
project flagged in `PROJECT.md` §Context). toymq commits PUB with a
per-message `fsync`; consumers building on top of ToyRaft + toymq must
understand how the two durability layers compose.

### The durability ladder

A write travelling from a client through the demo to a cluster commit
crosses these durability boundaries:

1. **Leader log append (per-message `fsync`).** `Storage.Append` on the
   leader writes the entry and `fsync`s the segment file before
   returning success (STOR-04). At this point the entry survives a
   single-node crash on the leader.
2. **Follower log append (per-message `fsync`).** Each follower
   receiving `AppendEntries` writes and `fsync`s before responding.
3. **Quorum durability.** Once a majority of nodes have `fsync`ed the
   entry, the leader may advance `commitIndex` (subject to the
   current-term commit rule, P0-1 / Figure 8). At this point the entry
   survives any minority-node failure.
4. **State machine apply.** The apply loop invokes
   `StateMachine.Apply(index, entry)` on each node. The state machine's
   own durability (if any) is opaque to Raft.

### The contract for consumers

A write is **durable to the cluster** once `commitIndex` advances past
its index AND a quorum has `fsync`ed the entry — i.e., step 3 above.
Single-node `fsync` on the leader (step 1) is **necessary but not
sufficient**: if the leader crashes between step 1 and step 2 on any
follower, the entry can be lost (a new leader from the majority that
never saw it can overwrite it).

**Consumers building on top of ToyRaft must not ack a client write
before their `StateMachine.Apply` has been invoked for it.** Acking
earlier — for instance, returning success from `Propose()` as soon as
the local log append fsyncs — replicates toymq's "fsync-on-PUB"
guarantee at the wrong layer and creates the silent-message-loss
failure mode described in `research/PITFALLS.md` §D-4 (ToyMQ
integration contract).

The library API enforces this by returning a commit future from
`Propose`: see `docs/LLD.md` §Node (`Propose` returns
`<-chan ProposeResult` that resolves on apply, not on log append).
Consumers SHOULD NOT bypass this contract.

### Why this lives in SECURITY (not just CONCURRENCY)

Durability errors at this layer are not crashes — they are silent
data loss. From the operator's perspective, "ToyRaft acked my write
and then lost it" is indistinguishable from a malicious server lying
about durability. The mitigation is contractual, not cryptographic,
which is why it belongs in the threat-model discussion.

---

## 4. Endpoints NOT to expose to untrusted networks

The following endpoints exist for operational and demo purposes. They
are **unauthenticated by design**. Exposing any of them to an
untrusted network (the public internet, a shared corporate VLAN, a
multi-tenant kubernetes cluster without NetworkPolicy) is an
operational error, not a ToyRaft bug.

| Endpoint                | Layer                   | What it exposes                                                                 | If exposed                                                                                  |
| ----------------------- | ----------------------- | ------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `POST /raft/message`    | Raft peer transport     | Full Raft RPC surface (RequestVote, AppendEntries, …)                          | Anyone can forge votes, inject entries, advance terms; cluster integrity is destroyed       |
| `GET /status`           | Demo (`cmd/toyraftd`)   | Current term, role, leader, commitIndex, lastApplied                            | Information disclosure of cluster state                                                     |
| `GET /kv/{key}`         | Demo                    | The KV state machine's data                                                     | Plaintext data leak                                                                         |
| `PUT /kv/{key}`         | Demo                    | KV writes                                                                       | Anyone can write any value                                                                  |
| `DELETE /kv/{key}`      | Demo                    | KV deletes                                                                      | Anyone can delete data                                                                      |
| `/debug/pprof`          | Go runtime (DEMO-03)    | CPU / heap / goroutine profiles                                                 | Process introspection, side-channel for sensitive data, DoS via `?seconds=N`                 |
| `/debug/vars` (expvar)  | Go runtime (DEMO-03)    | Counter / gauge values                                                          | Information disclosure                                                                      |

All of these are bound by default to the loopback interface (`127.0.0.1`)
in `cmd/toyraftd`. Binding to `0.0.0.0` is a config choice the operator
makes deliberately; the README warns against it.

---

## 5. If you must expose ToyRaft

ToyRaft does not intend to make this easy. If you have decided to
deploy a ToyRaft cluster across a network boundary anyway, the
supported pattern is to front each node with a **reverse proxy + mTLS
terminator** outside the ToyRaft process:

- **nginx**, **Caddy**, **Envoy**, or **Traefik** terminate mTLS,
  authenticate the peer's client cert, and proxy plaintext to the
  ToyRaft node on `127.0.0.1`.
- The ToyRaft node is bound to `127.0.0.1` and never speaks to the
  network directly.
- The reverse proxy is responsible for: TLS termination, client-cert
  validation, request rate limiting, request logging, IP allowlisting,
  and (optionally) request-body size limits to mitigate DoS via
  oversized JSON.

This pattern is **out of scope** for the ToyRaft project: we do not
ship config templates, we do not test against a reverse proxy, we do
not consider proxy-induced bugs (e.g., HTTP/2 downgrade, header
mangling, idle-timeout mismatches) to be ToyRaft bugs.

If this paragraph does not feel like sufficient operational guidance
for your deployment, that is the correct signal: **ToyRaft is not the
right tool for your deployment**. Pick `etcd` or `hashicorp/raft`.

---

## Cross-references

- `.planning/PROJECT.md` — single-tenant / localhost-only constraint
  and the toymq-parallel context flag.
- `.planning/REQUIREMENTS.md` QUAL-09 (the requirement this document
  satisfies), STOR-04 (per-message `fsync`), DEMO-03 (demo endpoints).
- `docs/WIRE.md` §1–3 — the unauthenticated wire format that the
  threat model is built around.
- `docs/CONCURRENCY.md` §6 (shutdown) — `Stop()` does not flush any
  audit buffer because no audit log exists.
- `docs/LLD.md` §Node — `Propose` returns a commit future, enforcing
  the contract in §3.
- `research/PITFALLS.md` §D-4 — ToyMQ integration commit/ack ordering;
  the canonical write-up of the per-message-fsync / cluster-durability
  composition.
- Future RFC if v1.x adds any security feature listed in §2.
