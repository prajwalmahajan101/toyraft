# ToyRaft

A minimal, from-scratch implementation of the [Raft consensus
algorithm](https://raft.github.io/) in Go, with a small HTTP key/value demo on
top.

## What it is

ToyRaft is a learning-grade reference implementation of Raft: leader election,
log replication, a durable fsynced append-only log, an HTTP peer transport, and
a two-binary demo (`toyraftd` daemon + `toyraftctl` CLI) you can drive
end-to-end with one command.

The consensus core (`pkg/raft`) is a single-mutex state machine driven by a
`Step`/`Ready` event loop — the whole of Raft's safety logic (Figure 8 commit
rule included) in one place you can actually read. Storage and transport are
interfaces (`pkg/storage`, `pkg/transport`) with an in-process implementation
for deterministic tests and an HTTP implementation for the real demo. The demo
daemon runs **two HTTP listeners**: the Raft peer (consensus) plane and a
client-facing KV API, so you can watch elections, replication, and partitions
with `curl` and a CLI.

It is built in the same house as its siblings [toykv](../toykv) and
[toymq](../toymq), and shares their durability discipline (per-message `fsync`
before ack).

## What it isn't

ToyRaft is **not production-grade** and does not try to be. Concretely:

- **Not hardened for hostile networks.** Every node and client is assumed
  benign — no authentication, no authorization, no TLS/mTLS, no rate limiting,
  no audit log. The trust model is single-tenant, localhost-only (see
  [docs/SECURITY.md](docs/SECURITY.md)). Nodes may be slow, partitioned, or
  crashed, but they are never adversarial.
- **Localhost-only by default.** Every listener binds `127.0.0.1`. Binding
  `0.0.0.0` is a deliberate operator choice the docs warn against.
- **No real snapshots / log compaction.** `Snapshot`/`Restore` return
  `ErrSnapshotUnsupported` — the log grows unbounded in v1. The interfaces are
  frozen for a future implementation, but only stubs ship.
- **Not tuned for throughput.** The core is guarded by a single mutex
  (deliberately — it makes the safety argument tractable, see ADR-0004), not
  sharded or pipelined for high write rates.
- **Not a drop-in for `etcd` / `hashicorp/raft`.** If you need any of the
  above, use one of those instead.

## Quickstart

```sh
make demo          # 3-node cluster on client ports 9001-9003 (peer ports 7001-7003)
make demo N=5      # 5-node cluster on client ports 9001-9005 (peer ports 7001-7005)
```

`N` is the cluster size and **must be odd** — Raft needs an odd member count for
a clean majority, so `make demo N=4` fails fast (before launching anything) with
`N must be odd`.

`make demo` builds both binaries into `bin/` and runs `scripts/smoke.sh`, which:

1. boots an `N`-node cluster and waits for the first leader election;
2. checks every node answers `GET /status`, `/debug/pprof/`, `/debug/vars`, and
   the leader serves `PUT`/`DELETE`/`GET /kv/{key}`;
3. round-trips a write: `toyraftctl set k v` then `get k` returns `v` (the CLI
   follows the follower 307 redirect to the leader);
4. **partitions the leader** (`kill -STOP`) and verifies a NEW leader is elected;
5. writes a value against the new leader, then **heals** the partition
   (`kill -CONT`) and verifies the OLD leader steps down and converges onto the
   new write.

The script's exit code is the pass/fail signal: it prints `SMOKE PASS` and exits
`0` on success. A `trap` cleans up every child process and temp data dir on exit,
so no processes or directories leak between runs.

The partition uses `kill -STOP`/`kill -CONT` by default — portable and
root-free, so it runs in CI. To partition only the consensus (peer) plane while
keeping the client API reachable, drop the peer port instead (needs root):

```sh
# Linux, target a node's PEER port (e.g. 7001)
sudo iptables -A INPUT -p tcp --dport 7001 -j DROP   # partition
sudo iptables -D INPUT -p tcp --dport 7001 -j DROP   # heal
# macOS: an equivalent pfctl block on the peer port
```

The `netns`-based partition chaos (real L3 hops, not loopback) is exercised in
`test/chaos/netns` under a dedicated CI job (see ADR-0020).

## Install

Install the binaries directly with the Go toolchain:

```sh
go install github.com/prajwalmahajan101/toyraft/cmd/toyraftd@latest
go install github.com/prajwalmahajan101/toyraft/cmd/toyraftctl@latest
```

Or download a prebuilt release binary — GoReleaser publishes archives for both
`toyraftd` and `toyraftctl` across `{linux, macOS} × {amd64, arm64}` on each
tagged release. Grab the archive for your platform from the
[Releases](https://github.com/prajwalmahajan101/toyraft/releases) page.

To use ToyRaft as a **library** (build your own daemon on `pkg/raft`), add the
module with `go get`:

```sh
go get github.com/prajwalmahajan101/toyraft@latest
```

The library ships as plain Git semver tags (no GoReleaser build); only the two
demo commands ship as release binaries.

## Architecture sketch

```
                client (toyraftctl / curl)
                          |
                    CLIENT port  (KV API + /status + /debug/*)
                          |
   +----------------------+----------------------+
   |  cmd/toyraftd  (reference daemon)           |
   |                                             |
   |   pkg/kvsm  (KV state machine)              |
   |        ^                                    |
   |        | Apply(committed entries)           |
   |   pkg/raft.Node  (single-mutex core:        |
   |        Step / Ready event loop,             |
   |        election + replication + commit)     |
   |     /            \                          |
   |  pkg/storage      pkg/transport             |
   |  (fsynced log +   (in-proc for tests,       |
   |   HardState)       HTTP for the demo)       |
   +----------------------+----------------------+
                          |
                     PEER port  (POST /raft/message — consensus plane)
                          |
                   other toyraftd nodes
```

- **Raft core (`pkg/raft`).** A single mutex guards all state (ADR-0004). The
  node advances via `Step(Message)` and emits side effects through a `Ready()`
  drain (ADR-0008) — persist HardState/entries, send RPCs, apply committed
  entries. The Figure-8 safety fix is the current-term commit rule (ADR-0010).
- **Storage (`pkg/storage`).** A frozen interface (ADR-0005) with an
  append-only, per-message-fsynced file implementation and atomic `HardState`
  rename (`pkg/storage/file`). `Snapshot`/`Restore` are v1 stubs.
- **Transport (`pkg/transport`).** In-process Hub for deterministic chaos tests
  (ADR-0007) and an HTTP transport (ADR-0015) speaking `POST /raft/message`.
- **Demo daemon (`cmd/toyraftd`).** Two listeners (ADR-0016): the peer transport
  on the PEER port, and a client KV API on the CLIENT port. A follower answers
  `/kv` and `/status` with a `307` redirect (+ `X-Raft-Leader-Hint`) to the
  leader; if no leader is known it returns `503`.

### `toyraftd` flags

| Flag         | Meaning                                                             |
| ------------ | ------------------------------------------------------------------ |
| `-id`        | this node's stable NodeID; MUST appear in `-peers` (e.g. `n0`)      |
| `-peers`     | cluster membership as `id=host:peerport,...` (the PEER addresses, includes self) |
| `-listen`    | this node's CLIENT API bind address `host:port` (serves `/kv`, `/status`, `/debug/*`) |
| `-data-dir`  | directory for the durable append-only log + hard state             |
| `-seed`      | deterministic RNG seed for election-timeout draws (`0` = clock entropy) |
| `-log-level` | log verbosity: `debug` \| `info` \| `warn` \| `error` (default `info`) |

### Port scheme

`-peers` carries the **PEER** (consensus) addresses. Each node's **CLIENT** port
is derived by a fixed convention: `clientPort = peerPort + 2000`. So in the demo:

| Node | Peer addr (`-peers`) | Client addr (`-listen`, `toyraftctl -addr`) |
| ---- | -------------------- | ------------------------------------------- |
| `n0` | `127.0.0.1:7001`     | `127.0.0.1:9001`                            |
| `n1` | `127.0.0.1:7002`     | `127.0.0.1:9002`                            |
| `n2` | `127.0.0.1:7003`     | `127.0.0.1:9003`                            |

A single node, run by hand:

```sh
bin/toyraftd \
  -id n0 \
  -peers 'n0=127.0.0.1:7001,n1=127.0.0.1:7002,n2=127.0.0.1:7003' \
  -listen 127.0.0.1:9001 \
  -data-dir /tmp/toyraft/n0 \
  -seed 0 -log-level info
```

### HTTP endpoints (CLIENT port)

| Endpoint                  | Method              | Notes                                            |
| ------------------------- | ------------------- | ------------------------------------------------ |
| `/status`                 | `GET`               | per-node role/term/commit/apply + peer set (JSON); every node answers |
| `/kv/{key}`               | `PUT` `GET` `DELETE`| set / read / delete a value (leader-served)      |
| `/debug/pprof/`           | `GET`               | Go `net/http/pprof` (note the trailing slash)    |
| `/debug/vars`             | `GET`               | `expvar` runtime metrics + `raft.*` counters     |

`/status` is answered by **every** node (followers included) — it reports the
local view. `/kv` is leader-served: a follower returns a `307 Temporary
Redirect` whose `Location` points at the leader's CLIENT url (WIRE §5) and adds
an `X-Raft-Leader-Hint` header; if no leader is known it returns `503`. A `307`
preserves the method and body, so a client that follows redirects reaches the
leader transparently.

`GET /status` reports `role` as a **lowercase string** — `"follower"`,
`"candidate"`, or `"leader"` — never a numeric code.

### `toyraftctl`

The reference CLI. `-addr` targets any node's CLIENT api (default
`127.0.0.1:9001`); the CLI follows the `307` to the leader, so a command aimed at
any node reaches the current leader.

| Command                 | Effect                                                        |
| ----------------------- | ------------------------------------------------------------- |
| `set <key> <value>`     | `PUT /kv/<key>` with the value as the body                    |
| `get <key>`             | `GET /kv/<key>`; prints the value, exit `1` if not found      |
| `del <key>`             | `DELETE /kv/<key>`                                             |
| `status`                | pretty-print role / term / commit / apply / leader hint       |
| `members`               | pretty-print the peer set                                     |

```sh
bin/toyraftctl -addr 127.0.0.1:9001 set greeting hello
bin/toyraftctl -addr 127.0.0.1:9002 get greeting     # -> hello (follows 307)
bin/toyraftctl -addr 127.0.0.1:9003 status
```

## What surprised me

The honest "I didn't expect that" ledger — the most instructive part of building
a Raft from scratch. Drawn from the phase journals (`.journal/M*.md`):

- **The current-term commit rule is one `&&`, and it is the whole phase.** The
  Figure-8 safety fix reduces to adding `&& log[N].term == currentTerm` to the
  quorum check — a single conjunct that separates a correct Raft from a
  data-losing one (ADR-0010, M6).
- **An isolated leader keeps reporting `Role=Leader` at its stale term.**
  Network isolation does not demote a leader; partition is a *delivery*
  property, not a *role* property. That distinction cost an afternoon of
  debugging a test that routed proposals into a partitioned node (M6).
- **"No errors raised" is a silent-failure trap.** A cluster that quietly stops
  committing raises nothing — only *positive* oracles (≥N commits, ≥M leader
  changes) catch it. This shaped the entire chaos suite (M11).
- **The chaos harness was the source of non-determinism, not the chaos RNGs.**
  A between-pass `time.Sleep(2ms)` racing an async dispatcher goroutine made the
  committed set diverge run-to-run; the fix was a synchronous drain seam
  (ADR-0017, M11).
- **In-process loopback is "too friendly" and hides real partition bugs.** The
  loopback path can't exhibit a genuine L3 partition, so the netns harness gives
  each node its own network namespace + veth + a real bridge hop, and cuts links
  with `iptables -j DROP` (ADR-0020, M13).
- **`HardState.Commit` must persist even though `commitIndex` is "in-memory
  only."** The volatile-commit rule is a *node-level* policy; at the storage
  layer, `Commit` is part of the on-disk wire format. Getting it backwards
  silently fails conformance (ADR-0013, M8).
- **Porcupine linearizability checking needed a per-key partition to stay
  tractable**, and a deliberately-illegal history was the only way to prove the
  checker (and the Figure-8 visualization) was non-vacuous (ADR-0019, M12).

## Roadmap

Everything below is **aspirational / post-1.0** — not implemented in v1, and
each would land behind an RFC + ADR before any code:

- **Real snapshots + log compaction.** Replace the `ErrSnapshotUnsupported`
  stubs so the log doesn't grow unbounded (the interfaces are already frozen for
  it).
- **Dynamic membership.** Joint-consensus config changes to add/remove nodes at
  runtime instead of a fixed `-peers` set.
- **Leadership transfer.** A graceful `TransferLeadership` for planned failover
  and rolling restarts.
- **Batching / pipelining.** Amortize `fsync` and RPC cost across multiple
  in-flight proposals for higher throughput (v1 is deliberately single-mutex,
  one-at-a-time).

## License

Released under the [MIT License](LICENSE).
