# ToyRaft

A minimal, from-scratch implementation of the [Raft consensus
algorithm](https://raft.github.io/) in Go, with a small HTTP key/value demo on
top. It is a learning-grade reference: leader election, log replication, a
durable fsynced append-only log, an HTTP peer transport, and a two-binary demo
(`toyraftd` daemon + `toyraftctl` CLI) you can drive end-to-end with one command.

## Run the demo

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

## `toyraftd` flags

`toyraftd` is the reference daemon. It runs **two HTTP listeners**: the Raft peer
transport (consensus plane) on the PEER port, and a client-facing KV API on the
CLIENT port.

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

## HTTP endpoints (CLIENT port)

| Endpoint                  | Method              | Notes                                            |
| ------------------------- | ------------------- | ------------------------------------------------ |
| `/status`                 | `GET`               | node role/term/commit/apply + peer set (JSON)    |
| `/kv/{key}`               | `PUT` `GET` `DELETE`| set / read / delete a value                      |
| `/debug/pprof/`           | `GET`               | Go `net/http/pprof` (note the trailing slash)    |
| `/debug/vars`             | `GET`               | `expvar` runtime metrics                         |

Only the **leader** serves `/kv` and `/status` locally. A follower answers with a
`307 Temporary Redirect` whose `Location` points at the leader's CLIENT url (WIRE
§5), and adds an `X-Raft-Leader-Hint` header; if no leader is known it returns
`503`. A `307` preserves the method and body, so a client that follows redirects
reaches the leader transparently.

`GET /status` reports `role` as a **lowercase string** — `"follower"`,
`"candidate"`, or `"leader"` — never a numeric code.

## `toyraftctl`

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
