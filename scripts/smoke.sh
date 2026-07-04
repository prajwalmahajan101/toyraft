#!/usr/bin/env bash
#
# scripts/smoke.sh — the ToyRaft end-to-end demo (DEMO-06/07/08).
#
# Boots an N-node cluster of bin/toyraftd, then proves, in order:
#   1. every node answers its client endpoints (GET /status, /debug/pprof/,
#      /debug/vars) and the leader serves PUT/DELETE/GET /kv/{key}   (SC3)
#   2. a write via toyraftctl round-trips (set k v; get k == v)      (SC1)
#   3. partitioning the leader elects a NEW leader within ~1.5s      (SC5)
#   4. a write against the new leader commits                        (SC5)
#   5. healing the partition steps the OLD leader down to follower
#      and converges it onto the new write                          (SC5/DEMO-08)
#
# The script's exit code IS the pass/fail signal: `SMOKE PASS`/exit 0 on
# success, a diagnostic + exit 1 on any failure.
#
# Port scheme (LOCKED in 10-02, matches cmd/toyraftd): node i (0-based) binds
# PEER port 7001+i and CLIENT port 9001+i (client = peer + 2000). -peers carries
# the PEER addresses; -listen is the node's client bind addr.
#
# PARTITION METHOD: `kill -STOP <leaderpid>` is the portable, root-free default
# (freezes ALL the leader's traffic, incl. its peer port, so survivors time out
# and re-elect). `kill -CONT` heals it. Alternatives that need root, targeting
# the PEER port only (client stays reachable to observe state):
#   Linux : sudo iptables -A INPUT -p tcp --dport 7001 -j DROP   (heal: -D ...)
#   macOS : pfctl-based block on the peer port
# The kill -STOP/-CONT default keeps this CI-runnable without privileges.
#
# Portable bash (Linux + macOS bash 3.2): no GNU-only flags, arrays guarded
# with :- for `set -u`, all timing budgets via nanosecond deadlines.

set -euo pipefail

N="${N:-3}"
BASEP=7001   # first peer port
BASEC=9001   # first client port (peer + 2000)
HOST=127.0.0.1
DATA="$(mktemp -d)"
BIN_D=bin/toyraftd
BIN_C=bin/toyraftctl
PIDS=()

# CURL timeouts (seconds). A kill -STOP'd node keeps its TCP port BOUND (the
# kernel completes the handshake) but the frozen process never answers — so an
# un-timed curl would hang forever. --connect-timeout bounds the dial and
# --max-time bounds the whole request so polling the STOPped leader returns fast.
CT=1   # connect timeout
MT=2   # total request timeout

# Cleanup trap (Pitfall 4/5): CONT any STOPped child so kill can reap it, kill
# every child daemon, and remove the temp data dirs — even on early exit — so a
# re-run never recovers stale logs and no process/port/dir leaks between runs.
cleanup() {
    for p in "${PIDS[@]:-}"; do
        [ -n "$p" ] || continue
        kill -CONT "$p" 2>/dev/null || true
        kill "$p" 2>/dev/null || true
    done
    for p in "${PIDS[@]:-}"; do
        [ -n "$p" ] || continue
        wait "$p" 2>/dev/null || true
    done
    rm -rf "$DATA"
}
trap cleanup EXIT

fail() { echo "SMOKE FAIL: $*" >&2; exit 1; }

# now_ns prints a monotonic-ish nanosecond stamp for deadline loops. GNU date
# gives ns; macOS `date` lacks %N, so fall back to whole seconds * 1e9.
now_ns() {
    local n
    n="$(date +%s%N 2>/dev/null)"
    case "$n" in
        *N|"") echo "$(( $(date +%s) * 1000000000 ))" ;;
        *)     echo "$n" ;;
    esac
}

client_port() { echo "$(( BASEC + $1 ))"; }
peer_port()   { echo "$(( BASEP + $1 ))"; }
client_addr() { echo "${HOST}:$(client_port "$1")"; }

# role_of <i> prints node i's role — the LOWERCASE STRING from GET /status
# ("follower"/"candidate"/"leader") per the 10-02 contract. NEVER a numeric
# 0/1/2. Uses jq when present, else a portable grep/sed pulling the quoted value.
# A node that redirects (307, e.g. a follower answering /status) is handled by
# curl -sS -L following the redirect to the leader, but for role detection we
# want the node's OWN role — so we do NOT follow redirects here and read the
# body only on a direct 200. A redirecting follower has an empty body, so we
# treat "no role parsed" as "not leader" (empty output).
role_of() {
    local body
    body="$(curl -s --connect-timeout "$CT" --max-time "$MT" "http://$(client_addr "$1")/status" 2>/dev/null || true)"
    [ -n "$body" ] || return 0
    if command -v jq >/dev/null 2>&1; then
        echo "$body" | jq -r '.role // empty' 2>/dev/null || true
    else
        # Extract the value of "role":"<value>" without assuming field order.
        echo "$body" | sed -n 's/.*"role"[[:space:]]*:[[:space:]]*"\([a-z]*\)".*/\1/p'
    fi
}

# find_leader scans 0..N-1 and prints the FIRST index whose role == "leader"
# (empty if none is currently a leader).
find_leader() {
    local i
    for (( i = 0; i < N; i++ )); do
        if [ "$(role_of "$i")" = "leader" ]; then
            echo "$i"
            return 0
        fi
    done
    return 0
}

# http_code <url> prints the HTTP status code curl saw (000 on connection
# refused), NOT following redirects — we only assert the node ANSWERED.
http_code() {
    curl -s --connect-timeout "$CT" --max-time "$MT" -o /dev/null -w '%{http_code}' "$1" 2>/dev/null || echo 000
}

# assert_responds <url> — fail unless the endpoint answers with a real HTTP
# status (anything but 000 = connection refused/no server).
assert_responds() {
    local code
    code="$(http_code "$1")"
    [ "$code" != "000" ] || fail "no HTTP response from $1 (connection refused)"
}

# ---- Boot the cluster -------------------------------------------------------

# Build the shared -peers string: n0=HOST:7001,n1=HOST:7002,...
PEERS=""
for (( i = 0; i < N; i++ )); do
    entry="n${i}=${HOST}:$(peer_port "$i")"
    if [ -z "$PEERS" ]; then PEERS="$entry"; else PEERS="${PEERS},${entry}"; fi
done

echo "booting ${N}-node cluster (peer 7001..$(peer_port $((N-1))), client 9001..$(client_port $((N-1))))"
for (( i = 0; i < N; i++ )); do
    "$BIN_D" \
        -id "n${i}" \
        -listen "$(client_addr "$i")" \
        -peers "$PEERS" \
        -data-dir "${DATA}/n${i}" \
        -seed "$i" \
        -log-level info &
    PIDS+=($!)
done

# Wait for the first election to settle (poll for a leader up to ~3s).
deadline=$(( $(now_ns) + 3000000000 ))
LEADER=""
while [ "$(now_ns)" -lt "$deadline" ]; do
    LEADER="$(find_leader)"
    [ -z "$LEADER" ] || break
    sleep 0.1
done
[ -n "$LEADER" ] || fail "no leader elected within 3s"
echo "initial leader: n${LEADER} (client $(client_addr "$LEADER"))"

# ---- Endpoint checks (SC3) --------------------------------------------------

echo "checking endpoints on every node"
for (( i = 0; i < N; i++ )); do
    ca="$(client_addr "$i")"
    assert_responds "http://${ca}/status"
    assert_responds "http://${ca}/debug/pprof/"   # trailing slash: net/http/pprof
    assert_responds "http://${ca}/debug/vars"
done

# /kv PUT/GET/DELETE on the leader (leader-only routes; 200 each).
LCA="$(client_addr "$LEADER")"
echo "checking /kv PUT/GET/DELETE on the leader"
code="$(curl -s --connect-timeout "$CT" --max-time "$MT" -o /dev/null -w '%{http_code}' -X PUT --data-binary 'p' "http://${LCA}/kv/probe")"
[ "$code" = "200" ] || fail "PUT /kv/probe on leader returned $code (want 200)"
code="$(curl -s --connect-timeout "$CT" --max-time "$MT" -o /dev/null -w '%{http_code}' "http://${LCA}/kv/probe")"
[ "$code" = "200" ] || fail "GET /kv/probe on leader returned $code (want 200)"
code="$(curl -s --connect-timeout "$CT" --max-time "$MT" -o /dev/null -w '%{http_code}' -X DELETE "http://${LCA}/kv/probe")"
[ "$code" = "200" ] || fail "DELETE /kv/probe on leader returned $code (want 200)"

# ---- Write/read round-trip (SC1) --------------------------------------------

echo "write/read round-trip via toyraftctl (follows the 307 to the leader)"
"$BIN_C" -addr "$(client_addr 0)" set k v || fail "toyraftctl set k v failed"
got="$("$BIN_C" -addr "$(client_addr 0)" get k)" || fail "toyraftctl get k failed"
[ "$got" = "v" ] || fail "round-trip mismatch: got '$got' want 'v'"
echo "  k=v round-trip OK"

# ---- Partition the leader, expect re-election (SC5) -------------------------

echo "partitioning leader n${LEADER} (kill -STOP) — expecting a new leader"
kill -STOP "${PIDS[$LEADER]}" || fail "could not STOP leader pid ${PIDS[$LEADER]}"

NEW=""
# ~8s ceiling. The steady-state re-election is well under ~1.5s, but the
# daemon's per-Send backoff to the now-frozen leader (SendTimeout 1s) briefly
# stalls a campaigner's tick loop, so allow generous headroom to stay flake-free.
deadline=$(( $(now_ns) + 8000000000 ))
while [ "$(now_ns)" -lt "$deadline" ]; do
    for (( i = 0; i < N; i++ )); do
        [ "$i" -eq "$LEADER" ] && continue
        if [ "$(role_of "$i")" = "leader" ]; then
            NEW="$i"
            break
        fi
    done
    [ -z "$NEW" ] || break
    # 0.3s between sweeps: the frozen leader stalls each survivor's tick loop on
    # a ~1s Send backoff, so a tight poll would starve the very election we await.
    sleep 0.3
done
[ -n "$NEW" ] || { kill -CONT "${PIDS[$LEADER]}" 2>/dev/null || true; fail "no NEW leader elected after partition"; }
echo "  new leader: n${NEW} (client $(client_addr "$NEW"))"

# Write a new value while the partition holds. We do NOT pin the write to the
# node that first reported "leader": in a larger cluster leadership can still
# churn for a beat after the first election, so that node may have stepped down
# by the time we write (yielding 503 no_leader_known forever). Instead we aim
# each attempt at a SURVIVOR (any node but the frozen old leader) and let
# toyraftctl follow the 307 to whoever is leader RIGHT NOW. A freshly-elected
# leader may also still be committing its term's first entry, so a proposal can
# transiently 503 or time out; retry against a deadline comfortably larger than
# one toyraftctl 5s client timeout. TARGET picks the first non-frozen node.
TARGET=0
[ "$TARGET" -eq "$LEADER" ] && TARGET=1
echo "writing k2=v2 via survivor n${TARGET} (toyraftctl follows the 307 to the current leader)"
deadline=$(( $(now_ns) + 20000000000 ))
wrote=""
while [ "$(now_ns)" -lt "$deadline" ]; do
    if "$BIN_C" -addr "$(client_addr "$TARGET")" set k2 v2 2>/dev/null; then
        wrote=1
        break
    fi
    sleep 0.3
done
[ -n "$wrote" ] || {
    kill -CONT "${PIDS[$LEADER]}" 2>/dev/null || true
    fail "toyraftctl set k2 v2 against new leader failed"
}

# ---- Heal, expect old leader to step down + converge (SC5/DEMO-08) ----------

echo "healing partition (kill -CONT n${LEADER}) — expecting step-down + convergence"
kill -CONT "${PIDS[$LEADER]}" || fail "could not CONT old leader pid ${PIDS[$LEADER]}"

# (a) step-down: the OLD leader must stop reporting role=="leader". Once it is a
# follower again its /status 307-redirects to the current leader (WIRE §5.2), so
# a DIRECT (redirect-free) read of its own /status no longer yields "leader" —
# role_of returns "" (empty) for a demoted node. Asserting role_of != "leader"
# is therefore the redirect-safe step-down signal (a still-leader node answers
# "leader" directly; a follower redirects and yields empty). The convergence
# read below (toyraftctl get, which DOES follow the 307) then proves it rejoined.
deadline=$(( $(now_ns) + 8000000000 ))
stepped=""
while [ "$(now_ns)" -lt "$deadline" ]; do
    if [ "$(role_of "$LEADER")" != "leader" ]; then
        stepped=1
        break
    fi
    sleep 0.2
done
[ -n "$stepped" ] || fail "old leader n${LEADER} did not step down (still reports leader) after heal"
echo "  old leader n${LEADER} stepped down (no longer reports leader)"

# (b) value convergence — REDIRECT-SAFE. The old leader is now a follower and
# 307-redirects a raw GET /kv/k2 (WIRE §5.2), so we NEVER curl its /kv directly.
# toyraftctl follows the 307 from the demoted node to the current leader and
# returns the value — proving the old node rejoined and the cluster converged.
deadline=$(( $(now_ns) + 20000000000 ))
converged=""
while [ "$(now_ns)" -lt "$deadline" ]; do
    got="$(bin/toyraftctl -addr "$(client_addr "$LEADER")" get k2 2>/dev/null || true)"
    if [ "$got" = "v2" ]; then
        converged=1
        break
    fi
    sleep 0.2
done
[ -n "$converged" ] || fail "k2 did not converge to v2 via the demoted old leader (redirect-safe read)"
echo "  k2=v2 converged (read through demoted n${LEADER} via toyraftctl 307-follow)"

echo "SMOKE PASS"
exit 0
