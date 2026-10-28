#!/usr/bin/env bash
# Real-process smoke test: five objstored processes over loopback, k=3 m=2, then failures.
# Needs ./bin (make build). Exits non-zero on the first thing that does not hold.
set -euo pipefail
cd "$(dirname "$0")/.."
BIN=${BIN:-./bin}
DATA=$(mktemp -d)
VOTERS="n1=127.0.0.1:9601,n2=127.0.0.1:9602,n3=127.0.0.1:9603"
declare -A pid

cleanup() {
  for p in "${pid[@]}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$DATA"
}
trap cleanup EXIT

start() {
  local i=$1
  "$BIN/objstored" --id "n$i" --listen "127.0.0.1:960$i" --data-dir "$DATA/n$i" --zone "rack$i" \
    --voters "$VOTERS" -k 3 -m 2 --shard-size 65536 --repair-delay 3s --audit-interval 1m \
    >"$DATA/n$i.log" 2>&1 &
  pid[$i]=$!
}

ctl() { "$BIN/objctl" -e "http://127.0.0.1:960${EP:-1}" "$@"; }
fail() { echo "FAIL: $*" >&2; tail -n 20 "$DATA"/n*.log >&2 || true; exit 1; }

# retries cover failure detection and leader election windows
retry() {
  local n=$1; shift
  for _ in $(seq "$n"); do "$@" >/dev/null 2>&1 && return 0; sleep 1; done
  return 1
}

for i in 1 2 3 4 5; do start "$i"; done
retry 30 ctl status || fail "cluster did not come up"
retry 30 ctl mb smoke || fail "create bucket"

head -c 3000000 /dev/urandom >"$DATA/obj1"
want=$(sha256sum <"$DATA/obj1" | cut -d' ' -f1)
retry 15 ctl put smoke obj1 "$DATA/obj1" || fail "put"
for ep in 1 2 3 4 5; do
  got=$(EP=$ep ctl get smoke obj1 - | sha256sum | cut -d' ' -f1)
  [ "$got" = "$want" ] || fail "read via n$ep returned different bytes"
done
echo "ok: write once, read back from every node"

# two storage nodes gone: still readable (m=2), via a survivor
kill -9 "${pid[4]}" "${pid[5]}"; unset 'pid[4]' 'pid[5]'
got=$(EP=2 ctl get smoke obj1 - | sha256sum | cut -d' ' -f1)
[ "$got" = "$want" ] || fail "degraded read returned different bytes"
echo "ok: degraded read with 2 of 5 nodes down"

# a voter gone: the other two elect a leader and keep serving writes
kill -9 "${pid[1]}"; unset 'pid[1]'
head -c 1000000 /dev/urandom >"$DATA/obj2"
want2=$(sha256sum <"$DATA/obj2" | cut -d' ' -f1)
export EP=2
# five shards need five holders, so bring the storage nodes back before writing
start 4; start 5
retry 40 ctl put smoke obj2 "$DATA/obj2" || fail "write after voter loss"
got=$(ctl get smoke obj2 - | sha256sum | cut -d' ' -f1)
[ "$got" = "$want2" ] || fail "read after voter loss returned different bytes"
echo "ok: leader loss, new leader, write and read"

# the first voter returns from its disk and catches up
start 1
retry 40 ctl status || fail "restarted voter not serving"
got=$(EP=1 ctl get smoke obj2 - | sha256sum | cut -d' ' -f1)
[ "$got" = "$want2" ] || fail "restarted voter cannot read the new object"
echo "ok: restarted voter caught up"
echo "e2e passed"
