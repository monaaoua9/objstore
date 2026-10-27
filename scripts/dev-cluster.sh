#!/usr/bin/env bash
# Boots a 5-node local cluster (3 raft voters + 2 storage-only), k=3 m=2. Ctrl-C stops everything.
set -euo pipefail
cd "$(dirname "$0")/.."
BIN=${BIN:-./bin}
DATA=${DATA:-./.dev-data}
VOTERS="n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003"

mkdir -p "$DATA"
pids=()
trap 'kill "${pids[@]}" 2>/dev/null || true' EXIT INT TERM

for i in 1 2 3 4 5; do
  "$BIN/objstored" --id "n$i" --listen "127.0.0.1:900$i" --data-dir "$DATA/n$i" --zone "rack$i" \
    --voters "$VOTERS" -k 3 -m 2 --shard-size 65536 --repair-delay 20s --audit-interval 1m \
    --s3-listen "127.0.0.1:910$i" --s3-access-key devkey --s3-secret-key devsecret123 &
  pids+=($!)
done
echo "cluster up: objctl -e http://127.0.0.1:9001 status   (S3: 127.0.0.1:9101, devkey/devsecret123)"
wait
