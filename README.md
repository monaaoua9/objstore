# objstore — distributed, fault-tolerant object storage engine

[![ci](https://github.com/monaaoua9/objstore/actions/workflows/ci.yml/badge.svg)](https://github.com/monaaoua9/objstore/actions/workflows/ci.yml) [![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Erasure-coded object storage in Go with a Raft-replicated metadata plane. Standard library only: no
third-party modules, one static binary.

* **Reed-Solomon erasure coding** (own GF(2^8) implementation, Cauchy matrix, any `K` of `K+M` shards
  reconstruct) — ~1.2 GB/s encode on a 2018 laptop CPU.
* **Consistent hashing** with virtual nodes, weights and zone-aware spreading; shard locations are
  recorded in metadata, so topology changes never strand data. Joins and drains rebalance online.
* **Bitrot defence in depth**: SHA-256 per stripe, verified on ingest, on every read, and by a
  rate-limited background scrubber that quarantines bad shards and triggers **self-healing**.
* **Raft** (own implementation: pre-vote, check-quorum, snapshots, WAL, ReadIndex) giving
  linearizable metadata, with the repair and GC queues stored in the replicated state machine.
* **Streaming everywhere**: HTTP/2 (h2c or TLS) from client to gateway to storage nodes; 256 MiB
  moves through the system with < 30 MiB of heap growth, and memory per request is independent of
  object size.

Design write-up with data-flow diagrams, failure matrix and trade-offs: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). The short version of the decisions: [docs/DESIGN.md](docs/DESIGN.md).

## Try it

```sh
make build
make cluster            # 5 local processes: 3 raft voters + 2 storage-only, 3+2 coding (Windows: scripts/dev-cluster.ps1)
```
In another shell:
```sh
export OBJCTL_ENDPOINT=http://127.0.0.1:9001
./bin/objctl mb photos
./bin/objctl put photos holiday.mp4 ./holiday.mp4      # streams from disk
./bin/objctl mput photos raw.mov ./raw.mov 64           # multipart, 64 MiB parts, 4 in flight
./bin/objctl get photos holiday.mp4 /tmp/out.mp4
./bin/objctl -r 0-1048575 get photos holiday.mp4 -      # byte range (flags go before the command)
./bin/objctl status
```
Now `kill -9` two of the `objstored` processes and fetch the file again. It still comes back
byte-identical; after `--repair-delay` the missing shards are rebuilt on the survivors.

Docker: `make docker && docker compose up` gives a 6-node 4+2 cluster over three zones.

## Running a cluster

```sh
objstored --id n1 --listen :9000 --advertise n1.example:9000 --data-dir /var/lib/objstore \
          --zone eu-a --voters n1=n1.example:9000,n2=n2.example:9000,n3=n3.example:9000 \
          -k 4 -m 2
```
Every node gets the same `--voters`. Nodes listed there also run Raft; the rest are storage/gateway-only.
Use 3 or 5 voters. Need at least `k+m` storage nodes (more gives repair somewhere to go).

| flag | default | meaning |
|---|---|---|
| `-k`, `-m` | 4, 2 | data / parity shards. Survives any `m` simultaneous shard losses per object |
| `--shard-size` | 256 KiB | per-shard bytes per stripe (stripe = k × this) |
| `--write-quorum` | k+1 | shards that must be durable before commit |
| `--repair-delay` | 2m | how long a node stays down before its shards are rebuilt elsewhere |
| `--scrub-interval`, `--scrub-rate` | 6h, 64 MiB/s | integrity pass cadence and read throttle |
| `--audit-interval` | 15m | cluster-wide existence/size sweep + rebalance cadence |
| `--zone` | – | failure-domain label; shards are spread across zones |
| `--cluster-secret`, `--api-token` | env | secret for node↔node endpoints; bearer token for the public API |
| `--tls-cert/--tls-key/--tls-ca` | – | HTTPS + HTTP/2 + (optional) peer verification |
| `--max-object-size` | unlimited | reject larger uploads |
| `--join` | off | start as a Raft replica that waits to be added with `objctl addvoter` |
| `--s3-listen`, `--s3-access-key`, `--s3-secret-key`, `--s3-credentials` | – | S3 front end and its keys (see below) |

Operations: `objctl drain <node>` moves all of a node's shards elsewhere, then `objctl rmnode <node>`.
`objctl repairs` shows what is queued for healing. Prometheus metrics on `/metrics`.

**Changing the metadata voters online.** Start the new node with `--join --voters <existing voters>`
(it waits, empty), then `objctl addvoter n4 host:9000` on any voter. The leader streams it the log or
a snapshot first and only then lets it vote. `objctl rmvoter n1` removes one; if it is the leader,
leadership moves. One change at a time (Raft single-server changes); storage-only nodes pick up the
new voter list on their own. `objctl voters` lists the group.

## API (all under `/api/v1`)

| | |
|---|---|
| `PUT /buckets/{b}` · `DELETE /buckets/{b}` · `GET /buckets` | bucket management |
| `PUT /buckets/{b}/objects/{key...}` | streaming upload → `201 {data:{etag,version,size}}` (ETag = SHA-256) |
| `GET`/`HEAD /buckets/{b}/objects/{key...}` | download; `Range`, `If-None-Match`, `If-Match` |
| `DELETE /buckets/{b}/objects/{key...}` | idempotent delete |
| `GET /buckets/{b}/objects?prefix=&cursor=&limit=` | cursor-paginated listing |
| `POST /buckets/{b}/uploads?key=K` → `PUT .../uploads/{id}/parts/{n}` → `POST .../uploads/{id}/complete` `{"parts":[{"part":1,"etag":"…"}]}` | multipart upload (parts 1-10000, any order, retryable; `GET`/`DELETE .../uploads/{id}` list/abort; abandoned uploads expire after 7 days) |
| `PUT /buckets/{b}/versioning` `{"enabled":true}` · `GET .../versions?prefix=&cursor=` · `?version=<id>` on object GET/HEAD/DELETE | versioning: overwrites keep history, a plain DELETE adds a delete marker (`X-Delete-Marker`), deleting a specific version is permanent |
| `PUT\|GET\|DELETE /buckets/{b}/lifecycle` `{"rules":[{"id":"logs","prefix":"logs/","expire_days":30,"noncurrent_days":7,"clean_markers":true}]}` | expiry rules, run by the leader |
| `GET /cluster/voters`, `PUT\|DELETE /cluster/voters/{id}` `{"addr":"host:port"}` | metadata voters |
| `GET /cluster`, `GET /cluster/repair`, `POST /cluster/nodes/{id}/drain\|activate`, `DELETE /cluster/nodes/{id}` | admin |

Responses are `{data, meta}` or `{error:{code,message}}` with stable string codes
(`OBJECT_NOT_FOUND`, `INSUFFICIENT_STORAGE_NODES`, `DATA_UNAVAILABLE`, …). Custom metadata travels as
`X-Meta-*` headers.

## S3 API

Start a node with `--s3-listen :9100 --s3-access-key AK --s3-secret-key SK` (or the
`OBJSTORE_S3_ACCESS_KEY` / `OBJSTORE_S3_SECRET_KEY` env vars) and point any S3 client at it, path-style:

```sh
aws --endpoint-url http://localhost:9100 s3 cp ./big.iso s3://media/big.iso
```

SigV4 (header and presigned), `aws-chunked` streaming uploads, buckets, Put/Get/Head/Delete, `Range`,
CopyObject, DeleteObjects, ListObjectsV2 (+v1) with prefix/delimiter/paging, and the full multipart
flow, bucket versioning (`?versioning`, `?versions`, `versionId=`, delete markers) and lifecycle
(`?lifecycle`: expiration days, noncurrent-version expiration, expired delete markers). Suspending versioning works
(writes then replace the "null" version). Not there:
bucket policies/ACLs, SSE, virtual-host-style addressing, delimiter in
`?versions`. One difference to know about: ETags are SHA-256 hex, not MD5, so clients that insist on
comparing an MD5 ETag will complain.

More than one key: `--s3-credentials keys.json`, a list of
`{"access_key":"..","secret_key":"..(8+ chars)","buckets":["photos"],"read_only":true}`. `buckets`
empty means all; `read_only` blocks every write; a key can only list and copy from buckets it may
read. `kill -HUP` reloads the file without a restart.

## Tests

```sh
make race        # everything, with the race detector (~2 min)
make bench
go run ./cmd/objbench -e http://127.0.0.1:9001 -size 1048576 -n 200 -c 16   # cluster throughput + p50/p95/p99
```

Numbers from a 5-process cluster on one laptop and the erasure codec throughput (about 500 MB/s for 10+4) are in [docs/BENCHMARKS.md](docs/BENCHMARKS.md), with the caveats.
Beyond unit tests for the codec (every erasure pattern for several `k,m`), ring (balance, minimal
movement, zones), WAL (torn tails), frames and the state machine, there are:

* **Raft**: election, ordered replication under concurrency, leader failover with healing partition,
  minority-can't-commit, snapshot catch-up, full-cluster restart from disk, ReadIndex linearizability
  incl. a deposed leader refusing reads.
* **Linearizability under a nemesis** (`TestLinearizableUnderNemesis`): five Raft nodes, concurrent
  writers and ReadIndex readers, while a nemesis isolates nodes and leaders, splits the cluster 2|3 and
  crashes/restarts nodes with snapshots forced every 40 entries. The history is checked against the final
  log: every acknowledged write present exactly once, real-time order respected, every read a prefix that
  includes everything acknowledged before it started and nothing begun after it ended, reads never go
  backwards. `TestVerifyCatchesViolations` feeds the checker bad histories to prove it can fail. The fault
  schedule replays with `RAFT_SEED=<seed>`.
* **Real processes** (`make e2e`, also in CI): five `objstored` processes, write and read back through
  every node, two nodes `kill -9`'d (degraded read), a voter killed (new leader, writes continue), the
  voter restarted from its disk and caught up.
* **Fuzzing** (`go test -fuzz`): frame reader and round trip, aws-chunked decoder, Range parsing, and
  loading damaged WAL/snapshot files.
* **Chaos**: writers and readers run while storage nodes are repeatedly killed and restarted; no read ever returns wrong bytes and every acknowledged write is intact afterwards.
* **Cluster integration** (real HTTP/2 between in-process nodes): ranges/conditionals/paging, GC on
  delete+overwrite, concurrent clients, reads with `m` nodes killed, automatic rebuild after node loss,
  bitrot found by the scrubber and by the read path, deleted shard files found by the audit, Raft leader
  loss, node join → rebalance → drain, writes refused (and no ghost object) without quorum, orphan and
  stale-copy cleanup, whole-cluster restart, bounded-memory 256 MiB streaming.

Prometheus alert rules for the exported metrics (no leader, election churn, node down, failing repairs,
bitrot, stale scrub, degraded writes, error ratios) are in [deploy/alerts.yml](deploy/alerts.yml).

## Layout

```
cmd/objstored   node daemon              cmd/objctl   CLI client
internal/
  sigv4  s3api                                       S3 signatures and S3 REST front end
  erasure  hashring  frame  blobstore  ec          data plane building blocks
  raft  meta  cluster  shardrpc  httpx             consensus, metadata, networking
  engine  gateway  repair  scrub  metrics  app     orchestration and operations
docs/ARCHITECTURE.md
```

## Known limits

Metadata records are spilled to disk on the voters and only an index stays in memory (about 275 bytes per
object version, see the footprint test; snapshots are gzip'd). No bucket policies or SSE. See the
trade-offs section of the architecture doc for the reasoning.
