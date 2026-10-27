# Architecture

An object store that survives disk, node and rack failures without a single point of coordination.
Bytes live on storage nodes as Reed-Solomon shards; *where* those bytes live is a small, strongly
consistent database replicated with Raft.

```
                      ┌───────────────────────── control plane ─────────────────────────┐
                      │   n1 (voter)        n2 (voter)        n3 (voter)                │
   metadata           │   ┌────────┐ raft  ┌────────┐ raft  ┌────────┐                  │
   buckets, object →  │   │  FSM   │◄─────►│  FSM   │◄─────►│  FSM   │   linearizable   │
   shard placement,   │   └────────┘       └────────┘       └────────┘   reads (ReadIndex)
   node registry,     └──────▲─────────────────▲─────────────────▲──────────────────────┘
   repair + gc queues        │ commit/lookup   │                 │
                      ┌──────┴─────────────────┴─────────────────┴──────────────────────┐
   client ──HTTP/2──► │  gateway (any node)  →  engine: stripe, encode, fan-out, decode │
                      └──────┬──────────┬──────────┬──────────┬──────────┬──────────┬───┘
                             │ framed shard streams over HTTP/2 (one per shard index)  │
                         ┌───▼──┐   ┌───▼──┐   ┌───▼──┐   ┌───▼──┐   ┌───▼──┐   ┌───▼──┐
   data plane            │ n1   │   │ n2   │   │ n3   │   │ n4   │   │ n5   │   │ n6   │
                         │blobs │   │blobs │   │blobs │   │blobs │   │blobs │   │blobs │
                         └──────┘   └──────┘   └──────┘   └──────┘   └──────┘   └──────┘
                          every node: shard store + scrubber;  voters also run raft;
                          the raft leader additionally runs the repair supervisor
```

Every process is the same binary. Whether it votes in Raft is decided by `--voters`; every node is a
storage node and serves the public API, so clients can talk to any of them.

## Packages

| package | role |
|---|---|
| `internal/erasure` | GF(2^8) arithmetic, Cauchy-matrix Reed-Solomon, parallel encode/reconstruct, decode-matrix cache |
| `internal/hashring` | consistent hash ring: virtual nodes, weights, zone-aware spreading |
| `internal/frame` | per-stripe frame format `[len u32][sha256][payload]` shared by disk and wire |
| `internal/blobstore` | per-node shard files: validate-on-write, atomic commit, verify, quarantine |
| `internal/ec` | the streaming pipeline: `Stream` (read → stripe → encode → fan-out) and `Decoder` (K lanes → stripes, with live failover to parity) |
| `internal/raft` | Raft: election with pre-vote + check-quorum, replication, snapshots, WAL, ReadIndex |
| `internal/meta` | the replicated state machine and its typed client |
| `internal/shardrpc` | node-to-node shard protocol (streaming PUT/GET/HEAD/DELETE/verify) |
| `internal/engine` | PUT/GET/DELETE orchestration (no HTTP) |
| `internal/gateway` | public REST API: ranges, conditionals, paging, admin |
| `internal/sigv4` | AWS Signature V4: header + presigned verification, `aws-chunked` decoding with the signature chain |
| `internal/s3api` | optional S3 REST front end (own listener) on top of the same engine and metadata client |
| `internal/repair` | leader-only supervisor: failure detection, rebuild, audit, rebalance, GC |
| `internal/scrub` | per-node integrity scrubber |
| `internal/cluster` | each process's view of the nodes: ring, suspect list |
| `internal/app` | wires a node together; the integration tests live here |

## Data layout: stripes, shards, frames

An object is cut into **stripes** of `K × shardSize` bytes (default 4 × 256 KiB = 1 MiB). Each stripe
is encoded into `K` data + `M` parity shards. Shard *i* of every stripe is appended to **one file
per (object version, shard index)** on one node. Each stripe's shard is stored as a **frame**:

```
┌──────────┬──────────────────────┬─────────────────────┐
│ len  u32 │ SHA-256(payload) 32B │ payload (≤ shardSize)│   ← one per stripe
└──────────┴──────────────────────┴─────────────────────┘
```

Consequences worth knowing:

* The checksum is computed once, by the gateway, and travels with the data. The receiving node
  re-verifies it before the shard is published, the scrubber re-verifies it at rest, and the reader
  re-verifies it again. A flipped bit is therefore caught at the first stripe that contains it, on
  every path, and never turns into silently wrong bytes.
* Every stripe except the last is full, so stripe *s* starts at `s × (36 + shardSize)` in every
  shard file. That gives O(1) seeking for HTTP `Range` requests without any index.
* The last stripe is zero-padded to a multiple of `K`; the true object size lives in metadata.
* The Cauchy construction makes `[I; C]` MDS: *any* `K` of the `K+M` shards reconstruct the stripe.
  Reading with all data shards healthy costs zero GF math, which is the common case.

## Write path

```mermaid
sequenceDiagram
  participant C as Client
  participant G as Gateway/engine
  participant R as Raft (meta)
  participant N as Nodes n1..n(k+m)
  C->>G: PUT /buckets/b/objects/k (streaming body)
  G->>R: bucket exists? (linearizable read)
  G->>G: version = random 128-bit; place k+m distinct nodes via ring
  G->>N: open k+m streaming PUTs (HTTP/2, one per shard index)
  loop every stripe (bounded memory)
    C-->>G: next 1 MiB
    G->>G: SHA-256 (ETag), RS-encode, frame each shard
    G-->>N: shard i → node i (backpressure via small bounded queues)
  end
  G->>N: end of body → each node fsyncs and renames tmp → final
  G->>R: CommitObject{placement, size, etag, degraded=[failed shard idx]}
  R-->>G: committed (quorum)
  G-->>C: 201 {etag, version}
```

* **Atomicity.** Shard files become visible only on a clean end of stream (fsync + rename). The object
  becomes visible only when the Raft commit applies. A crash anywhere in between leaves invisible
  garbage that the orphan sweep removes; there is no state in which a reader can see half an object.
* **Write quorum.** Default `K+1` shards must be durable. If a node dies mid-upload the stripe stream
  keeps going to the others; the object is committed *degraded* and the failed indexes go straight
  into the repair queue in the same Raft entry. Below quorum the upload aborts and shards are deleted.
* **Memory.** A stripe buffer is `(K+M) × shardSize`, pooled and reference-counted across the fan-out
  workers; each shard has a 2-deep queue. Resident memory per upload is a few MiB regardless of
  object size (the test suite pushes 256 MiB through five in-process nodes with < 30 MiB of heap growth).
* **Overwrite** commits the new version and moves the old version's placement into a GC queue inside
  the same state-machine transition, so a crash cannot leak or prematurely delete it.

## Read path

* One `Decoder` lane per shard prefetches frames (2-deep) from its node. The decoder starts with the
  `K` **data** lanes, so a healthy read is a straight concatenation.
* If a lane fails (node down, shard missing, checksum mismatch, short file), that lane is dropped, a
  parity lane is opened **at the current stripe**, and the stripe is reconstructed. The client sees
  one uninterrupted body. Failures after the first stripe work too (tested with mid-stream bitrot).
* The first stripe is fetched *before* the HTTP status line is written, so "unrecoverable" is a clean
  `503`, not a truncated `200`.
* Every bad shard the reader ran into is reported to the repair queue (**read-repair**), except
  plain network errors, which the failure detector owns.
* `Range` maps to a start stripe, then trims the first/last chunk.

## Placement: consistent hashing

`hashring.Build(upNodes)` places 128 virtual points per node (× weight) using FNV-1a with a Murmur3
finalizer. `Locate(versionID, K+M)` walks clockwise collecting distinct nodes; when nodes carry
`--zone` labels the walk first prefers zones with the fewest shards so far (6 shards over 3 zones
→ 2 per zone). Tests assert balance within ±20%, minimal movement when a node joins (<15% of keys
move for 10→11 nodes), determinism across processes, and zone spread.

The ring decides **initial** placement. The authoritative location of every shard is recorded in
metadata, so placement never needs to be recomputed to read data, and topology changes cannot
orphan anything. When the topology changes the *rebalancer* diffs recorded placement against the
ring and moves only the shards whose node is no longer in the object's ideal set, pairing each with
a free ideal node. Rebalancing waits for topology to be quiet (no node state change for
`repair-delay / 2`) so a flapping node cannot make shards ping-pong.

## Metadata: Raft

The replicated state machine (`meta.FSM`) holds buckets, objects (sorted per bucket for O(log n)
paged listing), the node registry, the **damaged-shard queue**, and the **GC queue**. Putting the
queues in the FSM means a new leader inherits all pending repair work; nothing is in leader memory.

The Raft implementation (`internal/raft`) covers:

* leader election with randomized timeouts, **pre-vote** (a partitioned node rejoining doesn't bump
  terms) and **check-quorum** (a leader that can't reach a majority steps down);
* log replication with fast conflict backtracking, batching, and per-peer replicators;
* **snapshots** + log compaction, and `InstallSnapshot` for lagging followers, sent in 1 MiB chunks the follower acknowledges by offset (a lost or replayed chunk resumes from where it left off, no giant single message; both ends still hold the whole snapshot in memory);
* a CRC-framed **WAL** with torn-tail repair, atomic state/snapshot files, fsync before acknowledging;
* **linearizable reads** via ReadIndex: the leader records its commit index, proves leadership with
  a quorum round *after* the read began (sequence numbers, not timestamps), then the reader waits
  for `applied ≥ index`. Followers obtain the read index from the leader and serve reads locally.
* **group commit**: proposals queue on the leader and one goroutine appends everything that piled up
  with a single fsync, so concurrent writers share the cost instead of serialising on it. Measured with
  `cmd/objbench` on a laptop where all five nodes share one SSD: deletes (Raft only) went from ~140 to
  ~1200 ops/s, 4 KiB writes from ~17 to ~42 ops/s. Large writes stay bound by the five shard-file
  fsyncs per object, not by Raft; separate disks per node would move that number a lot.
* proposals from any node forward to the leader; `ErrLostLeader` marks the "outcome unknown" case.

*Storage* membership is dynamic and lives in the FSM, so adding/removing/draining storage nodes needs no
Raft reconfiguration. Storage-only nodes reach metadata through any voter (`meta.HTTPBackend`).

### Changing the voter set

Voters change one at a time (single-server changes, Raft thesis §4). A config is a log entry
(`EntryConfig`) holding the *whole* voter set with addresses, and it takes effect on each node when
the entry is **appended**, not committed. Consequences, all handled in `membership.go`:

* The leader accepts a change only after it committed an entry of its own term and the previous
  config entry committed (`ErrConfigBusy` otherwise), so two overlapping majorities can't exist.
* `AddVoter` first replicates to the newcomer as a **learner** (not counted in quorum, not in
  check-quorum) until it has answered and is within a few entries of the tail, so a blank disk
  can't stall commits while it copies a snapshot. Only then is the config entry proposed.
* A log suffix being truncated can contain a config, so the active config is recomputed from
  *latest config entry in the log → snapshot's `Peers` → bootstrap list*. Snapshots carry the
  config in force at their index; compaction never loses it.
* A joiner (`--join`) has no voters and never campaigns until a leader gives it a config. Requests
  for votes from nodes outside the current config are ignored, so a removed node coming back can't
  bump terms. A leader that removed itself steps down once the entry commits.
* Replicators are started and retired dynamically; the HTTP transport learns addresses from config
  entries. Storage-only nodes poll `/internal/meta/voters` and re-point their metadata client. They can only learn the new set from a voter they already know, so when replacing every voter, wait for one poll interval (`--voter-refresh`) before stopping the old ones; a removed voter still answers with the last set it saw.

The FSM is deterministic: timestamps are stamped by the proposer, never read inside `Apply`.

## Multipart upload

Each part is an ordinary erasure-coded object version, staged under a hidden `~mpu` bucket as
`<uploadID>/<part#>` (user bucket names cannot start with `~`, and the gateway 404s the namespace).
So parts get everything for free: streaming, quorum writes, scrubbing, audit, repair, rebalance.
`complete` is a single Raft transition that checks the listed `(part, etag)` pairs, builds a
**manifest** object (no shards of its own, just `[]PartRef`; ETag = `mp-<sha256 of part etags>-N`),
removes the parts from the staging namespace *without* retiring them, retires every unselected part,
and replaces any previous object at that key. Deleting or overwriting the manifest retires its parts
recursively. Reads walk the parts in order and reuse the single-object path (degraded reads, read-repair
and `Range` all work per part). Abort and the 7-day expiry (leader GC loop) retire all staged parts.
The audit scan lists a manifest's parts alongside it, since parts are no longer in the staging bucket.

## S3 front end

`--s3-listen` starts a second listener whose handler is `s3api.Server`. Every request is verified
before anything else: the canonical request is rebuilt from the wire (path re-encoded the AWS way,
query sorted, signed headers only), compared in constant time, and the timestamp must be within 15
minutes. Bodies are then unwrapped once, centrally: `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` is decoded
chunk by chunk, each chunk's signature chained to the previous one, so a truncated or edited stream
fails before the engine commits the object. A plain signed payload hash is checked as the body streams
and the mismatch surfaces at EOF, which makes `Put` abort and nothing is committed. Handlers map onto
the same `engine` and `meta.Client` calls the REST gateway uses; multipart reuses the staging scheme
above. Listings fold common prefixes while walking the sorted key space, and the continuation token is
the last key or prefix returned. ETags stay SHA-256, so they are not MD5-compatible.

## Versioning and lifecycle

Every PUT already created a fresh internal version (the shard-file identity), so object versions
fall out of that. In a bucket with versioning on, the FSM keeps `objs[key]` as the newest entry and
`hist[key]` as older ones (newest first). Overwrite pushes the old entry down instead of retiring
it; plain delete pushes a **delete marker** (no shards) on top; deleting a specific version removes
that entry, retires its shards for GC, and promotes the next entry if it was current. Older versions
stay in the live-placement index, so audit, scrub, repair and rebalance keep protecting them.
Listings hide markers; `list_versions` pages by `(key, version)`. Versioning can be suspended but
never fully switched off: history stays. While suspended, writes and deletes act on the single
"null" version (`Object.Null`, addressable as `versionId=null`) and leave older versions alone.

Lifecycle rules live on the bucket but are **evaluated by the leader's supervisor**, not the FSM,
because the FSM may not read a clock. The supervisor plans from listings (expire current versions
older than N days; delete versions N days after their successor appeared; drop a delete marker that
is all that's left), then issues ordinary deletes. Everything after that, including GC of shard
files, is the usual path, which also makes passes idempotent and safe to repeat after a failover.

## Background machinery (leader only unless noted)

| loop | what it does |
|---|---|
| **probe** | pings every node each second; `DownAfter` consecutive failures → `down` (and back to `up` on recovery). If a majority of probes fail simultaneously it assumes *its own* network is broken and changes nothing. |
| **repair** | pulls the damaged queue, per-shard exponential retry. For each: confirm it's really damaged (verify + size), pick a target (same node if alive, otherwise next ring node not already holding a shard of the object), stream a rebuild from `K` healthy shards straight into the target, then `Relocate` in Raft (compare-and-set on the old holder). |
| **sweep / anti-entropy** | slowly scans all objects: HEAD every shard (exists, exact expected size); shards on removed nodes or nodes down longer than `repair-delay` are flagged; then runs the rebalance diff. |
| **gc** | deletes shard files of retired versions on their nodes; acks to Raft only when every node confirmed. |
| **scrubber** (every node) | rate-limited full read of local shards verifying each frame's SHA-256; corrupt shards are **quarantined** (not deleted) and reported with the node id, so a stale copy on a former holder can't trigger a bogus repair. Also removes orphans (files metadata has never heard of, older than 1 h) and old quarantine files. |

## Failure matrix

| failure | behaviour |
|---|---|
| ≤ M shard holders unreachable | reads continue (parity lanes); writes continue if ≥ write-quorum nodes remain; repair after `repair-delay` |
| bit flip on disk | caught by SHA-256 on read (served from parity, repair queued) or by the scrubber (quarantine + repair) |
| shard file deleted | next anti-entropy sweep notices via HEAD, rebuilds it |
| truncated / torn upload | node refuses to publish a file that doesn't end on a frame boundary |
| gateway dies mid-upload | tmp files vanish with the connection; nothing committed |
| gateway dies after shards, before commit | orphan shards removed by the scrubber after 1 h |
| raft leader dies | new election; metadata reads/writes resume; background loops start on the new leader |
| leader partitioned from followers | check-quorum steps it down; ReadIndex can't get a quorum so it never serves stale reads |
| leader partitioned from *data nodes* | probe refuses to declare the majority dead |
| node returns after being declared down | marked `up`; shards already rebuilt elsewhere are not touched, stale copies are deleted by GC/orphan sweep |
| whole-cluster power cycle | WAL + snapshot recovery; tested |
| more than M holders lost for one stripe | `503 DATA_UNAVAILABLE`; never wrong bytes |

## Trade-offs, honestly

* **Metadata lives in RAM** on each voter (snapshotted to disk, gzip'd, strings interned). Measured
  by `TestMetadataFootprint`: roughly 650 bytes of heap per object version with 6 shards, i.e. ~650 MB
  per million objects, snapshot a few tens of MB. Fine up to a few million objects per voter; beyond
  that you want a disk-backed index (snapshots now travel in chunks, but are still assembled in memory) or to shard the keyspace across several Raft groups.
* **One shard file per (object, shard)** keeps the model simple and repair surgical, but it is a lot of
  small files for small objects. A production system would pack small objects or use an LSM/segment store.
* **Streaming a rebuild reads `K` shards over the network** (the classical RS repair cost); LRC or
  regenerating codes would cut that.
* Auth: the REST API takes one bearer token, the S3 API takes any number of SigV4 keys that can be
  limited to buckets and/or read-only. There are no bucket policies, per-object ACLs or SSE.
  Membership changes need a healthy majority; if a majority is permanently lost, recovery is manual
  (restore a voter from backup, or start a new single-voter group).

## Observability

`/metrics` (Prometheus text) on every node: request counts/latency, put/get bytes, degraded
reads/writes, read-repair hints, repair/rebalance/gc counters, scrub progress, per-node shard/byte
gauges, raft term/leader/applied index. `/healthz` and `/readyz` for orchestrators. Structured
`slog` logs (`--log-format json`).
