# objstore: design decisions

Why it's shaped this way. Data layout, write/read paths, failure matrix and the honest trade-off list: [ARCHITECTURE.md](ARCHITECTURE.md).

## Scope

A distributed object store with an HTTP API and an S3 front end. Objects are erasure-coded across nodes; metadata is replicated with Raft. The goal was durability and repair behaviour, not feature parity with S3.

## Decisions

**Erasure coding instead of replication.** Objects are cut into stripes of `K × shardSize` and each stripe becomes `K` data + `M` parity shards (Reed-Solomon, Cauchy construction, so any `K` of `K+M` shards rebuild a stripe). Storage overhead is `(K+M)/K` rather than 3×. The cost is repair: rebuilding a shard reads `K` shards over the network. When all data shards are healthy a read does no GF math.

**Every shard is a sequence of frames with its own SHA-256.** `len | SHA-256 | payload`, one frame per stripe. The gateway computes the checksum once and it travels with the data: the receiving node re-verifies before publishing, the scrubber re-verifies at rest, the reader re-verifies on read. A flipped bit is caught at the first stripe containing it, on every path, and never turns into wrong bytes. A bonus: all stripes but the last are full-size, so stripe `s` starts at a computable offset and `Range` requests need no index.

**Consistent hashing decides initial placement only.** 128 virtual points per node, with zone-aware spreading (6 shards over 3 zones gives 2 per zone). The authoritative location of every shard is stored in metadata, so reads never recompute placement and topology changes can't orphan data. A rebalancer moves only shards whose node left the object's ideal set, and waits for topology to be quiet so a flapping node can't make shards ping-pong.

**Metadata in Raft, with records spilled to disk.** The replicated state machine keeps a sorted key index in memory; the records themselves (placement, ETag, user metadata) live in an append-only spill file read through a small cache. About 275 bytes of heap per object version. The spill file is a cache of Raft state, truncated and rebuilt on start. Past tens of millions of objects per voter you'd need an on-disk ordered index or several Raft groups.

**Never serve wrong bytes.** More than `M` holders lost for a stripe returns `503 DATA_UNAVAILABLE`. A node refuses to publish a shard file that doesn't end on a frame boundary. A leader partitioned from the data nodes won't declare the majority dead.

**One shard file per (object version, shard).** Simple, and repair is surgical. It's a lot of small files for small objects; a production system would pack small objects or use a segment store.

**ETags are SHA-256 hex, not MD5.** Clients that insist on comparing an MD5 ETag will complain. I chose to keep one hash through the whole pipeline.

## Checked by

Codec tests for every erasure pattern at several `(K, M)`, ring balance and minimal-movement tests, WAL torn-tail tests, Raft tests (election, failover, snapshot catch-up, full-cluster restart, ReadIndex under a deposed leader), and `TestLinearizableUnderNemesis`: five Raft nodes under partitions, crashes and restarts, with the history checked against the final log. `make e2e` runs five real `objstored` processes.

## Not here

Bucket policies, per-object ACLs, server-side encryption, virtual-host-style S3 addressing. If a majority of voters is permanently lost, recovery is manual.
