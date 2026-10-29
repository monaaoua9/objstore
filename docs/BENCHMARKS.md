# objstore: benchmarks

Measured 2026-10-29.

| | |
|---|---|
| CPU | Intel i5-8365U, 4 cores / 8 threads, 1.6 GHz base |
| RAM / disk | 8 GB, SSD |
| OS | Windows 11 |
| Toolchain | Go 1.27.0 |

## Erasure coding

```
go test -run xxx -bench . -benchmem ./internal/erasure
```

| benchmark | throughput |
|---|---:|
| encode, 10 data + 4 parity | 507 MB/s |
| reconstruct, 10 data + 4 parity | 509 MB/s |

Single core of the codec, pure Go. The hash ring package has no benchmarks.

## Cluster

Five `objstored` processes on this one laptop (3 Raft voters + 2 storage-only, K=3, M=2, shard size 64 KiB, one zone per node), over loopback, all sharing four cores and one SSD. Load generator spreads requests over the five nodes with 16 concurrent clients, 200 objects per run.

```
go build -o bin/objstored ./cmd/objstored && go build -o bin/objbench ./cmd/objbench
# start 5 nodes as in scripts/dev-cluster.sh, then:
bin/objbench -e http://127.0.0.1:9001,...,http://127.0.0.1:9005 -size 1048576 -n 200 -c 16
```

1 MiB objects:

| op | throughput | ops/s | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|
| write | 39.3 MiB/s | 39.3 | 404 ms | 510 ms | 564 ms |
| read | 75.8 MiB/s | 75.8 | 198 ms | 354 ms | 426 ms |
| delete | n/a | 1,339 | 11.5 ms | 15.8 ms | 18.4 ms |

64 KiB objects:

| op | throughput | ops/s | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|
| write | 3.7 MiB/s | 58.5 | 258 ms | 404 ms | 431 ms |
| read | 6.0 MiB/s | 95.8 | 160 ms | 253 ms | 320 ms |
| delete | n/a | 1,135 | 13.8 ms | 19.4 ms | 21.6 ms |

0 failed requests in all runs. One run each.

## Reading these numbers

- Five server processes and the load generator compete for four cores, and every write does SHA-256, Reed-Solomon encoding, shard writes to several nodes and a Raft commit. Latencies here say very little about a real cluster, where each node has its own CPU and disk.
- Small objects are slow per byte: a 64 KiB write still pays the whole commit path, so throughput in MiB/s is low while ops/s is only a bit higher. That's the "one shard file per (object, shard)" trade-off showing.
- Deletes are two orders of magnitude faster than writes, which fits them being a metadata change rather than a data transfer. I haven't checked exactly when the shard files are removed.
- I haven't profiled where write time goes, so I can't say whether the codec, the hashing, fsync or the Raft round trip dominates.
- No comparison against MinIO or another store has been run.
