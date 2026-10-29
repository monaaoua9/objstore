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
go test -run xxx -bench . -benchmem -count 3 ./internal/erasure
```

Median of three runs, min–max in brackets:

| benchmark | throughput |
|---|---:|
| encode, 10 data + 4 parity | 681 MB/s (603 – 683) |
| reconstruct, 10 data + 4 parity | 676 MB/s (657 – 710) |

Single core of the codec, pure Go. The hash ring package has no benchmarks.

## Cluster

Five `objstored` processes on this one laptop (3 Raft voters + 2 storage-only, K=3, M=2, shard size 64 KiB, one zone per node), over loopback, all sharing four cores and one SSD. The load generator spreads requests over the five nodes with 16 concurrent clients, 200 objects per run. Three runs on one cluster, each size in turn; median with min–max in brackets. 0 failed requests in every run.

```
go build -o bin/objstored ./cmd/objstored && go build -o bin/objbench ./cmd/objbench
# start 5 nodes as in scripts/dev-cluster.sh, then:
bin/objbench -e http://127.0.0.1:9001,...,http://127.0.0.1:9005 -size 1048576 -n 200 -c 16
```

1 MiB objects:

| op | MiB/s | p50 | p99 |
|---|---:|---:|---:|
| write | 32.5 (29.0 – 32.7) | 454 ms (430 – 555) | 764 ms (738 – 980) |
| read | 64.8 (61.3 – 70.9) | 231 ms (213 – 250) | 458 ms (448 – 650) |

64 KiB objects:

| op | ops/s | p50 | p99 |
|---|---:|---:|---:|
| write | 62.2 (58.9 – 62.4) | 253 ms (239 – 263) | 395 ms (379 – 415) |
| read | 97.9 (76.8 – 108.3) | 157 ms (144 – 203) | 277 ms (274 – 407) |

## Reading these numbers

- Five server processes and the load generator compete for four cores, and every write does SHA-256, Reed-Solomon encoding, shard writes to several nodes and a Raft commit. Latencies here say very little about a real cluster, where each node has its own CPU and disk.
- Small objects are slow per byte: a 64 KiB write still pays the whole commit path, so throughput in MiB/s is low while ops/s is only about twice that of 1 MiB writes. That's the "one shard file per (object, shard)" trade-off showing.
- I haven't profiled where write time goes, so I can't say whether the codec, the hashing, fsync or the Raft round trip dominates.
- Deletes are far faster than writes (over 1,000 ops/s in an earlier single run), which fits them being a metadata change rather than a data transfer. I haven't checked exactly when the shard files are removed and didn't repeat that measurement.
- No comparison against MinIO or another store has been run.
- An earlier overlapping run, where another benchmark was using the CPU at the same time, gave far lower numbers (write 14–19 MiB/s). I discarded it; the lesson is that these only mean something when nothing else is running.
