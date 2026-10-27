package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/monaaoua9/objstore/internal/app"
	"github.com/monaaoua9/objstore/internal/repair"
	"github.com/monaaoua9/objstore/internal/s3api"
	"github.com/monaaoua9/objstore/internal/scrub"
)

func main() {
	var (
		id        = flag.String("id", "", "unique node id (required)")
		listen    = flag.String("listen", ":9000", "bind address for API, shard and raft traffic")
		advertise = flag.String("advertise", "", "host:port other nodes should use to reach this one (default: listen address)")
		dataDir   = flag.String("data-dir", "./data", "directory for shards and raft state")
		zone      = flag.String("zone", "", "failure domain label (rack/AZ); shards are spread across zones")
		voters    = flag.String("voters", "", "metadata raft voters: id=host:port,id=host:port,... (odd count, 3 or 5)")

		join = flag.Bool("join", false, "run as a metadata replica that waits to be added (objctl addvoter); --voters lists existing voters")

		k         = flag.Int("k", 4, "data shards per stripe")
		m         = flag.Int("m", 2, "parity shards per stripe")
		shardSize = flag.Int("shard-size", 256<<10, "bytes per shard per stripe")
		quorum    = flag.Int("write-quorum", 0, "shards that must be durable before commit (default k+1)")
		maxObj    = flag.Int64("max-object-size", 0, "reject uploads larger than this many bytes (0 = unlimited)")

		secret  = flag.String("cluster-secret", os.Getenv("OBJSTORE_CLUSTER_SECRET"), "shared secret guarding node-to-node endpoints (env OBJSTORE_CLUSTER_SECRET)")
		token   = flag.String("api-token", os.Getenv("OBJSTORE_API_TOKEN"), "bearer token required on the public API (env OBJSTORE_API_TOKEN)")
		s3Addr  = flag.String("s3-listen", "", "serve the S3 API on this address (needs --s3-access-key/--s3-secret-key)")
		s3AK    = flag.String("s3-access-key", os.Getenv("OBJSTORE_S3_ACCESS_KEY"), "S3 access key id (env OBJSTORE_S3_ACCESS_KEY)")
		s3SK    = flag.String("s3-secret-key", os.Getenv("OBJSTORE_S3_SECRET_KEY"), "S3 secret key (env OBJSTORE_S3_SECRET_KEY)")
		s3Creds = flag.String("s3-credentials", "", "JSON file with more S3 keys, each optionally limited to buckets and/or read-only; reloaded on SIGHUP")
		s3Reg   = flag.String("s3-region", "us-east-1", "region name clients sign with")
		cert    = flag.String("tls-cert", "", "TLS certificate (enables HTTPS/HTTP2 everywhere)")
		key     = flag.String("tls-key", "", "TLS private key")
		ca      = flag.String("tls-ca", "", "CA bundle for verifying peer certificates")

		scrubEvery = flag.Duration("scrub-interval", 6*time.Hour, "pause between integrity passes over local shards")
		scrubRate  = flag.Int64("scrub-rate", 64<<20, "scrub read throttle in bytes/sec (0 = unlimited)")
		repairWait = flag.Duration("repair-delay", 2*time.Minute, "how long a node must stay down before its shards are rebuilt elsewhere")
		sweepEvery = flag.Duration("audit-interval", 15*time.Minute, "pause between cluster-wide anti-entropy sweeps")
		noBalance  = flag.Bool("no-rebalance", false, "never move healthy shards to match the ring")

		logLevel  = flag.String("log-level", "info", "debug|info|warn|error")
		logFormat = flag.String("log-format", "text", "text|json")
	)
	flag.Parse()

	if *id == "" {
		fatal("--id is required")
	}
	vm, err := parseVoters(*voters)
	if err != nil {
		fatal(err.Error())
	}
	if len(vm) == 0 {
		fatal("--voters is required (even for a single-node dev cluster: --voters n1=localhost:9000)")
	}

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		fatal("bad --log-level")
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler = slog.NewTextHandler(os.Stderr, opts)
	if *logFormat == "json" {
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	lg := slog.New(h)
	slog.SetDefault(lg)

	var s3Keys map[string]s3api.Key
	loadKeys := func() (map[string]s3api.Key, error) {
		keys := map[string]s3api.Key{}
		if *s3AK != "" && *s3SK != "" {
			keys[*s3AK] = s3api.Key{Secret: *s3SK}
		}
		if *s3Creds != "" {
			more, err := s3api.LoadKeys(*s3Creds)
			if err != nil {
				return nil, err
			}
			for k, v := range more {
				keys[k] = v
			}
		}
		return keys, nil
	}
	if *s3Addr != "" {
		var err error
		if s3Keys, err = loadKeys(); err != nil {
			fatal(err.Error())
		}
		if len(s3Keys) == 0 {
			fatal("--s3-listen needs --s3-access-key/--s3-secret-key or --s3-credentials")
		}
	}

	n, err := app.Start(app.Config{
		ID: *id, Listen: *listen, Advertise: *advertise, DataDir: *dataDir, Zone: *zone, Voters: vm, Join: *join,
		K: *k, M: *m, ShardSize: *shardSize, WriteQuorum: *quorum, MaxObjectSize: *maxObj,
		ClusterSecret: *secret, APIToken: *token, TLSCert: *cert, TLSKey: *key, TLSCA: *ca,
		S3Listen: *s3Addr, S3Keys: s3Keys, S3Region: *s3Reg,
		Repair: repair.Config{RepairDelay: *repairWait, SweepEvery: *sweepEvery, DisableBalance: *noBalance},
		Scrub:  scrub.Config{Interval: *scrubEvery, RateBytes: *scrubRate},
		Logger: lg,
	})
	if err != nil {
		lg.Error("startup failed", "err", err)
		os.Exit(1)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	for {
		s := <-sig
		if s == syscall.SIGHUP {
			if *s3Addr == "" {
				continue
			}
			keys, err := loadKeys()
			if err != nil || len(keys) == 0 {
				lg.Error("s3 credentials reload failed, keeping the old set", "err", err)
				continue
			}
			n.SetS3Keys(keys)
			lg.Info("s3 credentials reloaded", "keys", len(keys))
			continue
		}
		lg.Info("shutting down", "signal", s.String())
		n.Stop()
		return
	}
}

func parseVoters(s string) (map[string]string, error) {
	out := map[string]string{}
	if s == "" {
		return out, nil
	}
	for _, part := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("bad voter %q, want id=host:port", part)
		}
		out[id] = addr
	}
	return out, nil
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "objstored:", msg)
	os.Exit(2)
}
