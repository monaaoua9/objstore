// Package scrub is the per-node integrity auditor: it re-reads every local shard, verifies each
// frame's SHA-256, quarantines rot and tells the metadata plane so the leader can rebuild it.
package scrub

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/monaaoua9/objstore/internal/blobstore"
	"github.com/monaaoua9/objstore/internal/frame"
	"github.com/monaaoua9/objstore/internal/meta"
	"github.com/monaaoua9/objstore/internal/metrics"
)

type Config struct {
	Interval    time.Duration // pause between passes
	RateBytes   int64         // read throttle, bytes/sec; 0 = unthrottled
	OrphanAge   time.Duration // unreferenced shards younger than this are left alone (in-flight uploads)
	QuarantineT time.Duration // how long quarantined files are kept for forensics
}

func (c *Config) setDefaults() {
	if c.Interval == 0 {
		c.Interval = 6 * time.Hour
	}
	if c.OrphanAge == 0 {
		c.OrphanAge = time.Hour
	}
	if c.QuarantineT == 0 {
		c.QuarantineT = 7 * 24 * time.Hour
	}
}

type Scrubber struct {
	NodeID string
	Store  *blobstore.Store
	Meta   *meta.Client
	Cfg    Config
	Log    *slog.Logger

	limiter *blobstore.Limiter

	scanned, corrupt, orphans, bytes *metrics.Counter
	lastPass                         *metrics.Gauge
}

func New(nodeID string, st *blobstore.Store, mc *meta.Client, cfg Config, reg *metrics.Registry, lg *slog.Logger) *Scrubber {
	cfg.setDefaults()
	return &Scrubber{
		NodeID: nodeID, Store: st, Meta: mc, Cfg: cfg, Log: lg.With("component", "scrub"),
		limiter:  blobstore.NewLimiter(cfg.RateBytes),
		scanned:  reg.Counter("objstore_scrub_shards_total", "Shards verified by the scrubber."),
		corrupt:  reg.Counter("objstore_scrub_corrupt_total", "Shards the scrubber found damaged."),
		orphans:  reg.Counter("objstore_scrub_orphans_removed_total", "Unreferenced shard files deleted."),
		bytes:    reg.Counter("objstore_scrub_bytes_total", "Bytes read while scrubbing."),
		lastPass: reg.Gauge("objstore_scrub_last_pass_unix", "Unix time the last scrub pass finished."),
	}
}

func (s *Scrubber) Run(ctx context.Context) {
	t := time.NewTimer(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		s.Pass(ctx)
		t.Reset(s.Cfg.Interval)
	}
}

type Result struct {
	Checked, Corrupt, Orphans int
}

func (s *Scrubber) Pass(ctx context.Context) Result {
	var res Result
	var entries []blobstore.Entry
	if err := s.Store.Walk(func(e blobstore.Entry) error {
		entries = append(entries, e)
		return ctx.Err()
	}); err != nil {
		s.Log.Warn("scrub walk failed", "err", err)
		return res
	}

	const batch = 256
	for lo := 0; lo < len(entries); lo += batch {
		hi := min(lo+batch, len(entries))
		live, err := s.placements(ctx, entries[lo:hi])
		if err != nil {
			s.Log.Warn("scrub paused: metadata unavailable", "err", err)
			return res
		}
		for _, e := range entries[lo:hi] {
			if ctx.Err() != nil {
				return res
			}
			// not referenced at all, or referenced but we are no longer the holder (moved / rebuilt elsewhere
			// while we were down). Either way it's dead weight, but give in-flight uploads and moves time to land.
			o := live[e.Version]
			if o == nil || e.Index >= len(o.Shards) || o.Shards[e.Index].Node != s.NodeID {
				if time.Since(e.ModTime) > s.Cfg.OrphanAge {
					if s.Store.Delete(e.Version, e.Index) == nil {
						res.Orphans++
						s.orphans.Inc()
					}
				}
				continue
			}
			res.Checked++
			s.scanned.Inc()
			s.bytes.Add(uint64(e.Size))
			_, err := s.Store.Verify(e.Version, e.Index, s.limiter)
			switch {
			case err == nil:
			case errors.Is(err, blobstore.ErrNotFound):
				// deleted under us by GC
			case errors.Is(err, frame.ErrCorrupt), errors.Is(err, io.ErrUnexpectedEOF):
				res.Corrupt++
				s.corrupt.Inc()
				s.Log.Warn("bitrot detected", "version", e.Version, "shard", e.Index, "err", err)
				s.Store.Quarantine(e.Version, e.Index)
				if err := s.Meta.MarkDamagedOn(ctx, e.Version, e.Index, s.NodeID, "scrub-bitrot"); err != nil {
					s.Log.Error("could not report corrupt shard", "version", e.Version, "shard", e.Index, "err", err)
				}
			default:
				s.Log.Warn("scrub read error", "version", e.Version, "shard", e.Index, "err", err)
			}
		}
	}
	s.Store.Sweep(time.Hour, s.Cfg.QuarantineT)
	s.lastPass.Set(time.Now().Unix())
	s.Log.Info("scrub pass done", "checked", res.Checked, "corrupt", res.Corrupt, "orphans", res.Orphans)
	return res
}

func (s *Scrubber) placements(ctx context.Context, es []blobstore.Entry) (map[string]*meta.Object, error) {
	seen := map[string]bool{}
	var vs []string
	for _, e := range es {
		if !seen[e.Version] {
			seen[e.Version] = true
			vs = append(vs, e.Version)
		}
	}
	return s.Meta.Placements(ctx, vs)
}
