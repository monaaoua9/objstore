// Package repair is the background brain of the cluster. Only the Raft leader runs it:
// failure detection, rebuilding damaged shards, anti-entropy audits, rebalancing after
// topology changes, and garbage collection of deleted versions.
package repair

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/monaaoua9/objstore/internal/cluster"
	"github.com/monaaoua9/objstore/internal/ec"
	"github.com/monaaoua9/objstore/internal/engine"
	"github.com/monaaoua9/objstore/internal/meta"
	"github.com/monaaoua9/objstore/internal/metrics"
	"github.com/monaaoua9/objstore/internal/shardrpc"
)

type Config struct {
	ProbeInterval  time.Duration // how often we ping every node
	DownAfter      int           // consecutive failed probes before a node is declared down
	RepairDelay    time.Duration // grace before a down node's shards are rebuilt elsewhere
	RepairEvery    time.Duration
	Workers        int
	SweepEvery     time.Duration // pause between full anti-entropy passes
	SweepBatch     int
	SweepPause     time.Duration // pause between batches inside a pass
	GCEvery        time.Duration
	DisableBalance bool
	UploadTTL      time.Duration // abandoned multipart uploads are aborted after this long

	LifecycleEvery time.Duration // pause between lifecycle passes
	DayLength      time.Duration // what a lifecycle "day" lasts; only tests change it
	LifecycleBatch int           // most deletions one bucket gets per pass
}

func (c *Config) setDefaults() {
	def := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	def(&c.ProbeInterval, time.Second)
	def(&c.RepairDelay, time.Minute)
	def(&c.RepairEvery, 2*time.Second)
	def(&c.SweepEvery, 10*time.Minute)
	def(&c.SweepPause, 100*time.Millisecond)
	def(&c.GCEvery, 5*time.Second)
	def(&c.UploadTTL, 7*24*time.Hour)
	def(&c.LifecycleEvery, time.Hour)
	def(&c.DayLength, 24*time.Hour)
	if c.LifecycleBatch == 0 {
		c.LifecycleBatch = 5000
	}
	if c.DownAfter == 0 {
		c.DownAfter = 5
	}
	if c.Workers == 0 {
		c.Workers = 4
	}
	if c.SweepBatch == 0 {
		c.SweepBatch = 100
	}
}

type Supervisor struct {
	Meta     *meta.Client
	View     *cluster.View
	RPC      *shardrpc.Client
	Eng      *engine.Engine
	IsLeader func() bool
	Cfg      Config
	Log      *slog.Logger

	repaired, repairFail, moved, gcDeleted, auditFlagged, nodesDown *metrics.Counter
	repairBytes, lcDeleted                                          *metrics.Counter

	mu          sync.Mutex
	probeFails  map[string]int
	retryAt     map[string]time.Time
	retryDelay  map[string]time.Duration
	lastChange  time.Time // last time any node changed state, used to hold off rebalancing
	knownStates map[string]meta.NodeState
}

func New(mc *meta.Client, v *cluster.View, rpc *shardrpc.Client, eng *engine.Engine, isLeader func() bool, cfg Config, reg *metrics.Registry, lg *slog.Logger) *Supervisor {
	cfg.setDefaults()
	s := &Supervisor{
		Meta: mc, View: v, RPC: rpc, Eng: eng, IsLeader: isLeader, Cfg: cfg, Log: lg.With("component", "repair"),
		probeFails: map[string]int{}, retryAt: map[string]time.Time{}, retryDelay: map[string]time.Duration{},
		knownStates: map[string]meta.NodeState{}, lastChange: time.Now(),
	}
	s.repaired = reg.Counter("objstore_repair_shards_total", "Shards rebuilt by the repair workers.")
	s.repairFail = reg.Counter("objstore_repair_failures_total", "Repair attempts that failed and will be retried.")
	s.repairBytes = reg.Counter("objstore_repair_bytes_total", "Shard bytes written by repairs.")
	s.moved = reg.Counter("objstore_rebalance_moves_total", "Shards moved to match the placement ring.")
	s.gcDeleted = reg.Counter("objstore_gc_shards_deleted_total", "Shard files removed for deleted/overwritten objects.")
	s.auditFlagged = reg.Counter("objstore_audit_flagged_total", "Shards the anti-entropy audit found missing or the wrong size.")
	s.lcDeleted = reg.Counter("objstore_lifecycle_expired_total", "Objects and versions removed by lifecycle rules.")
	s.nodesDown = reg.Counter("objstore_node_down_events_total", "Times a node was declared down.")
	return s
}

func (s *Supervisor) Run(ctx context.Context) {
	var wg sync.WaitGroup
	loop := func(every time.Duration, fn func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(every)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if s.IsLeader() {
						fn(ctx)
					}
				}
			}
		}()
	}
	loop(s.Cfg.ProbeInterval, s.probe)
	loop(s.Cfg.RepairEvery, s.repairBatch)
	loop(s.Cfg.GCEvery, s.gcBatch)
	loop(s.Cfg.LifecycleEvery, s.lifecycle)

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.sweepLoop(ctx)
	}()
	wg.Wait()
}

// ---- failure detection ----

func (s *Supervisor) probe(ctx context.Context) {
	nodes := s.View.Nodes()
	if len(nodes) == 0 {
		return
	}
	type outcome struct {
		n  meta.Node
		ok bool
	}
	res := make(chan outcome, len(nodes))
	for _, n := range nodes {
		go func() {
			c, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			_, err := s.RPC.Info(c, n.Addr)
			res <- outcome{n, err == nil}
		}()
	}
	var outs []outcome
	failed := 0
	for range nodes {
		o := <-res
		outs = append(outs, o)
		if !o.ok {
			failed++
		}
	}
	// if most of the cluster looks dead the problem is probably *our* network; don't wreck metadata over it
	trustFailures := failed*2 < len(nodes) || len(nodes) == 1

	changed := false
	for _, o := range outs {
		id := o.n.ID
		s.mu.Lock()
		if o.ok {
			s.probeFails[id] = 0
		} else {
			s.probeFails[id]++
		}
		fails := s.probeFails[id]
		s.mu.Unlock()

		switch {
		case o.ok && o.n.State == meta.NodeDown:
			s.Log.Info("node is back", "node", id)
			if s.Meta.SetNodeState(ctx, id, meta.NodeUp) == nil {
				changed = true
			}
		case !o.ok && fails >= s.Cfg.DownAfter && o.n.State == meta.NodeUp && trustFailures:
			s.Log.Warn("node declared down", "node", id, "failed_probes", fails)
			if s.Meta.SetNodeState(ctx, id, meta.NodeDown) == nil {
				s.nodesDown.Inc()
				changed = true
			}
		}
		s.mu.Lock()
		if prev, ok := s.knownStates[id]; ok && prev != o.n.State {
			s.lastChange = time.Now()
		}
		s.knownStates[id] = o.n.State
		s.mu.Unlock()
	}
	if changed {
		s.mu.Lock()
		s.lastChange = time.Now()
		s.mu.Unlock()
		s.View.Refresh(ctx)
	}
}

// ---- repair ----

func (s *Supervisor) repairBatch(ctx context.Context) {
	ds, err := s.Meta.Damaged(ctx, s.Cfg.Workers*8)
	if err != nil || len(ds) == 0 {
		return
	}
	sem := make(chan struct{}, s.Cfg.Workers)
	var wg sync.WaitGroup
	now := time.Now()
	for _, d := range ds {
		key := fmt.Sprintf("%s/%d", d.Version, d.Index)
		s.mu.Lock()
		wait := now.Before(s.retryAt[key])
		s.mu.Unlock()
		if wait {
			continue
		}
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			err := s.RepairShard(ctx, d)
			s.mu.Lock()
			defer s.mu.Unlock()
			if err == nil {
				delete(s.retryAt, key)
				delete(s.retryDelay, key)
				return
			}
			s.repairFail.Inc()
			dl := s.retryDelay[key] * 2
			if dl == 0 {
				dl = 2 * time.Second
			}
			dl = min(dl, 5*time.Minute)
			s.retryDelay[key], s.retryAt[key] = dl, time.Now().Add(dl)
			s.Log.Warn("repair failed", "version", d.Version, "shard", d.Index, "reason", d.Reason, "retry_in", dl, "err", err)
		}()
	}
	wg.Wait()
}

// RepairShard rebuilds one damaged shard from the survivors and repoints metadata at it.
func (s *Supervisor) RepairShard(ctx context.Context, d meta.Damage) error {
	obj, err := s.Meta.GetVersion(ctx, d.Version)
	if errors.Is(err, meta.ErrObjectNotFound) {
		return nil // deleted since; the FSM already dropped the damage record
	}
	if err != nil {
		return err
	}
	idx := d.Index
	lay := ec.LayoutOf(obj)
	cur := obj.Shards[idx].Node
	curNode, curKnown := s.View.Node(cur)
	curAlive := curKnown && curNode.State == meta.NodeUp && !s.View.IsSuspect(cur)

	// reads can report transient failures as damage; don't rebuild a shard that is actually fine
	if curAlive {
		addr := curNode.Addr
		vctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		vr, verr := s.RPC.Verify(vctx, addr, obj.Version, idx)
		cancel()
		if verr == nil && vr.OK {
			if sz, herr := s.RPC.Head(ctx, addr, obj.Version, idx); herr == nil && sz == lay.ShardFileSize() {
				s.Log.Info("damage report was a false alarm", "version", obj.Version, "shard", idx)
				return s.Meta.Relocate(ctx, obj.Version, idx, cur, cur, true)
			}
		}
	}

	target := cur
	if !curAlive {
		taken := map[string]bool{}
		for _, sh := range obj.Shards {
			taken[sh.Node] = true
		}
		cands := s.View.Ring().LocateExcluding(obj.Version, 1, taken)
		if len(cands) == 0 {
			return fmt.Errorf("no spare node to rebuild shard %d (current holder %s is unavailable)", idx, cur)
		}
		target = cands[0]
	}
	tn, ok := s.View.Node(target)
	if !ok {
		return fmt.Errorf("target node %s unknown", target)
	}

	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ps := s.RPC.OpenPut(rctx, tn.Addr, obj.Version, idx)
	cw := &countWriter{w: ps}
	dec, err := ec.Rebuild(rctx, lay, s.Eng.ShardOpener(obj), idx, cw)
	if err != nil {
		ps.Abort(err)
		return fmt.Errorf("rebuild: %w", err)
	}
	if err := ps.Close(); err != nil {
		return fmt.Errorf("write rebuilt shard to %s: %w", target, err)
	}
	if err := s.Meta.Relocate(ctx, obj.Version, idx, cur, target, true); err != nil {
		if errors.Is(err, meta.ErrStalePlacement) {
			// somebody else (rebalancer, overwrite) changed this shard; our copy is surplus
			s.RPC.Delete(ctx, tn.Addr, obj.Version, idx)
			return nil
		}
		return err
	}
	if target != cur && curAlive {
		s.RPC.Delete(ctx, curNode.Addr, obj.Version, idx)
	}
	s.repaired.Inc()
	s.repairBytes.Add(uint64(cw.n))
	s.Log.Info("shard repaired", "version", obj.Version, "bucket", obj.Bucket, "key", obj.Key,
		"shard", idx, "from", cur, "to", target, "reason", d.Reason, "bad_sources", len(dec.Bad()))
	return nil
}

type countWriter struct {
	w io.Writer
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// ---- garbage collection ----

func (s *Supervisor) gcBatch(ctx context.Context) {
	s.expireUploads(ctx)
	items, err := s.Meta.GCQueue(ctx, 100)
	if err != nil {
		return
	}
	for _, it := range items {
		done := true
		for _, sh := range it.Shards {
			n, known := s.View.Node(sh.Node)
			if !known {
				continue // node was decommissioned, nothing left to delete
			}
			c, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := s.RPC.Delete(c, n.Addr, it.Version, sh.Index)
			cancel()
			if err != nil {
				done = false
				continue
			}
			s.gcDeleted.Inc()
		}
		if done {
			if err := s.Meta.GCDone(ctx, it.Version); err != nil {
				s.Log.Debug("gc ack failed", "err", err)
			}
		}
	}
}

func (s *Supervisor) expireUploads(ctx context.Context) {
	ups, err := s.Meta.ListUploads(ctx)
	if err != nil {
		return
	}
	for _, u := range ups {
		if time.Since(u.Created) > s.Cfg.UploadTTL {
			if err := s.Meta.AbortUpload(ctx, u.ID); err == nil {
				s.Log.Info("expired abandoned multipart upload", "upload", u.ID, "bucket", u.Bucket, "key", u.Key)
			}
		}
	}
}
