package repair

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/monaaoua9/objstore/internal/ec"
	"github.com/monaaoua9/objstore/internal/meta"
	"github.com/monaaoua9/objstore/internal/shardrpc"
)

// sweepLoop walks every object slowly, checking that each shard still exists with the right size
// (cheap HEADs; the per-node scrubber does the expensive checksum pass) and that placement still
// matches the ring.
func (s *Supervisor) sweepLoop(ctx context.Context) {
	// first pass shortly after startup so a fresh leader catches up fast
	next := time.NewTimer(min(5*time.Second, s.Cfg.SweepEvery))
	defer next.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-next.C:
		}
		if s.IsLeader() {
			s.sweepOnce(ctx)
		}
		next.Reset(s.Cfg.SweepEvery)
	}
}

func (s *Supervisor) sweepOnce(ctx context.Context) {
	cursor := ""
	var checked, flagged, moves int
	for {
		if !s.IsLeader() || ctx.Err() != nil {
			return
		}
		objs, next, err := s.Meta.Scan(ctx, cursor, s.Cfg.SweepBatch)
		if err != nil {
			s.Log.Debug("sweep scan failed", "err", err)
			return
		}
		for _, o := range objs {
			f, m := s.sweepObject(ctx, o)
			flagged += f
			moves += m
			checked++
		}
		if next == "" {
			break
		}
		cursor = next
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.Cfg.SweepPause):
		}
	}
	if flagged > 0 || moves > 0 {
		s.Log.Info("sweep finished", "objects", checked, "flagged", flagged, "moved", moves)
	}
}

func (s *Supervisor) sweepObject(ctx context.Context, o *meta.Object) (flagged, moved int) {
	lay := ec.LayoutOf(o)
	want := lay.ShardFileSize()

	for i, sh := range o.Shards {
		n, known := s.View.Node(sh.Node)
		switch {
		case !known:
			s.flag(ctx, o, i, "node-removed")
			flagged++
		case n.State == meta.NodeDown:
			if time.Since(n.Since) >= s.Cfg.RepairDelay {
				s.flag(ctx, o, i, "node-lost")
				flagged++
			}
		default:
			c, cancel := context.WithTimeout(ctx, 5*time.Second)
			size, err := s.RPC.Head(c, n.Addr, o.Version, i)
			cancel()
			switch {
			case errors.Is(err, shardrpc.ErrNotFound):
				s.flag(ctx, o, i, "audit-missing")
				flagged++
			case err == nil && size != want:
				s.flag(ctx, o, i, "audit-size")
				flagged++
			}
			// any other error is a connectivity problem; the prober owns that decision
		}
	}
	if flagged == 0 && !s.Cfg.DisableBalance {
		moved = s.rebalance(ctx, o)
	}
	return
}

func (s *Supervisor) flag(ctx context.Context, o *meta.Object, idx int, reason string) {
	s.auditFlagged.Inc()
	if err := s.Meta.MarkDamaged(ctx, o.Version, idx, reason); err != nil {
		s.Log.Debug("mark damaged failed", "err", err)
	}
}

// rebalance moves shards that sit on nodes the ring no longer wants (draining, or a new node
// took over their slot). Waits for topology to be quiet so a flapping node doesn't cause churn.
func (s *Supervisor) rebalance(ctx context.Context, o *meta.Object) int {
	s.mu.Lock()
	quiet := time.Since(s.lastChange) >= s.Cfg.RepairDelay/2
	s.mu.Unlock()
	if !quiet {
		return 0
	}

	ideal := s.View.Ring().Locate(o.Version, len(o.Shards))
	if len(ideal) < len(o.Shards) {
		return 0
	}
	want := map[string]bool{}
	for _, id := range ideal {
		want[id] = true
	}
	have := map[string]bool{}
	for _, sh := range o.Shards {
		have[sh.Node] = true
	}
	var free []string // wanted nodes that hold none of our shards yet
	for _, id := range ideal {
		if !have[id] {
			free = append(free, id)
		}
	}
	sort.Strings(free)

	moved := 0
	for _, sh := range o.Shards {
		if want[sh.Node] || len(free) == 0 {
			continue
		}
		dst := free[0]
		free = free[1:]
		if err := s.moveShard(ctx, o, sh.Index, sh.Node, dst); err != nil {
			s.Log.Warn("rebalance move failed", "version", o.Version, "shard", sh.Index, "from", sh.Node, "to", dst, "err", err)
			continue
		}
		moved++
	}
	return moved
}

// moveShard copies the framed bytes verbatim (the destination re-verifies every checksum on the way in).
func (s *Supervisor) moveShard(ctx context.Context, o *meta.Object, idx int, from, to string) error {
	fn, ok1 := s.View.Node(from)
	tn, ok2 := s.View.Node(to)
	if !ok1 || !ok2 {
		return fmt.Errorf("unknown node")
	}
	if fn.State == meta.NodeDown {
		return fmt.Errorf("source %s is down; repair path will handle it", from)
	}
	rc, err := s.RPC.Get(ctx, fn.Addr, o.Version, idx, 0)
	if err != nil {
		return err
	}
	defer rc.Close()
	ps := s.RPC.OpenPut(ctx, tn.Addr, o.Version, idx)
	if _, err := io.Copy(ps, rc); err != nil {
		ps.Abort(err)
		return err
	}
	if err := ps.Close(); err != nil {
		return err
	}
	if err := s.Meta.Relocate(ctx, o.Version, idx, from, to, false); err != nil {
		s.RPC.Delete(ctx, tn.Addr, o.Version, idx)
		if errors.Is(err, meta.ErrStalePlacement) {
			return nil
		}
		return err
	}
	s.RPC.Delete(ctx, fn.Addr, o.Version, idx)
	s.moved.Inc()
	s.Log.Info("shard moved", "bucket", o.Bucket, "key", o.Key, "shard", idx, "from", from, "to", to)
	return nil
}
