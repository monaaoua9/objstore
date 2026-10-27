// Package cluster keeps each process's local picture of the storage nodes: addresses,
// the placement ring, and a short-lived "suspect" list so we stop hammering dead peers.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/monaaoua9/objstore/internal/hashring"
	"github.com/monaaoua9/objstore/internal/meta"
)

var ErrNoCapacity = errors.New("not enough healthy storage nodes for the configured k+m")

type View struct {
	meta *meta.Client
	lg   *slog.Logger

	mu      sync.RWMutex
	nodes   map[string]meta.Node
	ring    *hashring.Ring // up nodes only
	suspect map[string]time.Time
}

const suspectFor = 3 * time.Second

func NewView(mc *meta.Client, lg *slog.Logger) *View {
	return &View{meta: mc, lg: lg, nodes: map[string]meta.Node{}, ring: hashring.Build(nil, 0), suspect: map[string]time.Time{}}
}

func (v *View) Refresh(ctx context.Context) error {
	ns, err := v.meta.Nodes(meta.WithStale(ctx))
	if err != nil {
		return err
	}
	m := make(map[string]meta.Node, len(ns))
	var ringNodes []hashring.Node
	for _, n := range ns {
		m[n.ID] = n
		if n.State == meta.NodeUp {
			ringNodes = append(ringNodes, hashring.Node{ID: n.ID, Zone: n.Zone, Weight: n.Weight})
		}
	}
	ring := hashring.Build(ringNodes, 0)
	v.mu.Lock()
	v.nodes, v.ring = m, ring
	v.mu.Unlock()
	return nil
}

func (v *View) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if err := v.Refresh(ctx); err != nil && ctx.Err() == nil {
			v.lg.Debug("view refresh failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (v *View) Node(id string) (meta.Node, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	n, ok := v.nodes[id]
	return n, ok
}

func (v *View) Addr(id string) (string, bool) {
	n, ok := v.Node(id)
	return n.Addr, ok
}

func (v *View) Nodes() []meta.Node {
	v.mu.RLock()
	defer v.mu.RUnlock()
	out := make([]meta.Node, 0, len(v.nodes))
	for _, n := range v.nodes {
		out = append(out, n)
	}
	return out
}

func (v *View) Ring() *hashring.Ring {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.ring
}

// Suspect marks a node as probably unreachable for a few seconds. Cheap local circuit breaker.
func (v *View) Suspect(id string) {
	v.mu.Lock()
	v.suspect[id] = time.Now().Add(suspectFor)
	v.mu.Unlock()
}

func (v *View) Healthy(id string) {
	v.mu.Lock()
	delete(v.suspect, id)
	v.mu.Unlock()
}

func (v *View) IsSuspect(id string) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return time.Now().Before(v.suspect[id])
}

// Usable: registered, not down, not suspected. Draining nodes still serve reads.
func (v *View) Usable(id string) bool {
	n, ok := v.Node(id)
	return ok && n.State != meta.NodeDown && !v.IsSuspect(id)
}

// Place picks n distinct nodes for a new object version. Suspected nodes are avoided while
// there are enough alternatives; if not, we fall back to them rather than refusing the write.
func (v *View) Place(version string, n int) ([]string, error) {
	ring := v.Ring()
	skip := map[string]bool{}
	v.mu.RLock()
	now := time.Now()
	for id, until := range v.suspect {
		if now.Before(until) {
			skip[id] = true
		}
	}
	v.mu.RUnlock()

	got := ring.LocateExcluding(version, n, skip)
	if len(got) < n {
		got = ring.Locate(version, n)
	}
	if len(got) < n {
		return nil, fmt.Errorf("%w: need %d, have %d", ErrNoCapacity, n, len(got))
	}
	return got, nil
}
