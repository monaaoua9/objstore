// Package hashring is a consistent hash ring with virtual nodes and optional zone spreading.
package hashring

import (
	"sort"
	"strconv"
)

const DefaultVnodes = 128

type Node struct {
	ID     string
	Zone   string // empty == no zone preference
	Weight int    // multiplier on vnode count, 0 means 1
}

type point struct {
	h    uint64
	node int
}

// Ring is immutable once built; swap the pointer to change topology.
type Ring struct {
	nodes  []Node
	points []point
}

func Build(nodes []Node, vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = DefaultVnodes
	}
	r := &Ring{nodes: append([]Node(nil), nodes...)}
	sort.Slice(r.nodes, func(i, j int) bool { return r.nodes[i].ID < r.nodes[j].ID })
	for ni, n := range r.nodes {
		w := n.Weight
		if w <= 0 {
			w = 1
		}
		for v := 0; v < vnodes*w; v++ {
			r.points = append(r.points, point{h: hash(n.ID + "#" + strconv.Itoa(v)), node: ni})
		}
	}
	sort.Slice(r.points, func(i, j int) bool {
		if r.points[i].h == r.points[j].h {
			return r.points[i].node < r.points[j].node
		}
		return r.points[i].h < r.points[j].h
	})
	return r
}

func (r *Ring) Size() int { return len(r.nodes) }

func (r *Ring) Nodes() []Node { return append([]Node(nil), r.nodes...) }

// Locate returns up to n distinct node IDs for key, clockwise from the key's position.
// Nodes in unseen zones win first, so a 6-shard object over 3 zones puts 2 shards per zone
// instead of whatever the ring happens to hand out.
func (r *Ring) Locate(key string, n int) []string {
	return r.LocateExcluding(key, n, nil)
}

func (r *Ring) LocateExcluding(key string, n int, skip map[string]bool) []string {
	if len(r.points) == 0 || n <= 0 {
		return nil
	}
	start := sort.Search(len(r.points), func(i int) bool { return r.points[i].h >= hash(key) })

	picked := make([]string, 0, n)
	used := make([]bool, len(r.nodes))
	zoneSeen := map[string]int{}
	hasZones := false
	for _, nd := range r.nodes {
		if nd.Zone != "" {
			hasZones = true
			break
		}
	}

	// round 0 only takes a node whose zone is the least-used so far; later rounds relax that.
	for round := 0; round < 2 && len(picked) < n; round++ {
		if round == 1 && !hasZones {
			break
		}
		for i := 0; i < len(r.points) && len(picked) < n; i++ {
			p := r.points[(start+i)%len(r.points)]
			nd := r.nodes[p.node]
			if used[p.node] || skip[nd.ID] {
				continue
			}
			if round == 0 && hasZones && nd.Zone != "" && zoneSeen[nd.Zone] > minZoneCount(zoneSeen, r.nodes, used, skip) {
				continue
			}
			used[p.node] = true
			zoneSeen[nd.Zone]++
			picked = append(picked, nd.ID)
		}
		if !hasZones {
			break
		}
	}
	return picked
}

// smallest usage count among zones that still have a free node
func minZoneCount(seen map[string]int, nodes []Node, used []bool, skip map[string]bool) int {
	best := -1
	for i, nd := range nodes {
		if used[i] || skip[nd.ID] {
			continue
		}
		c := seen[nd.Zone]
		if best == -1 || c < best {
			best = c
		}
	}
	if best == -1 {
		return 0
	}
	return best
}

// fnv-1a then murmur3's finalizer; plain fnv clusters badly on short similar strings
func hash(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}
