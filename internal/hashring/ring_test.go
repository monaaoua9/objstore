package hashring

import (
	"fmt"
	"testing"
)

func mk(n int) []Node {
	out := make([]Node, n)
	for i := range out {
		out[i] = Node{ID: fmt.Sprintf("n%02d", i)}
	}
	return out
}

func TestDistinct(t *testing.T) {
	r := Build(mk(8), 0)
	for i := 0; i < 500; i++ {
		got := r.Locate(fmt.Sprintf("obj-%d", i), 6)
		seen := map[string]bool{}
		for _, id := range got {
			if seen[id] {
				t.Fatalf("dup in %v", got)
			}
			seen[id] = true
		}
		if len(got) != 6 {
			t.Fatalf("want 6 got %d", len(got))
		}
	}
	if got := r.Locate("x", 20); len(got) != 8 {
		t.Fatalf("should cap at node count, got %d", len(got))
	}
}

func TestBalance(t *testing.T) {
	r := Build(mk(10), 0)
	cnt := map[string]int{}
	const N = 20000
	for i := 0; i < N; i++ {
		for _, id := range r.Locate(fmt.Sprintf("k%d", i), 3) {
			cnt[id]++
		}
	}
	ideal := float64(N*3) / 10
	for id, c := range cnt {
		if d := float64(c) / ideal; d < 0.8 || d > 1.2 {
			t.Errorf("%s holds %.2fx its share", id, d)
		}
	}
}

// adding one node to ten should move roughly 1/11th of primary placements, not everything
func TestMinimalMovement(t *testing.T) {
	a := Build(mk(10), 0)
	b := Build(mk(11), 0)
	moved := 0
	const N = 10000
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("k%d", i)
		if a.Locate(k, 1)[0] != b.Locate(k, 1)[0] {
			moved++
		}
	}
	frac := float64(moved) / N
	if frac > 0.15 {
		t.Fatalf("moved %.1f%% of keys", frac*100)
	}
}

func TestDeterministic(t *testing.T) {
	a := Build(mk(5), 64)
	b := Build(mk(5), 64)
	for i := 0; i < 100; i++ {
		k := fmt.Sprint(i)
		x, y := a.Locate(k, 3), b.Locate(k, 3)
		for j := range x {
			if x[j] != y[j] {
				t.Fatal("rings disagree")
			}
		}
	}
}

func TestZoneSpread(t *testing.T) {
	var ns []Node
	for z := 0; z < 3; z++ {
		for i := 0; i < 3; i++ {
			ns = append(ns, Node{ID: fmt.Sprintf("z%dn%d", z, i), Zone: fmt.Sprintf("z%d", z)})
		}
	}
	r := Build(ns, 0)
	zone := map[string]string{}
	for _, n := range ns {
		zone[n.ID] = n.Zone
	}
	for i := 0; i < 300; i++ {
		per := map[string]int{}
		for _, id := range r.Locate(fmt.Sprint("o", i), 6) {
			per[zone[id]]++
		}
		for z, c := range per {
			if c != 2 {
				t.Fatalf("zone %s got %d shards: %v", z, c, per)
			}
		}
	}
}

func TestExclude(t *testing.T) {
	r := Build(mk(6), 0)
	got := r.LocateExcluding("k", 3, map[string]bool{"n00": true, "n01": true})
	for _, id := range got {
		if id == "n00" || id == "n01" {
			t.Fatal("excluded node returned")
		}
	}
	if len(got) != 3 {
		t.Fatal("short")
	}
}
