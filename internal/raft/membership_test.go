package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"testing"
	"time"
)

func (c *cluster) join(id string) {
	c.t.Helper()
	c.sts[id] = NewMemStorage()
	c.ids = append(append([]string(nil), c.ids...), id)
	cfg := c.cfg
	cfg.ID, cfg.Join, cfg.Peers = id, true, nil
	f := &listFSM{}
	c.fsms[id] = f
	n, err := NewNode(cfg, f, c.sts[id], &memTr{c.net, id}, slog.New(slog.NewTextHandler(discard{}, nil)))
	if err != nil {
		c.t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = n
	c.net.mu.Lock()
	c.net.nodes[id] = n
	c.net.mu.Unlock()
}

func (c *cluster) addVoter(via *Node, id string) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		err := via.AddVoter(ctx, id, "mem://"+id)
		if err == nil {
			return
		}
		if !errors.Is(err, ErrConfigBusy) {
			c.t.Fatalf("add %s: %v", id, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (c *cluster) removeVoter(via *Node, id string) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := via.RemoveVoter(ctx, id); err != nil {
		c.t.Fatalf("remove %s: %v", id, err)
	}
}

func peersOf(n *Node) []string {
	p := n.Status().Peers
	slices.Sort(p)
	return p
}

func eventuallyPeers(t *testing.T, n *Node, want ...string) {
	t.Helper()
	slices.Sort(want)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if slices.Equal(peersOf(n), want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s peers = %v, want %v", n.cfg.ID, peersOf(n), want)
}

func TestAddVoter(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader()
	for i := 0; i < 30; i++ {
		c.propose(l, fmt.Sprint("a", i))
	}

	c.join("n3")
	var follower *Node
	for _, id := range c.ids[:3] {
		if c.nodes[id] != l {
			follower = c.nodes[id] // goes through forwarding
			break
		}
	}
	c.addVoter(follower, "n3")

	for _, id := range c.ids {
		eventuallyPeers(t, c.nodes[id], "n0", "n1", "n2", "n3")
	}
	for i := 0; i < 10; i++ {
		c.propose(c.leader(), fmt.Sprint("b", i))
	}
	if got := c.waitConverged(); len(got) != 40 {
		t.Fatalf("converged on %d entries, want 40", len(got))
	}

	// the new voter really counts: with the old leader gone the remaining three (incl. n3) still elect and commit
	old := c.leader()
	c.stop(old.cfg.ID)
	nl := c.leader(old.cfg.ID)
	c.propose(nl, "after-failover")
	if err := waitFor(func() bool { return len(c.fsms["n3"].snapshotVals()) == 41 }); err != nil {
		t.Fatal("n3 never applied the post-failover entry")
	}
}

func waitFor(f func() bool) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("timeout")
}

func TestAddVoterViaSnapshot(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.SnapshotThreshold = 20; cfg.SnapshotTrailing = 5 })
	l := c.leader()
	for i := 0; i < 120; i++ {
		c.propose(l, fmt.Sprint("v", i))
	}
	if l.Status().Base == 0 {
		t.Fatal("leader never compacted")
	}
	c.join("n3")
	c.addVoter(l, "n3")
	if got := c.waitConverged(); len(got) != 120 {
		t.Fatalf("converged on %d entries", len(got))
	}
	eventuallyPeers(t, c.nodes["n3"], "n0", "n1", "n2", "n3")

	// compact again so the snapshot itself has to carry the 4-voter config, then restart a node from it
	for i := 0; i < 60; i++ {
		c.propose(c.leader(), fmt.Sprint("w", i))
	}
	c.waitConverged()
	c.stop("n1")
	c.start("n1") // original 3-voter bootstrap list, same storage
	eventuallyPeers(t, c.nodes["n1"], "n0", "n1", "n2", "n3")
	c.propose(c.leader(), "post-restart")
}

func TestRemoveVoter(t *testing.T) {
	c := newCluster(t, 4, nil)
	l := c.leader()
	for i := 0; i < 10; i++ {
		c.propose(l, fmt.Sprint("a", i))
	}
	victim := "n3"
	if l.cfg.ID == victim {
		victim = "n2"
	}
	c.removeVoter(l, victim)
	for _, id := range c.ids {
		if id == victim {
			continue
		}
		if p := peersOf(c.nodes[id]); slices.Contains(p, victim) || len(p) != 3 {
			eventuallyPeers(t, c.nodes[id], without(c.ids, victim)...)
		}
	}
	term := c.leader().Status().Term
	for i := 0; i < 10; i++ {
		c.propose(c.leader(), fmt.Sprint("b", i))
	}
	time.Sleep(600 * time.Millisecond) // several election timeouts
	if c.leader().Status().Term != term {
		t.Fatal("removed node disturbed the group")
	}
	if got := len(c.fsms[victim].snapshotVals()); got > 11 {
		t.Fatalf("removed node still receives entries (%d)", got)
	}
}

func without(ids []string, drop string) []string {
	var out []string
	for _, id := range ids {
		if id != drop {
			out = append(out, id)
		}
	}
	return out
}

func TestRemoveLeader(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader()
	for i := 0; i < 5; i++ {
		c.propose(l, fmt.Sprint("a", i))
	}
	id := l.cfg.ID
	c.removeVoter(l, id)

	nl := c.leader(id)
	if nl.cfg.ID == id {
		t.Fatal("removed leader is still leader")
	}
	eventuallyPeers(t, nl, without(c.ids, id)...)
	c.propose(nl, "from-new-leader")
	if l.IsLeader() {
		t.Fatal("old leader never stepped down")
	}
	if err := waitFor(func() bool { return len(c.fsms[nl.cfg.ID].snapshotVals()) == 6 }); err != nil {
		t.Fatal("new leader lost entries")
	}
}

func TestMembershipRules(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var err error
	for i := 0; i < 100; i++ { // the new leader's first entry may not have committed yet
		if err = l.RemoveVoter(ctx, "ghost"); !errors.Is(err, ErrConfigBusy) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !errors.Is(err, ErrMembership) {
		t.Fatalf("remove unknown: %v", err)
	}
	other := "n0"
	if l.cfg.ID == other {
		other = "n1"
	}
	if err := l.AddVoter(ctx, other, "x"); !errors.Is(err, ErrMembership) {
		t.Fatalf("add existing: %v", err)
	}
	// the same error must survive forwarding through a follower
	if err := c.nodes[other].AddVoter(ctx, other, "x"); !errors.Is(err, ErrMembership) {
		t.Fatalf("forwarded add existing: %v", err)
	}

	// a joiner that never answers must not wedge the leader or leave a stale learner behind
	short, cancel2 := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel2()
	if err := l.AddVoter(short, "nowhere", "mem://nowhere"); err == nil {
		t.Fatal("added a server that never replied")
	}
	l.mu.Lock()
	stale := len(l.learners)
	l.mu.Unlock()
	if stale != 0 {
		t.Fatalf("%d learners left behind", stale)
	}
	c.propose(l, "still-fine")
}

func TestShrinkToOneAndGrowBack(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader()
	c.propose(l, "x")
	for _, id := range without(c.ids, l.cfg.ID) {
		c.removeVoter(l, id)
	}
	eventuallyPeers(t, l, l.cfg.ID)
	c.propose(l, "solo") // single voter commits alone
	if err := l.RemoveVoter(context.Background(), l.cfg.ID); !errors.Is(err, ErrMembership) {
		t.Fatalf("removing the last voter: %v", err)
	}
}

// snapshots larger than one chunk must arrive intact, including when the follower loses its place
func TestChunkedSnapshotCatchUp(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.SnapshotThreshold = 20; cfg.SnapshotTrailing = 2; cfg.SnapshotChunk = 64 })
	l := c.leader()
	var lag string
	for _, id := range c.ids {
		if c.nodes[id] != l {
			lag = id
			break
		}
	}
	c.net.isolate(lag, c.ids, true)
	for i := 0; i < 150; i++ {
		c.propose(l, fmt.Sprint("value-number-", i))
	}
	if l.Status().Base == 0 {
		t.Fatal("leader never compacted")
	}
	c.net.isolate(lag, c.ids, false)
	if got := c.waitConverged(); len(got) != 150 {
		t.Fatalf("converged on %d entries, want 150", len(got))
	}
}
