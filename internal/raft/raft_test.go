package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// ---- in-memory network ----

type memNet struct {
	mu    sync.Mutex
	nodes map[string]*Node
	cut   map[[2]string]bool
}

func newMemNet() *memNet {
	return &memNet{nodes: map[string]*Node{}, cut: map[[2]string]bool{}}
}

func (m *memNet) isolate(id string, peers []string, on bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range peers {
		if p == id {
			continue
		}
		m.cut[[2]string{id, p}] = on
		m.cut[[2]string{p, id}] = on
	}
}

func (m *memNet) target(from, to string) (*Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cut[[2]string{from, to}] {
		return nil, errors.New("partitioned")
	}
	n := m.nodes[to]
	if n == nil {
		return nil, errors.New("down")
	}
	return n, nil
}

type memTr struct {
	net  *memNet
	self string
}

func roundtrip[T any](in *T) *T {
	b, _ := json.Marshal(in)
	var out T
	json.Unmarshal(b, &out)
	return &out
}

func (t *memTr) AppendEntries(ctx context.Context, to string, r *AppendEntriesReq) (*AppendEntriesResp, error) {
	n, err := t.net.target(t.self, to)
	if err != nil {
		return nil, err
	}
	return roundtrip(n.HandleAppendEntries(roundtrip(r))), nil
}

func (t *memTr) RequestVote(ctx context.Context, to string, r *VoteReq) (*VoteResp, error) {
	n, err := t.net.target(t.self, to)
	if err != nil {
		return nil, err
	}
	return roundtrip(n.HandleVote(roundtrip(r))), nil
}

func (t *memTr) InstallSnapshot(ctx context.Context, to string, r *InstallSnapshotReq) (*InstallSnapshotResp, error) {
	n, err := t.net.target(t.self, to)
	if err != nil {
		return nil, err
	}
	return roundtrip(n.HandleInstallSnapshot(roundtrip(r))), nil
}

func (t *memTr) ReadIndex(ctx context.Context, to string) (uint64, error) {
	n, err := t.net.target(t.self, to)
	if err != nil {
		return 0, err
	}
	return n.HandleReadIndex(ctx)
}

func (t *memTr) Forward(ctx context.Context, to string, data []byte) ([]byte, error) {
	n, err := t.net.target(t.self, to)
	if err != nil {
		return nil, err
	}
	return n.HandleForward(ctx, data)
}

// ---- test FSM: an append-only list of commands ----

type listFSM struct {
	mu   sync.Mutex
	vals []string
}

func (f *listFSM) Apply(idx uint64, d []byte) []byte {
	f.mu.Lock()
	f.vals = append(f.vals, string(d))
	n := len(f.vals)
	f.mu.Unlock()
	return []byte(fmt.Sprint(n))
}

func (f *listFSM) Snapshot() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return json.Marshal(f.vals)
}

func (f *listFSM) Restore(b []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.vals = nil
	return json.Unmarshal(b, &f.vals)
}

func (f *listFSM) snapshotVals() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.vals...)
}

// ---- cluster harness ----

type cluster struct {
	t     *testing.T
	net   *memNet
	ids   []string
	nodes map[string]*Node
	fsms  map[string]*listFSM
	sts   map[string]Storage
	cfg   Config
}

func newCluster(t *testing.T, n int, tweak func(*Config)) *cluster {
	t.Helper()
	c := &cluster{t: t, net: newMemNet(), nodes: map[string]*Node{}, fsms: map[string]*listFSM{}, sts: map[string]Storage{}}
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	c.cfg = Config{Peers: c.ids, HeartbeatInterval: 15 * time.Millisecond, ElectionTimeout: 120 * time.Millisecond}
	if tweak != nil {
		tweak(&c.cfg)
	}
	for _, id := range c.ids {
		c.sts[id] = NewMemStorage()
		c.start(id)
	}
	t.Cleanup(func() {
		for _, id := range c.ids {
			c.stop(id)
		}
	})
	return c
}

func (c *cluster) start(id string) {
	c.t.Helper()
	cfg := c.cfg
	cfg.ID = id
	f := &listFSM{}
	c.fsms[id] = f
	lg := slog.New(slog.NewTextHandler(discard{}, nil))
	n, err := NewNode(cfg, f, c.sts[id], &memTr{c.net, id}, lg)
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

func (c *cluster) stop(id string) {
	c.net.mu.Lock()
	delete(c.net.nodes, id)
	c.net.mu.Unlock()
	if n := c.nodes[id]; n != nil {
		n.Stop()
		c.nodes[id] = nil
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

func (c *cluster) leader(exclude ...string) *Node {
	c.t.Helper()
	skip := map[string]bool{}
	for _, e := range exclude {
		skip[e] = true
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var found *Node
		cnt := 0
		for _, id := range c.ids {
			n := c.nodes[id]
			if n == nil || skip[id] {
				continue
			}
			if n.IsLeader() {
				found = n
				cnt++
			}
		}
		if cnt == 1 {
			return found
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no stable leader")
	return nil
}

func (c *cluster) propose(n *Node, s string) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := n.Propose(ctx, []byte(s)); err != nil {
		c.t.Fatalf("propose %q: %v", s, err)
	}
}

func (c *cluster) waitConverged(ids ...string) []string {
	c.t.Helper()
	if len(ids) == 0 {
		ids = c.ids
	}
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		var ref []string
		ok := true
		for i, id := range ids {
			if c.nodes[id] == nil {
				continue
			}
			v := c.fsms[id].snapshotVals()
			if i == 0 {
				ref = v
				continue
			}
			if fmt.Sprint(v) != fmt.Sprint(ref) {
				ok = false
			}
		}
		if ok {
			return ref
		}
		time.Sleep(15 * time.Millisecond)
	}
	for _, id := range ids {
		c.t.Logf("%s: %d vals", id, len(c.fsms[id].snapshotVals()))
	}
	c.t.Fatal("state machines did not converge")
	return nil
}

// ---- tests ----

func TestSingleNode(t *testing.T) {
	c := newCluster(t, 1, nil)
	l := c.leader()
	for i := 0; i < 10; i++ {
		c.propose(l, fmt.Sprint("v", i))
	}
	if got := c.waitConverged(); len(got) != 10 {
		t.Fatalf("got %d", len(got))
	}
	if err := l.ReadBarrier(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReplicationAndOrder(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.propose(l, fmt.Sprint("v", i))
		}()
	}
	wg.Wait()
	got := c.waitConverged()
	if len(got) != 100 {
		t.Fatalf("expected 100 entries, got %d", len(got))
	}
}

func TestProposeViaFollower(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader()
	for _, id := range c.ids {
		if c.nodes[id] != l {
			// followers need to learn who the leader is first
			deadline := time.Now().Add(2 * time.Second)
			for c.nodes[id].LeaderID() == "" && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			c.propose(c.nodes[id], "via-"+id)
		}
	}
	if got := c.waitConverged(); len(got) != 2 {
		t.Fatalf("got %v", got)
	}
}

func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, 5, nil)
	l1 := c.leader()
	for i := 0; i < 20; i++ {
		c.propose(l1, fmt.Sprint("a", i))
	}
	id1 := l1.cfg.ID
	c.net.isolate(id1, c.ids, true)

	l2 := c.leader(id1)
	if l2.cfg.ID == id1 {
		t.Fatal("same leader")
	}
	for i := 0; i < 20; i++ {
		c.propose(l2, fmt.Sprint("b", i))
	}

	// heal: old leader must step down and converge, not overwrite anything
	c.net.isolate(id1, c.ids, false)
	got := c.waitConverged()
	if len(got) != 40 {
		t.Fatalf("want 40 entries, got %d", len(got))
	}
	for i := 0; i < 20; i++ {
		if got[i] != fmt.Sprint("a", i) || got[20+i] != fmt.Sprint("b", i) {
			t.Fatalf("order broken at %d: %v", i, got)
		}
	}
}

func TestMinorityCannotCommit(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader()
	c.propose(l, "before")
	id := l.cfg.ID
	c.net.isolate(id, c.ids, true)

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if _, err := l.Propose(ctx, []byte("lonely")); err == nil {
		t.Fatal("isolated leader committed a write")
	}

	c.net.isolate(id, c.ids, false)
	got := c.waitConverged()
	for _, v := range got {
		if v == "lonely" {
			t.Fatal("uncommitted entry leaked into the log")
		}
	}
}

func TestSnapshotCatchUp(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.SnapshotThreshold = 20 })
	l := c.leader()
	var lag string
	for _, id := range c.ids {
		if c.nodes[id] != l {
			lag = id
			break
		}
	}
	c.net.isolate(lag, c.ids, true)
	for i := 0; i < 120; i++ {
		c.propose(l, fmt.Sprint("v", i))
	}
	if st := l.Status(); st.Base == 0 {
		t.Fatalf("leader never compacted: %+v", st)
	}
	c.net.isolate(lag, c.ids, false)
	got := c.waitConverged()
	if len(got) != 120 {
		t.Fatalf("got %d", len(got))
	}
	if c.nodes[lag].Status().Base == 0 {
		t.Fatal("lagging follower should have received a snapshot")
	}
}

func TestRestartKeepsState(t *testing.T) {
	dir := t.TempDir()
	c := &cluster{t: t, net: newMemNet(), nodes: map[string]*Node{}, fsms: map[string]*listFSM{}, sts: map[string]Storage{}}
	c.ids = []string{"a", "b", "c"}
	c.cfg = Config{Peers: c.ids, HeartbeatInterval: 15 * time.Millisecond, ElectionTimeout: 120 * time.Millisecond, SnapshotThreshold: 30}
	for _, id := range c.ids {
		fs, err := NewFileStorage(dir+"/"+id, true)
		if err != nil {
			t.Fatal(err)
		}
		c.sts[id] = fs
		c.start(id)
	}
	l := c.leader()
	for i := 0; i < 80; i++ {
		c.propose(l, fmt.Sprint("v", i))
	}
	c.waitConverged()

	for _, id := range c.ids {
		c.stop(id)
	}
	for _, id := range c.ids {
		fs, err := NewFileStorage(dir+"/"+id, true)
		if err != nil {
			t.Fatal(err)
		}
		c.sts[id] = fs
		c.start(id)
	}
	l = c.leader()
	c.propose(l, "after-restart")
	got := c.waitConverged()
	if len(got) != 81 || got[80] != "after-restart" || got[0] != "v0" || got[79] != "v79" {
		t.Fatalf("unexpected state after restart: len=%d", len(got))
	}
	t.Cleanup(func() {
		for _, id := range c.ids {
			c.sts[id].Close()
		}
	})
}

func TestReadBarrierSeesLatestWrite(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader()
	for _, id := range c.ids {
		for c.nodes[id].LeaderID() == "" {
			time.Sleep(5 * time.Millisecond)
		}
	}
	for i := 0; i < 30; i++ {
		c.propose(l, fmt.Sprint("v", i))
		for _, id := range c.ids {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			if err := c.nodes[id].ReadBarrier(ctx); err != nil {
				cancel()
				t.Fatalf("barrier on %s: %v", id, err)
			}
			cancel()
			if n := len(c.fsms[id].snapshotVals()); n < i+1 {
				t.Fatalf("%s read %d entries after barrier, expected >= %d", id, n, i+1)
			}
		}
	}
}

func TestDeposedLeaderCannotServeReads(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader()
	c.propose(l, "x")
	c.net.isolate(l.cfg.ID, c.ids, true)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := l.ReadBarrier(ctx); err == nil {
		t.Fatal("partitioned leader served a linearizable read")
	}
}

func TestWALTornTail(t *testing.T) {
	dir := t.TempDir()
	fs, _ := NewFileStorage(dir, true)
	if _, _, _, _, err := fs.Load(); err != nil {
		t.Fatal(err)
	}
	fs.Append([]Entry{{Index: 1, Term: 1, Data: []byte("a")}, {Index: 2, Term: 1, Data: []byte("b")}})
	fs.Close()

	// simulate a crash mid-write: half a record at the end
	f := openAppend(t, dir+"/wal.log")
	f.Write([]byte{9, 0, 0, 0, 1, 2, 3})
	f.Close()

	fs2, _ := NewFileStorage(dir, true)
	_, _, _, es, err := fs2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(es) != 2 {
		t.Fatalf("want 2 intact entries, got %d", len(es))
	}
	if err := fs2.Append([]Entry{{Index: 3, Term: 1, Data: []byte("c")}}); err != nil {
		t.Fatal(err)
	}
	fs2.Close()
	fs3, _ := NewFileStorage(dir, true)
	_, _, _, es, _ = fs3.Load()
	if len(es) != 3 || string(es[2].Data) != "c" {
		t.Fatalf("append after repair lost: %d", len(es))
	}
	fs3.Close()
}
