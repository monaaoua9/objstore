package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/monaaoua9/objstore/internal/httpx"
	"github.com/monaaoua9/objstore/internal/meta"
	"github.com/monaaoua9/objstore/internal/repair"
	"github.com/monaaoua9/objstore/internal/s3api"
	"github.com/monaaoua9/objstore/internal/scrub"
)

type tcluster struct {
	t       *testing.T
	voters  map[string]string
	nodes   map[string]*Node
	k, m    int
	root    string
	client  *http.Client
	logOut  io.Writer
	shardSz int
	nmu     sync.Mutex // chaos tests swap nodes while workers read the map
}

func quietLogger(t *testing.T) *slog.Logger {
	if os.Getenv("OBJSTORE_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newCluster boots nVoters raft voters plus nStorage storage-only nodes, all in-process on loopback.
func newCluster(t *testing.T, nVoters, nStorage, k, m int) *tcluster {
	t.Helper()
	c := &tcluster{t: t, voters: map[string]string{}, nodes: map[string]*Node{}, k: k, m: m, root: t.TempDir(), client: httpx.NewClient(nil), shardSz: 16 << 10}

	lns := map[string]net.Listener{}
	total := nVoters + nStorage
	for i := 1; i <= total; i++ {
		id := fmt.Sprintf("n%d", i)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[id] = ln
		if i <= nVoters {
			c.voters[id] = ln.Addr().String()
		}
	}
	for i := 1; i <= total; i++ {
		id := fmt.Sprintf("n%d", i)
		c.startNode(id, lns[id])
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.Kill()
		}
	})
	c.waitForNodes(total)
	return c
}

func (c *tcluster) config(id string, ln net.Listener) Config {
	return Config{
		ID: id, Listener: ln, DataDir: filepath.Join(c.root, id),
		Zone:   "z" + id,
		Voters: c.voters,
		K:      c.k, M: c.m, ShardSize: c.shardSz,
		RaftHeartbeat: 30 * time.Millisecond, RaftElection: 300 * time.Millisecond, RaftNoSync: true, RaftSnapshot: 200,
		Repair: repair.Config{
			ProbeInterval: 100 * time.Millisecond, DownAfter: 3, RepairDelay: 400 * time.Millisecond,
			RepairEvery: 100 * time.Millisecond, SweepEvery: 400 * time.Millisecond, SweepPause: time.Millisecond, GCEvery: 100 * time.Millisecond, LifecycleEvery: 100 * time.Millisecond, DayLength: 150 * time.Millisecond,
		},
		Scrub:        scrub.Config{Interval: 24 * time.Hour, OrphanAge: time.Hour},
		VoterRefresh: 200 * time.Millisecond,
		S3Listen:     "127.0.0.1:0", S3Keys: map[string]s3api.Key{
			testAK:   {Secret: testSK},
			roAK:     {Secret: "readonly-secret", ReadOnly: true},
			scopedAK: {Secret: "scoped-secret", Buckets: []string{"mine", "shared"}},
		},
		Logger: quietLogger(c.t),
	}
}

func (c *tcluster) startNode(id string, ln net.Listener) *Node {
	c.t.Helper()
	n, err := Start(c.config(id, ln))
	if err != nil {
		c.t.Fatalf("start %s: %v", id, err)
	}
	c.nmu.Lock()
	c.nodes[id] = n
	c.nmu.Unlock()
	return n
}

// addNode joins a fresh storage-only node to a running cluster.
func (c *tcluster) addNode(id string) *Node {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		c.t.Fatal(err)
	}
	return c.startNode(id, ln)
}

func (c *tcluster) kill(ids ...string) {
	for _, id := range ids {
		c.nodes[id].Kill()
	}
}

func (c *tcluster) base(id string) string {
	c.nmu.Lock()
	defer c.nmu.Unlock()
	return "http://" + c.nodes[id].Addr()
}

func (c *tcluster) waitForNodes(n int) {
	c.t.Helper()
	eventually(c.t, 15*time.Second, "all nodes registered and visible", func() bool {
		for _, nd := range c.nodes {
			if nd.View != nil && len(nd.View.Nodes()) < n {
				return false
			}
		}
		return true
	})
}

func (c *tcluster) leaderID() string {
	for id, n := range c.nodes {
		if n.Raft != nil && n.Raft.IsLeader() {
			return id
		}
	}
	return ""
}

func (c *tcluster) metaClient(id string) *meta.Client { return c.nodes[id].Meta }

func eventually(t *testing.T, d time.Duration, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", d, what)
}

// ---- tiny API client ----

func (c *tcluster) do(method, url string, body io.Reader, hdr map[string]string) (*http.Response, []byte) {
	c.t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		c.t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func (c *tcluster) mkbucket(via, name string) {
	c.t.Helper()
	resp, b := c.do("PUT", c.base(via)+"/api/v1/buckets/"+name, nil, nil)
	if resp.StatusCode != 201 {
		c.t.Fatalf("create bucket: %d %s", resp.StatusCode, b)
	}
}

func (c *tcluster) put(via, bucket, key string, data []byte) string {
	c.t.Helper()
	resp, b := c.do("PUT", c.base(via)+"/api/v1/buckets/"+bucket+"/objects/"+key, bytes.NewReader(data), map[string]string{"Content-Type": "application/x-test"})
	if resp.StatusCode != 201 {
		c.t.Fatalf("put %s: %d %s", key, resp.StatusCode, b)
	}
	var out struct {
		Data struct {
			ETag, Version string
		}
	}
	json.Unmarshal(b, &out)
	sum := sha256.Sum256(data)
	if out.Data.ETag != hex.EncodeToString(sum[:]) {
		c.t.Fatalf("etag %s != sha256 %x", out.Data.ETag, sum)
	}
	return out.Data.Version
}

func (c *tcluster) get(via, bucket, key string, hdr map[string]string) (*http.Response, []byte) {
	c.t.Helper()
	return c.do("GET", c.base(via)+"/api/v1/buckets/"+bucket+"/objects/"+key, nil, hdr)
}

func (c *tcluster) mustGet(via, bucket, key string, want []byte) {
	c.t.Helper()
	resp, b := c.get(via, bucket, key, nil)
	if resp.StatusCode != 200 {
		c.t.Fatalf("get %s via %s: %d %s", key, via, resp.StatusCode, b)
	}
	if !bytes.Equal(b, want) {
		c.t.Fatalf("get %s via %s: body differs (got %d bytes, want %d)", key, via, len(b), len(want))
	}
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func (c *tcluster) objectMeta(via, bucket, key string) *meta.Object {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o, err := c.metaClient(via).GetObject(ctx, bucket, key)
	if err != nil {
		c.t.Fatal(err)
	}
	return o
}

func (c *tcluster) damagedCount(via string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d, err := c.metaClient(via).Damaged(ctx, 0)
	if err != nil {
		return -1
	}
	return len(d)
}

var _ = sync.WaitGroup{}

func mustJSON(t *testing.T, b []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("bad json %q: %v", b, err)
	}
}

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }

// tryDo is do() without the fatal on transport errors; chaos tests expect some.
func (c *tcluster) tryDo(method, url string, body io.Reader) (*http.Response, []byte) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, nil
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, nil
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil
	}
	return resp, b
}
