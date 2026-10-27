package app

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/monaaoua9/objstore/internal/ec"
	"github.com/monaaoua9/objstore/internal/meta"
)

// flip one bit in the middle of a stored shard, bypassing the node's own API
func corruptShard(t *testing.T, c *tcluster, nodeID, version string, idx int) {
	t.Helper()
	p := fmt.Sprintf("%s/blobs/shards/%s/%s.%d", c.nodes[nodeID].Cfg.DataDir, version[:2], version, idx)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)/2] ^= 0x08
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func shardHealthy(c *tcluster, nodeID, version string, idx int, want int64) bool {
	n := c.nodes[nodeID]
	if n == nil {
		return false
	}
	if _, err := n.Blob.Verify(version, idx, nil); err != nil {
		return false
	}
	sz, err := n.Blob.Stat(version, idx)
	return err == nil && sz == want
}

func TestReadsSurviveLostNodes(t *testing.T) {
	// 3 voters + 4 storage nodes, 3+2 coding: any 2 shard holders may vanish
	c := newCluster(t, 3, 4, 3, 2)
	c.mkbucket("n1", "bkt")
	data := randBytes(2<<20 + 77)
	c.put("n1", "bkt", "obj", data)
	o := c.objectMeta("n1", "bkt", "obj")

	// kill two non-voters that hold shards of this object
	var victims []string
	for _, sh := range o.Shards {
		if c.nodes[sh.Node].Raft == nil && len(victims) < 2 {
			victims = append(victims, sh.Node)
		}
	}
	if len(victims) < 2 {
		t.Skip("placement put fewer than two shards on storage-only nodes")
	}
	c.kill(victims...)

	for _, via := range []string{"n1", "n2", "n3"} {
		c.mustGet(via, "bkt", "obj", data)
	}
}

func TestAutoRepairAfterNodeLoss(t *testing.T) {
	c := newCluster(t, 3, 4, 3, 2)
	c.mkbucket("n1", "bkt")
	objs := map[string][]byte{}
	for i := 0; i < 6; i++ {
		k := fmt.Sprintf("o%d", i)
		objs[k] = randBytes(300_000 + i*13)
		c.put("n1", "bkt", k, objs[k])
	}

	// lose one storage-only node outright; shards it held must reappear on survivors
	victim := "n6"
	held := 0
	for k := range objs {
		for _, sh := range c.objectMeta("n1", "bkt", k).Shards {
			if sh.Node == victim {
				held++
			}
		}
	}
	c.kill(victim)

	eventually(t, 30*time.Second, "all shards rehomed off the dead node and verified", func() bool {
		for k := range objs {
			o := c.objectMeta("n1", "bkt", k)
			want := ec.LayoutOf(o).ShardFileSize()
			for _, sh := range o.Shards {
				if sh.Node == victim || !shardHealthy(c, sh.Node, o.Version, sh.Index, want) {
					return false
				}
			}
		}
		return c.damagedCount("n1") == 0
	})
	t.Logf("%d shards were on %s and have been rebuilt", held, victim)

	for k, d := range objs {
		c.mustGet("n2", "bkt", k, d)
	}
}

func TestScrubberHealsBitrot(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2)
	c.mkbucket("n1", "bkt")
	data := randBytes(900_000)
	c.put("n1", "bkt", "obj", data)
	o := c.objectMeta("n1", "bkt", "obj")
	want := ec.LayoutOf(o).ShardFileSize()

	// two shards rot at once (== the parity budget)
	victims := o.Shards[:2]
	for _, sh := range victims {
		corruptShard(t, c, sh.Node, o.Version, sh.Index)
	}
	// data stays readable even before the scrubber notices
	c.mustGet("n1", "bkt", "obj", data)

	for _, sh := range victims {
		res := c.nodes[sh.Node].Scrb.Pass(context.Background())
		if res.Corrupt == 0 {
			t.Fatalf("scrubber on %s missed the bitrot", sh.Node)
		}
	}
	eventually(t, 20*time.Second, "bitrotten shards rebuilt", func() bool {
		o2 := c.objectMeta("n1", "bkt", "obj")
		for _, sh := range o2.Shards {
			if !shardHealthy(c, sh.Node, o2.Version, sh.Index, want) {
				return false
			}
		}
		return c.damagedCount("n1") == 0
	})
	c.mustGet("n3", "bkt", "obj", data)
}

func TestReadPathReportsAndRepairsRot(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2)
	c.mkbucket("n1", "bkt")
	data := randBytes(500_000)
	c.put("n1", "bkt", "obj", data)
	o := c.objectMeta("n1", "bkt", "obj")
	want := ec.LayoutOf(o).ShardFileSize()

	sh := o.Shards[0] // a data shard, so the reader hits it first
	corruptShard(t, c, sh.Node, o.Version, sh.Index)

	c.mustGet("n2", "bkt", "obj", data) // served from parity, and files a repair hint

	eventually(t, 20*time.Second, "read-repair rebuilt the shard", func() bool {
		o2 := c.objectMeta("n1", "bkt", "obj")
		return shardHealthy(c, o2.Shards[0].Node, o2.Version, 0, want) && c.damagedCount("n1") == 0
	})
}

func TestAuditNoticesDeletedShardFile(t *testing.T) {
	c := newCluster(t, 3, 2, 3, 2)
	c.mkbucket("n1", "bkt")
	data := randBytes(100_000)
	c.put("n1", "bkt", "obj", data)
	o := c.objectMeta("n1", "bkt", "obj")
	want := ec.LayoutOf(o).ShardFileSize()

	sh := o.Shards[3]
	if err := c.nodes[sh.Node].Blob.Delete(o.Version, sh.Index); err != nil {
		t.Fatal(err)
	}
	eventually(t, 20*time.Second, "anti-entropy sweep restored the missing shard file", func() bool {
		return shardHealthy(c, sh.Node, o.Version, sh.Index, want)
	})
}

func TestMetadataSurvivesLeaderLoss(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2) // 6 nodes: still 5 = k+m after losing the leader
	c.mkbucket("n1", "bkt")
	before := randBytes(250_000)
	c.put("n1", "bkt", "before", before)

	var lead string
	eventually(t, 5*time.Second, "leader", func() bool { lead = c.leaderID(); return lead != "" })
	c.kill(lead)

	// pick any surviving voter as the entry point
	var via string
	for id, n := range c.nodes {
		if id != lead && n.Raft != nil {
			via = id
			break
		}
	}
	// metadata must still be there and writes must resume once a new leader is elected
	eventually(t, 15*time.Second, "reads resume after failover", func() bool {
		resp, b := c.get(via, "bkt", "before", nil)
		return resp.StatusCode == 200 && string(b) == string(before)
	})
	after := randBytes(120_000)
	eventually(t, 15*time.Second, "writes resume after failover", func() bool {
		resp, _ := c.tryDo("PUT", c.base(via)+"/api/v1/buckets/bkt/objects/after", bytesReader(after))
		return resp != nil && resp.StatusCode == 201
	})
	c.mustGet(via, "bkt", "after", after)
}

func TestJoiningNodeTakesLoadAndDrainEmptiesNode(t *testing.T) {
	c := newCluster(t, 3, 2, 3, 2) // exactly k+m nodes at the start
	c.mkbucket("n1", "bkt")
	objs := map[string][]byte{}
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("o%02d", i)
		objs[k] = randBytes(40_000 + i)
		c.put("n1", "bkt", k, objs[k])
	}

	c.addNode("n6")
	c.addNode("n7")
	c.waitForNodes(7)

	// the ring now prefers the new nodes for some shards; the sweep should migrate them
	eventually(t, 40*time.Second, "new nodes received shards", func() bool {
		return c.nodes["n6"].Blob.Count()+c.nodes["n7"].Blob.Count() > 0
	})

	// every object keeps matching its ideal placement once the cluster settles
	eventually(t, 60*time.Second, "placement converged to the ring", func() bool {
		ring := c.nodes["n1"].View.Ring()
		for k := range objs {
			o := c.objectMeta("n1", "bkt", k)
			ideal := map[string]bool{}
			for _, id := range ring.Locate(o.Version, len(o.Shards)) {
				ideal[id] = true
			}
			for _, sh := range o.Shards {
				if !ideal[sh.Node] {
					return false
				}
			}
		}
		return true
	})
	for k, d := range objs {
		c.mustGet("n7", "bkt", k, d)
	}

	// now drain n6: all its shards should leave
	if resp, b := c.do("POST", c.base("n1")+"/api/v1/cluster/nodes/n6/drain", nil, nil); resp.StatusCode != 200 {
		t.Fatalf("drain: %d %s", resp.StatusCode, b)
	}
	eventually(t, 60*time.Second, "drained node emptied", func() bool {
		for k := range objs {
			for _, sh := range c.objectMeta("n1", "bkt", k).Shards {
				if sh.Node == "n6" {
					return false
				}
			}
		}
		return true
	})
	eventually(t, 10*time.Second, "files deleted from drained node", func() bool { return c.nodes["n6"].Blob.Count() == 0 })
	for k, d := range objs {
		c.mustGet("n1", "bkt", k, d)
	}
}

func TestWritesRefusedWithoutEnoughNodes(t *testing.T) {
	c := newCluster(t, 3, 2, 3, 2) // 5 nodes for 3+2: zero slack
	c.mkbucket("n1", "bkt")
	c.kill("n4", "n5")
	// before the cluster notices, writes may fail mid-stream or at quorum; either way: no 2xx and no ghost object
	resp, _ := c.do("PUT", c.base("n1")+"/api/v1/buckets/bkt/objects/x", bytesReader(randBytes(50_000)), nil)
	if resp.StatusCode == 201 {
		t.Fatal("write succeeded without a write quorum")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.metaClient("n1").GetObject(ctx, "bkt", "x"); err != meta.ErrObjectNotFound {
		t.Fatalf("failed write left metadata behind: %v", err)
	}
}

func TestScrubberRemovesOrphansAndStaleCopies(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2)
	c.mkbucket("n1", "bkt")
	c.put("n1", "bkt", "obj", randBytes(100_000))
	o := c.objectMeta("n1", "bkt", "obj")

	holder := map[string]bool{}
	for _, sh := range o.Shards {
		holder[sh.Node] = true
	}
	// a node that holds nothing of this object receives a stray copy of shard 0 (as if it had been moved away while the node was offline)
	var bystander string
	for id := range c.nodes {
		if !holder[id] {
			bystander = id
			break
		}
	}
	src := c.nodes[o.Shards[0].Node].Blob
	f, err := src.Open(o.Version, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.nodes[bystander].Blob.Put(o.Version, 0, f); err != nil {
		t.Fatal(err)
	}
	f.Close()
	// and a file for a version metadata has never seen
	const ghost = "deadbeefdeadbeefdeadbeefdeadbeef"
	if _, _, err := c.nodes[bystander].Blob.Put(ghost, 1, bytesReader(nil)); err != nil {
		t.Fatal(err)
	}

	sc := c.nodes[bystander].Scrb
	sc.Cfg.OrphanAge = time.Nanosecond
	res := sc.Pass(context.Background())
	if res.Orphans != 2 {
		t.Fatalf("removed %d, want the stale copy and the ghost", res.Orphans)
	}
	if _, err := c.nodes[bystander].Blob.Stat(o.Version, 0); err == nil {
		t.Fatal("stale copy survived")
	}
	// the real holder's copy is untouched
	if !shardHealthy(c, o.Shards[0].Node, o.Version, 0, ec.LayoutOf(o).ShardFileSize()) {
		t.Fatal("scrub damaged the legitimate shard")
	}
}
