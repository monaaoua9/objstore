package app

import (
	"fmt"
	"net"
	"testing"
	"time"
)

// full power cycle: stop every node, start them again on the same ports and data dirs
func TestClusterRestartKeepsEverything(t *testing.T) {
	c := newCluster(t, 3, 2, 3, 2)
	c.mkbucket("n1", "bkt")
	objs := map[string][]byte{}
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("o%d", i)
		objs[k] = randBytes(120_000 + i)
		c.put("n1", "bkt", k, objs[k])
	}
	addrs := map[string]string{}
	for id, n := range c.nodes {
		addrs[id] = n.Addr()
	}
	for _, n := range c.nodes {
		n.Stop()
	}

	for id, addr := range addrs {
		var ln net.Listener
		var err error
		for try := 0; try < 50; try++ { // port may linger briefly after close
			if ln, err = net.Listen("tcp", addr); err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil {
			t.Fatalf("rebind %s: %v", addr, err)
		}
		c.startNode(id, ln)
	}
	eventually(t, 20*time.Second, "cluster serves reads after restart", func() bool {
		resp, b := c.get("n4", "bkt", "o3", nil)
		return resp.StatusCode == 200 && string(b) == string(objs["o3"])
	})
	for k, d := range objs {
		c.mustGet("n2", "bkt", k, d)
	}
	// and it still takes writes, once the probe has marked every restarted node healthy again
	eventually(t, 30*time.Second, "writes accepted after restart", func() bool {
		resp, _ := c.tryDo("PUT", c.base("n5")+"/api/v1/buckets/bkt/objects/post-restart", bytesReader([]byte("alive")))
		return resp != nil && resp.StatusCode == 201
	})
	c.mustGet("n1", "bkt", "post-restart", []byte("alive"))
}
