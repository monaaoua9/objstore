package app

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPutGetRangeListDelete(t *testing.T) {
	c := newCluster(t, 3, 2, 3, 2)
	c.mkbucket("n1", "docs")

	sizes := map[string]int{"empty": 0, "tiny": 5, "one-stripe": 3 * c.shardSz, "ragged": 7*3*c.shardSz + 1234, "big": 3 << 20}
	data := map[string][]byte{}
	for name, n := range sizes {
		data[name] = randBytes(n)
		c.put("n1", "docs", "files/"+name, data[name])
	}
	// every node's gateway must serve every object, voter or not
	for _, via := range []string{"n1", "n2", "n3", "n4", "n5"} {
		for name := range sizes {
			c.mustGet(via, "docs", "files/"+name, data[name])
		}
	}

	t.Run("range", func(t *testing.T) {
		d := data["ragged"]
		cases := []struct {
			hdr        string
			lo, hi     int
			wantStatus int
		}{
			{"bytes=0-99", 0, 100, 206},
			{"bytes=1000-", 1000, len(d), 206},
			{"bytes=-500", len(d) - 500, len(d), 206},
			{fmt.Sprintf("bytes=%d-%d", 3*c.shardSz-5, 3*c.shardSz+5), 3*c.shardSz - 5, 3*c.shardSz + 6, 206}, // straddles stripes
			{fmt.Sprintf("bytes=%d-", len(d)+10), 0, 0, 416},
		}
		for _, tc := range cases {
			resp, b := c.get("n2", "docs", "files/ragged", map[string]string{"Range": tc.hdr})
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("%s: status %d", tc.hdr, resp.StatusCode)
			}
			if tc.wantStatus == 206 && string(b) != string(d[tc.lo:tc.hi]) {
				t.Fatalf("%s: wrong bytes (%d vs %d)", tc.hdr, len(b), tc.hi-tc.lo)
			}
		}
	})

	t.Run("head and conditional", func(t *testing.T) {
		resp, _ := c.do("HEAD", c.base("n3")+"/api/v1/buckets/docs/objects/files/tiny", nil, nil)
		if resp.StatusCode != 200 || resp.ContentLength != 5 || resp.Header.Get("ETag") == "" {
			t.Fatalf("head: %d len=%d", resp.StatusCode, resp.ContentLength)
		}
		resp, _ = c.get("n3", "docs", "files/tiny", map[string]string{"If-None-Match": resp.Header.Get("ETag")})
		if resp.StatusCode != 304 {
			t.Fatalf("conditional get: %d", resp.StatusCode)
		}
	})

	t.Run("list paging", func(t *testing.T) {
		for i := 0; i < 12; i++ {
			c.put("n1", "docs", fmt.Sprintf("logs/%02d", i), []byte("x"))
		}
		var seen []string
		cursor := ""
		for {
			url := c.base("n4") + "/api/v1/buckets/docs/objects?prefix=logs/&limit=5&cursor=" + cursor
			resp, b := c.do("GET", url, nil, nil)
			if resp.StatusCode != 200 {
				t.Fatalf("list: %d %s", resp.StatusCode, b)
			}
			var out struct {
				Data []struct{ Key string }
				Meta struct {
					Next string `json:"next_cursor"`
				}
			}
			mustJSON(t, b, &out)
			for _, it := range out.Data {
				seen = append(seen, it.Key)
			}
			if out.Meta.Next == "" {
				break
			}
			cursor = out.Meta.Next
		}
		if len(seen) != 12 || seen[0] != "logs/00" || seen[11] != "logs/11" {
			t.Fatalf("listing wrong: %v", seen)
		}
	})

	t.Run("errors", func(t *testing.T) {
		if resp, _ := c.get("n1", "docs", "nope", nil); resp.StatusCode != 404 {
			t.Fatalf("missing object: %d", resp.StatusCode)
		}
		if resp, _ := c.get("n1", "ghost-bucket", "x", nil); resp.StatusCode != 404 {
			t.Fatalf("missing bucket: %d", resp.StatusCode)
		}
		resp, _ := c.do("PUT", c.base("n1")+"/api/v1/buckets/ghost-bucket/objects/x", strings.NewReader("hi"), nil)
		if resp.StatusCode != 404 {
			t.Fatalf("put into missing bucket: %d", resp.StatusCode)
		}
		resp, _ = c.do("PUT", c.base("n1")+"/api/v1/buckets/docs", nil, nil)
		if resp.StatusCode != 409 {
			t.Fatalf("duplicate bucket: %d", resp.StatusCode)
		}
		resp, _ = c.do("PUT", c.base("n1")+"/api/v1/buckets/Bad_Name", nil, nil)
		if resp.StatusCode != 400 {
			t.Fatalf("bad bucket name: %d", resp.StatusCode)
		}
		resp, _ = c.do("DELETE", c.base("n1")+"/api/v1/buckets/docs", nil, nil)
		if resp.StatusCode != 409 {
			t.Fatalf("delete non-empty bucket: %d", resp.StatusCode)
		}
	})

	t.Run("delete and gc", func(t *testing.T) {
		before := totalShards(c)
		resp, _ := c.do("DELETE", c.base("n2")+"/api/v1/buckets/docs/objects/files/big", nil, nil)
		if resp.StatusCode != 204 {
			t.Fatalf("delete: %d", resp.StatusCode)
		}
		if resp, _ := c.get("n1", "docs", "files/big", nil); resp.StatusCode != 404 {
			t.Fatalf("deleted object still readable: %d", resp.StatusCode)
		}
		eventually(t, 10*time.Second, "shards of deleted object garbage-collected", func() bool {
			return totalShards(c) == before-5
		})
		// overwriting also retires the old version
		c.put("n1", "docs", "files/tiny", []byte("replaced"))
		c.mustGet("n3", "docs", "files/tiny", []byte("replaced"))
		eventually(t, 10*time.Second, "old version collected", func() bool {
			return totalShards(c) == before-5
		})
	})
}

func totalShards(c *tcluster) int {
	n := 0
	for _, nd := range c.nodes {
		n += int(nd.Blob.Count())
	}
	return n
}

func TestConcurrentClients(t *testing.T) {
	c := newCluster(t, 3, 2, 3, 2)
	c.mkbucket("n1", "load")
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			via := fmt.Sprintf("n%d", w%5+1)
			for i := 0; i < 5; i++ {
				d := randBytes(200_000 + w*1000 + i)
				key := fmt.Sprintf("w%d/o%d", w, i)
				c.put(via, "load", key, d)
				c.mustGet(fmt.Sprintf("n%d", (w+2)%5+1), "load", key, d)
			}
		}()
	}
	wg.Wait()
}
