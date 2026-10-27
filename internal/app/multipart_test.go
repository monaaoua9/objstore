package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/monaaoua9/objstore/internal/ec"
)

type partInfo struct {
	N    int
	ETag string
}

func (c *tcluster) uploadPart(via, bucket, id string, n int, data []byte) string {
	c.t.Helper()
	resp, b := c.do("PUT", fmt.Sprintf("%s/api/v1/buckets/%s/uploads/%s/parts/%d", c.base(via), bucket, id, n), bytes.NewReader(data), nil)
	if resp.StatusCode != 201 {
		c.t.Fatalf("part %d: %d %s", n, resp.StatusCode, b)
	}
	var out struct{ Data struct{ ETag string } }
	mustJSON(c.t, b, &out)
	return out.Data.ETag
}

func (c *tcluster) startUpload(via, bucket, key string) string {
	c.t.Helper()
	resp, b := c.do("POST", c.base(via)+"/api/v1/buckets/"+bucket+"/uploads?key="+key, nil, map[string]string{"Content-Type": "video/x-test"})
	if resp.StatusCode != 201 {
		c.t.Fatalf("create upload: %d %s", resp.StatusCode, b)
	}
	var out struct {
		Data struct {
			UploadID string `json:"upload_id"`
		}
	}
	mustJSON(c.t, b, &out)
	return out.Data.UploadID
}

func (c *tcluster) complete(via, bucket, id string, parts []partInfo) (int, []byte) {
	type sel struct {
		Part int    `json:"part"`
		ETag string `json:"etag"`
	}
	var body struct {
		Parts []sel `json:"parts"`
	}
	for _, p := range parts {
		body.Parts = append(body.Parts, sel{p.N, p.ETag})
	}
	b, _ := json.Marshal(body)
	resp, out := c.do("POST", fmt.Sprintf("%s/api/v1/buckets/%s/uploads/%s/complete", c.base(via), bucket, id), bytes.NewReader(b), nil)
	return resp.StatusCode, out
}

func TestMultipartUpload(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2)
	c.mkbucket("n1", "mpu")

	id := c.startUpload("n2", "mpu", "movies/long.bin")
	p1, p2, p3, junk := randBytes(200_000), randBytes(33_333), randBytes(120_000), randBytes(50_000)
	// out of order, via different gateways, with a retried part and a part we never select
	e3 := c.uploadPart("n3", "mpu", id, 3, p3)
	c.uploadPart("n1", "mpu", id, 2, randBytes(10)) // first attempt, replaced below
	e2 := c.uploadPart("n4", "mpu", id, 2, p2)
	e1 := c.uploadPart("n5", "mpu", id, 1, p1)
	c.uploadPart("n1", "mpu", id, 9, junk)

	resp, b := c.do("GET", fmt.Sprintf("%s/api/v1/buckets/mpu/uploads/%s", c.base("n6"), id), nil, nil)
	var listed struct{ Data []struct{ Part int } }
	mustJSON(t, b, &listed)
	if resp.StatusCode != 200 || len(listed.Data) != 4 {
		t.Fatalf("part listing: %d %s", resp.StatusCode, b)
	}

	if code, _ := c.complete("n1", "mpu", id, []partInfo{{1, e1}, {2, "bogus"}}); code != 400 {
		t.Fatalf("bad etag accepted: %d", code)
	}
	if code, out := c.complete("n1", "mpu", id, []partInfo{{1, e1}, {2, e2}, {3, e3}}); code != 201 {
		t.Fatalf("complete: %d %s", code, out)
	}
	if code, _ := c.complete("n1", "mpu", id, []partInfo{{1, e1}}); code != 404 {
		t.Fatalf("second complete should 404, got %d", code)
	}

	whole := append(append(append([]byte{}, p1...), p2...), p3...)
	for _, via := range []string{"n1", "n4", "n6"} {
		c.mustGet(via, "mpu", "movies/long.bin", whole)
	}
	resp, _ = c.get("n1", "mpu", "movies/long.bin", nil)
	if resp.Header.Get("Content-Type") != "video/x-test" {
		t.Fatalf("content-type lost: %q", resp.Header.Get("Content-Type"))
	}

	for _, r := range [][2]int{{0, 9}, {199_990, 200_010}, {233_000, 233_400}, {len(whole) - 5, len(whole) - 1}, {100, len(whole) - 100}} {
		resp, b := c.get("n3", "mpu", "movies/long.bin", map[string]string{"Range": fmt.Sprintf("bytes=%d-%d", r[0], r[1])})
		if resp.StatusCode != 206 || !bytes.Equal(b, whole[r[0]:r[1]+1]) {
			t.Fatalf("range %v: status %d, %d bytes", r, resp.StatusCode, len(b))
		}
	}

	if resp, _ := c.do("GET", c.base("n1")+"/api/v1/buckets/~mpu/objects", nil, nil); resp.StatusCode != 404 {
		t.Fatalf("staging namespace reachable: %d", resp.StatusCode)
	}

	// parts are ordinary erasure-coded objects: lose a shard of one and the audit restores it
	o := c.objectMeta("n1", "mpu", "movies/long.bin")
	if len(o.Parts) != 3 {
		t.Fatalf("manifest has %d parts", len(o.Parts))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pobj, err := c.metaClient("n1").GetVersion(ctx, o.Parts[1].Version)
	if err != nil {
		t.Fatal(err)
	}
	sh := pobj.Shards[0]
	c.nodes[sh.Node].Blob.Delete(pobj.Version, 0)
	eventually(t, 20*time.Second, "audit restored a shard of a multipart part", func() bool {
		return shardHealthy(c, sh.Node, pobj.Version, 0, ec.LayoutOf(pobj).ShardFileSize())
	})

	c.kill("n5", "n6")
	c.mustGet("n1", "mpu", "movies/long.bin", whole)

	c.do("DELETE", c.base("n1")+"/api/v1/buckets/mpu/objects/movies/long.bin", nil, nil)
	eventually(t, 20*time.Second, "all shards gone after delete", func() bool { return totalShardsAlive(c) == 0 })
}

// killed nodes can't be counted
func totalShardsAlive(c *tcluster) int {
	n := 0
	for id, nd := range c.nodes {
		if id == "n5" || id == "n6" {
			continue
		}
		n += int(nd.Blob.Count())
	}
	return n
}

func TestAbortMultipartFreesParts(t *testing.T) {
	c := newCluster(t, 3, 2, 3, 2)
	c.mkbucket("n1", "mpu")
	id := c.startUpload("n1", "mpu", "x")
	c.uploadPart("n2", "mpu", id, 1, randBytes(90_000))
	if totalShards(c) == 0 {
		t.Fatal("part wrote no shards")
	}
	resp, _ := c.do("DELETE", fmt.Sprintf("%s/api/v1/buckets/mpu/uploads/%s", c.base("n1"), id), nil, nil)
	if resp.StatusCode != 204 {
		t.Fatalf("abort: %d", resp.StatusCode)
	}
	eventually(t, 15*time.Second, "aborted parts collected", func() bool { return totalShards(c) == 0 })
	resp, _ = c.do("PUT", fmt.Sprintf("%s/api/v1/buckets/mpu/uploads/%s/parts/1", c.base("n1"), id), bytes.NewReader([]byte("late")), nil)
	if resp.StatusCode != 404 {
		t.Fatalf("part accepted after abort: %d", resp.StatusCode)
	}
}
