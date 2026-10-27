package meta

import (
	"context"
	"crypto/sha256"
	"fmt"
	"runtime"
	"testing"
	"time"
)

// not an assertion on exact numbers (they move with the Go version); it pins the order of magnitude
// and prints the figures quoted in the architecture doc
func TestMetadataFootprint(t *testing.T) {
	const n = 30_000
	ctx := context.Background()
	f := NewFSM()
	c := &Client{direct{f}}
	c.CreateBucket(ctx, "big")

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	nodes := []string{"node-a", "node-b", "node-c", "node-d", "node-e", "node-f"}
	for i := 0; i < n; i++ {
		o := &Object{
			Bucket: "big", Key: fmt.Sprintf("photos/2024/%08d.jpg", i), Version: fmt.Sprintf("%026d", i),
			Size: 4 << 20, ETag: fmt.Sprintf("%x", sha256.Sum256([]byte{byte(i), byte(i >> 8), byte(i >> 16)})), ContentType: "image/jpeg", Created: time.Now(),
			K: 4, M: 2, ShardSize: 256 << 10, Stripes: 16,
		}
		for j := 0; j < 6; j++ {
			o.Shards = append(o.Shards, ShardLoc{j, nodes[(i+j)%6]})
		}
		if err := c.CommitObject(ctx, o, nil); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	per := (after.HeapAlloc - before.HeapAlloc) / n

	t0 := time.Now()
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	took := time.Since(t0)
	g := NewFSM()
	if err := g.Restore(snap); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d objects: ~%d bytes of heap each, snapshot %d MiB gzip'd (%d bytes/object) in %v", n, per, len(snap)>>20, len(snap)/n, took)
	if per > 2500 {
		t.Fatalf("heap per object grew to %d bytes", per)
	}
	if st, _ := (&Client{direct{g}}).Stats(ctx); st.Objects != n {
		t.Fatalf("restored %d objects", st.Objects)
	}
}
