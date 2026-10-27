package meta

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// records are spilled to a file, so this measures the in-memory index only: key, sorted-key slot,
// two map entries. Not exact (it moves with the Go version); it pins the order of magnitude.
func TestMetadataFootprint(t *testing.T) {
	const n = 30_000
	ctx := context.Background()
	f, err := NewFSMAt(filepath.Join(t.TempDir(), "spill"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.Close)
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
	f.st.dropCache() // a fixed-size cache would skew the per-object figure at this n
	runtime.GC()
	runtime.ReadMemStats(&after)
	per := (after.HeapAlloc - before.HeapAlloc) / n

	t0 := time.Now()
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	took := time.Since(t0)
	g, _ := NewFSMAt(filepath.Join(t.TempDir(), "spill"))
	t.Cleanup(g.Close)
	if err := g.Restore(snap); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d objects: ~%d bytes of heap each, snapshot %d MiB gzip'd (%d bytes/object) in %v", n, per, len(snap)>>20, len(snap)/n, took)
	if per > 450 {
		t.Fatalf("heap per object grew to %d bytes", per)
	}
	if st, _ := (&Client{direct{g}}).Stats(ctx); st.Objects != n {
		t.Fatalf("restored %d objects", st.Objects)
	}
}
