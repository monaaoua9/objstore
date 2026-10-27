package meta

import (
	"context"
	"errors"
	"testing"
)

func vclient(t *testing.T) (*Client, *FSM) {
	t.Helper()
	f := NewFSM()
	c := &Client{direct{f}}
	ctx := context.Background()
	if err := c.CreateBucket(ctx, "vb"); err != nil {
		t.Fatal(err)
	}
	if err := c.EnableVersioning(ctx, "vb"); err != nil {
		t.Fatal(err)
	}
	return c, f
}

func TestVersionedOverwriteKeepsHistory(t *testing.T) {
	ctx := context.Background()
	c, _ := vclient(t)
	c.CommitObject(ctx, obj("vb", "k", "v1"), nil)
	c.CommitObject(ctx, obj("vb", "k", "v2"), nil)

	if gc, _ := c.GCQueue(ctx, 0); len(gc) != 0 {
		t.Fatalf("overwrite in a versioned bucket must not retire the old data: %+v", gc)
	}
	cur, _ := c.GetObject(ctx, "vb", "k")
	if cur.Version != "v2" {
		t.Fatalf("current = %s", cur.Version)
	}
	old, err := c.GetObjectVersion(ctx, "vb", "k", "v1")
	if err != nil || old.Version != "v1" {
		t.Fatalf("old version: %v %v", old, err)
	}
	// both are live placements, so repair and rebalance keep caring about them
	pl, _ := c.Placements(ctx, []string{"v1", "v2"})
	if len(pl) != 2 {
		t.Fatalf("placements %v", pl)
	}
	objs, _, _ := c.Scan(ctx, "", 100)
	if len(objs) != 2 {
		t.Fatalf("audit scan sees %d objects, want both versions", len(objs))
	}
}

func TestDeleteMarkerAndRestore(t *testing.T) {
	ctx := context.Background()
	c, _ := vclient(t)
	c.CommitObject(ctx, obj("vb", "k", "v1"), nil)

	m, err := c.DeleteObject(ctx, "vb", "k")
	if err != nil || m == nil || !m.DeleteMarker {
		t.Fatalf("delete should add a marker: %v %v", m, err)
	}
	if _, err := c.GetObject(ctx, "vb", "k"); !errors.Is(err, ErrDeleteMarker) || !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("read behind marker: %v", err)
	}
	if items, _, _ := c.ListObjects(ctx, "vb", "", "", 0); len(items) != 0 {
		t.Fatalf("listing shows a deleted key: %+v", items)
	}
	vs, _, _, _ := c.ListVersions(ctx, "vb", "", "", "", 0)
	if len(vs) != 2 || !vs[0].DeleteMarker || !vs[0].IsLatest || vs[1].Version != "v1" || vs[1].IsLatest {
		t.Fatalf("versions: %+v", vs)
	}
	if objs, _, _ := c.Scan(ctx, "", 100); len(objs) != 1 {
		t.Fatalf("scan should skip markers, got %d", len(objs))
	}
	if err := c.DeleteBucket(ctx, "vb"); !errors.Is(err, ErrBucketNotEmpty) {
		t.Fatalf("bucket with only a marker + history is not empty: %v", err)
	}

	// removing the marker brings the object back
	if err := c.DeleteVersion(ctx, "vb", "k", m.Version); err != nil {
		t.Fatal(err)
	}
	cur, err := c.GetObject(ctx, "vb", "k")
	if err != nil || cur.Version != "v1" {
		t.Fatalf("after removing marker: %v %v", cur, err)
	}
}

func TestDeleteSpecificVersionRetiresIt(t *testing.T) {
	ctx := context.Background()
	c, _ := vclient(t)
	for _, v := range []string{"v1", "v2", "v3"} {
		c.CommitObject(ctx, obj("vb", "k", v), nil)
	}
	if err := c.DeleteVersion(ctx, "vb", "k", "v2"); err != nil { // middle of the chain
		t.Fatal(err)
	}
	gc, _ := c.GCQueue(ctx, 0)
	if len(gc) != 1 || gc[0].Version != "v2" {
		t.Fatalf("gc %+v", gc)
	}
	if err := c.DeleteVersion(ctx, "vb", "k", "v2"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("second delete: %v", err)
	}
	if err := c.DeleteVersion(ctx, "vb", "k", "v3"); err != nil { // current: v1 takes over
		t.Fatal(err)
	}
	cur, _ := c.GetObject(ctx, "vb", "k")
	if cur.Version != "v1" {
		t.Fatalf("promoted %s", cur.Version)
	}
	if err := c.DeleteVersion(ctx, "vb", "k", "v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetObject(ctx, "vb", "k"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("key should be gone: %v", err)
	}
	if err := c.DeleteBucket(ctx, "vb"); err != nil {
		t.Fatalf("empty bucket: %v", err)
	}
}

func TestVersionListingPaging(t *testing.T) {
	ctx := context.Background()
	c, _ := vclient(t)
	want := 0
	for _, k := range []string{"a", "b", "c"} {
		for _, v := range []string{"1", "2", "3"} {
			c.CommitObject(ctx, obj("vb", k, k+v), nil)
			want++
		}
	}
	c.DeleteObject(ctx, "vb", "b")
	want++

	seen := map[string]bool{}
	ak, av := "", ""
	for pages := 0; pages < 20; pages++ {
		items, nk, nv, err := c.ListVersions(ctx, "vb", "", ak, av, 4)
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range items {
			id := it.Key + "/" + it.Version
			if seen[id] {
				t.Fatalf("repeated %s", id)
			}
			seen[id] = true
		}
		if nk == "" {
			break
		}
		ak, av = nk, nv
	}
	if len(seen) != want {
		t.Fatalf("paged %d versions, want %d", len(seen), want)
	}
}

func TestVersionedSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	c, f := vclient(t)
	c.CommitObject(ctx, obj("vb", "k", "v1"), nil)
	c.CommitObject(ctx, obj("vb", "k", "v2"), nil)
	c.DeleteObject(ctx, "vb", "k")

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	g := NewFSM()
	if err := g.Restore(snap); err != nil {
		t.Fatal(err)
	}
	c2 := &Client{direct{g}}
	vs, _, _, _ := c2.ListVersions(ctx, "vb", "", "", "", 0)
	if len(vs) != 3 || !vs[0].DeleteMarker || vs[1].Version != "v2" || vs[2].Version != "v1" {
		t.Fatalf("history after restore: %+v", vs)
	}
	if bs, _ := c2.ListBuckets(ctx); !bs[0].Versioning {
		t.Fatal("versioning flag lost")
	}
	if pl, _ := c2.Placements(ctx, []string{"v1", "v2"}); len(pl) != 2 {
		t.Fatalf("placements after restore: %d", len(pl))
	}
	if st, _ := c2.Stats(ctx); st.Objects != 2 {
		t.Fatalf("stats counted %d live versions", st.Objects)
	}
}

func TestUnversionedBucketUnchanged(t *testing.T) {
	ctx := context.Background()
	f := NewFSM()
	c := &Client{direct{f}}
	c.CreateBucket(ctx, "plain")
	c.CommitObject(ctx, obj("plain", "k", "v1"), nil)
	m, err := c.DeleteObject(ctx, "plain", "k")
	if err != nil || m != nil {
		t.Fatalf("plain delete: %v %v", m, err)
	}
	if gc, _ := c.GCQueue(ctx, 0); len(gc) != 1 {
		t.Fatalf("plain delete should retire: %+v", gc)
	}
}
