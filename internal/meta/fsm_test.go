package meta

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// direct backend, no raft: Apply/Query hit the FSM synchronously
type direct struct{ f *FSM }

func (d direct) Apply(_ context.Context, c []byte) ([]byte, error) { return d.f.Apply(0, c), nil }
func (d direct) Query(_ context.Context, q []byte) ([]byte, error) { return d.f.Query(q), nil }

func obj(bucket, key, ver string) *Object {
	return &Object{Bucket: bucket, Key: key, Version: ver, Size: 10, K: 2, M: 1,
		Shards: []ShardLoc{{0, "a"}, {1, "b"}, {2, "c"}}}
}

func TestObjectLifecycle(t *testing.T) {
	ctx := context.Background()
	f := NewFSM()
	c := &Client{direct{f}}

	if err := c.CommitObject(ctx, obj("nb", "k", "v1"), nil); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("want bucket not found, got %v", err)
	}
	if err := c.CreateBucket(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	if err := c.CreateBucket(ctx, "b"); !errors.Is(err, ErrBucketExists) {
		t.Fatalf("dup bucket: %v", err)
	}
	if err := c.CommitObject(ctx, obj("b", "k", "v1"), []int{2}); err != nil {
		t.Fatal(err)
	}
	if d, _ := c.Damaged(ctx, 0); len(d) != 1 || d[0].Index != 2 {
		t.Fatalf("degraded write should queue repair, got %+v", d)
	}
	if err := c.DeleteBucket(ctx, "b"); !errors.Is(err, ErrBucketNotEmpty) {
		t.Fatalf("got %v", err)
	}

	// overwrite retires v1 into the gc queue and drops its damage records
	if err := c.CommitObject(ctx, obj("b", "k", "v2"), nil); err != nil {
		t.Fatal(err)
	}
	gc, _ := c.GCQueue(ctx, 0)
	if len(gc) != 1 || gc[0].Version != "v1" || len(gc[0].Shards) != 3 {
		t.Fatalf("gc = %+v", gc)
	}
	if d, _ := c.Damaged(ctx, 0); len(d) != 0 {
		t.Fatalf("stale damage left: %+v", d)
	}
	if got, _ := c.GetObject(ctx, "b", "k"); got.Version != "v2" {
		t.Fatal("overwrite lost")
	}
	// v1 is retired (pending gc), zzz never existed: only the live version has a placement
	if live, _ := c.Placements(ctx, []string{"v1", "v2", "zzz"}); len(live) != 1 || live["v2"] == nil {
		t.Fatalf("placements = %v", live)
	}
	c.GCDone(ctx, "v1")
	if gc, _ := c.GCQueue(ctx, 0); len(gc) != 0 {
		t.Fatal("gc_done ignored")
	}

	if _, err := c.DeleteObject(ctx, "b", "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteObject(ctx, "b", "k"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("got %v", err)
	}
	if err := c.DeleteBucket(ctx, "b"); err != nil {
		t.Fatal(err)
	}
}

func TestRelocate(t *testing.T) {
	ctx := context.Background()
	c := &Client{direct{NewFSM()}}
	c.CreateBucket(ctx, "b")
	c.CommitObject(ctx, obj("b", "k", "v1"), []int{1})

	if err := c.Relocate(ctx, "v1", 1, "WRONG", "z", true); !errors.Is(err, ErrStalePlacement) {
		t.Fatalf("got %v", err)
	}
	if err := c.Relocate(ctx, "v1", 1, "b", "z", true); err != nil {
		t.Fatal(err)
	}
	o, _ := c.GetObject(ctx, "b", "k")
	if o.Shards[1].Node != "z" {
		t.Fatal("not moved")
	}
	if d, _ := c.Damaged(ctx, 0); len(d) != 0 {
		t.Fatal("damage not cleared")
	}
	// relocating something that no longer exists is a no-op, not an error
	if err := c.Relocate(ctx, "gone", 0, "a", "b", false); err != nil {
		t.Fatal(err)
	}
}

func TestListPaging(t *testing.T) {
	ctx := context.Background()
	c := &Client{direct{NewFSM()}}
	c.CreateBucket(ctx, "b")
	for i := 0; i < 25; i++ {
		c.CommitObject(ctx, obj("b", fmt.Sprintf("logs/%02d", i), fmt.Sprint("v", i)), nil)
	}
	c.CommitObject(ctx, obj("b", "other", "vo"), nil)

	var got []string
	after := ""
	pages := 0
	for {
		items, next, err := c.ListObjects(ctx, "b", "logs/", after, 10)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, it := range items {
			got = append(got, it.Key)
		}
		if next == "" {
			break
		}
		after = next
	}
	if len(got) != 25 || pages != 3 {
		t.Fatalf("got %d keys over %d pages", len(got), pages)
	}
	for i, k := range got {
		if k != fmt.Sprintf("logs/%02d", i) {
			t.Fatalf("order wrong at %d: %s", i, k)
		}
	}
}

func TestScanAcrossBuckets(t *testing.T) {
	ctx := context.Background()
	c := &Client{direct{NewFSM()}}
	for _, b := range []string{"a", "b"} {
		c.CreateBucket(ctx, b)
		for i := 0; i < 7; i++ {
			c.CommitObject(ctx, obj(b, fmt.Sprint("k", i), b+fmt.Sprint(i)), nil)
		}
	}
	n, cur := 0, ""
	for {
		objs, next, err := c.Scan(ctx, cur, 4)
		if err != nil {
			t.Fatal(err)
		}
		n += len(objs)
		if next == "" {
			break
		}
		cur = next
	}
	if n != 14 {
		t.Fatalf("scan saw %d", n)
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	f := NewFSM()
	c := &Client{direct{f}}
	c.CreateBucket(ctx, "b")
	c.SetNode(ctx, Node{ID: "n1", Addr: "x:1"})
	c.CommitObject(ctx, obj("b", "k1", "v1"), []int{0})
	c.CommitObject(ctx, obj("b", "k1", "v2"), nil)
	c.CommitObject(ctx, obj("b", "k2", "v3"), []int{1})

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	g := NewFSM()
	if err := g.Restore(snap); err != nil {
		t.Fatal(err)
	}
	c2 := &Client{direct{g}}
	s1, _ := c.Stats(ctx)
	s2, _ := c2.Stats(ctx)
	if s1 != s2 {
		t.Fatalf("stats differ: %+v vs %+v", s1, s2)
	}
	if gc, _ := c2.GCQueue(ctx, 0); len(gc) != 1 {
		t.Fatal("gc queue lost in snapshot")
	}
	if ns, _ := c2.Nodes(ctx); len(ns) != 1 || ns[0].State != NodeUp {
		t.Fatalf("nodes: %+v", ns)
	}
}
