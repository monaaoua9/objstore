package meta

import (
	"context"
	"errors"
	"testing"
)

func TestMultipartLifecycle(t *testing.T) {
	ctx := context.Background()
	f := NewFSM()
	c := &Client{direct{f}}
	c.CreateBucket(ctx, "b")
	if err := c.CreateUpload(ctx, Upload{ID: "up1", Bucket: "nope", Key: "k"}); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("got %v", err)
	}
	c.CreateUpload(ctx, Upload{ID: "up1", Bucket: "b", Key: "big"})

	part := func(n int, ver string, size int64) *Object {
		o := obj(MPUBucket, PartKey("up1", n), ver)
		o.Size, o.ETag = size, "etag-"+ver
		return o
	}
	c.CommitObject(ctx, part(1, "p1", 10), nil)
	c.CommitObject(ctx, part(2, "p2old", 5), nil)
	c.CommitObject(ctx, part(2, "p2", 7), nil) // re-upload of part 2 retires the first attempt
	c.CommitObject(ctx, part(3, "p3", 1), nil) // never selected, must be discarded
	if bs, _ := c.ListBuckets(ctx); len(bs) != 1 {
		t.Fatalf("staging bucket leaked into listing: %+v", bs)
	}

	if _, err := c.CompleteUpload(ctx, "up1", []PartSel{{1, "etag-p1"}, {2, "wrong"}}); !errors.Is(err, ErrInvalidPart) {
		t.Fatalf("etag mismatch accepted: %v", err)
	}
	if _, err := c.CompleteUpload(ctx, "up1", []PartSel{{2, "etag-p2"}, {1, "etag-p1"}}); !errors.Is(err, ErrInvalidPart) {
		t.Fatalf("unordered parts accepted: %v", err)
	}

	m, err := c.CompleteUpload(ctx, "up1", []PartSel{{1, "etag-p1"}, {2, "etag-p2"}})
	if err != nil {
		t.Fatal(err)
	}
	if m.Size != 17 || len(m.Parts) != 2 || m.ETag[:3] != "mp-" || m.ETag[len(m.ETag)-2:] != "-2" {
		t.Fatalf("manifest = %+v", m)
	}
	if got, _ := c.GetObject(ctx, "b", "big"); got == nil || got.Version != "up1" {
		t.Fatal("manifest not visible")
	}
	if _, err := c.GetUpload(ctx, "up1"); !errors.Is(err, ErrUploadNotFound) {
		t.Fatal("upload session survived completion")
	}
	if gc, _ := c.GCQueue(ctx, 0); len(gc) != 2 { // p2old and p3
		t.Fatalf("gc = %+v", gc)
	}

	// scan exposes the parts so audit and repair can see their shards
	objs, _, _ := c.Scan(ctx, "", 100)
	seen := map[string]bool{}
	for _, o := range objs {
		seen[o.Version] = true
	}
	if !seen["p1"] || !seen["p2"] || !seen["up1"] {
		t.Fatalf("scan missed parts: %v", seen)
	}

	snap, _ := f.Snapshot()
	g := NewFSM()
	if err := g.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if po, err := (&Client{direct{g}}).GetVersion(ctx, "p2"); err != nil || po.Size != 7 {
		t.Fatalf("part lost in snapshot: %v", err)
	}

	c.DeleteObject(ctx, "b", "big")
	if gc, _ := c.GCQueue(ctx, 0); len(gc) != 4 {
		t.Fatalf("gc after delete = %d entries", len(gc))
	}
	if _, err := c.GetVersion(ctx, "p1"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatal("part still live after parent delete")
	}
}

func TestAbortUpload(t *testing.T) {
	ctx := context.Background()
	c := &Client{direct{NewFSM()}}
	c.CreateBucket(ctx, "b")
	c.CreateUpload(ctx, Upload{ID: "u", Bucket: "b", Key: "k"})
	o := obj(MPUBucket, PartKey("u", 1), "px")
	o.ETag = "e"
	c.CommitObject(ctx, o, nil)
	if err := c.AbortUpload(ctx, "u"); err != nil {
		t.Fatal(err)
	}
	if gc, _ := c.GCQueue(ctx, 0); len(gc) != 1 || gc[0].Version != "px" {
		t.Fatalf("gc = %+v", gc)
	}
	if err := c.AbortUpload(ctx, "u"); !errors.Is(err, ErrUploadNotFound) {
		t.Fatalf("got %v", err)
	}
}
