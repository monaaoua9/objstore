package meta

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func spillFSM(t *testing.T) (*FSM, *Client, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spill")
	f, err := NewFSMAt(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.Close)
	return f, &Client{direct{f}}, path
}

func TestSpillCompaction(t *testing.T) {
	old := compactMin
	compactMin = 20_000
	t.Cleanup(func() { compactMin = old })

	ctx := context.Background()
	f, c, path := spillFSM(t)
	c.CreateBucket(ctx, "b")
	c.EnableVersioning(ctx, "b")

	// overwrite a small key set many times so most records are dead, and move shards around on the
	// versions that stay live (a relocated record must be visible from the key, the version and the history)
	for round := 0; round < 40; round++ {
		for i := 0; i < 10; i++ {
			o := obj("b", fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d-%02d", round, i))
			if err := c.CommitObject(ctx, o, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := c.Relocate(ctx, "v00-03", 0, "a", "z", false); err != nil {
		t.Fatal(err)
	}
	if err := c.Relocate(ctx, "v39-03", 1, "b", "y", false); err != nil {
		t.Fatal(err)
	}

	// plain bucket: every overwrite leaves a dead record behind
	c.CreateBucket(ctx, "n")
	for round := 0; round < 40; round++ {
		for i := 0; i < 10; i++ {
			c.CommitObject(ctx, obj("n", fmt.Sprintf("k%02d", i), fmt.Sprintf("n%02d-%02d", round, i)), nil)
		}
	}
	before := f.st.end
	f.mu.Lock()
	f.compact()
	f.mu.Unlock()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if f.st.end >= before*3/4 || fi.Size() != f.st.end {
		t.Fatalf("compaction did not shrink the store: %d -> %d (file %d)", before, f.st.end, fi.Size())
	}

	o, err := c.GetObject(ctx, "b", "k03")
	if err != nil || o.Version != "v39-03" || o.Shards[1].Node != "y" {
		t.Fatalf("current k03: %+v %v", o, err)
	}
	h, err := c.GetObjectVersion(ctx, "b", "k03", "v00-03")
	if err != nil || h.Shards[0].Node != "z" {
		t.Fatalf("history k03: %+v %v", h, err)
	}
	vs, _, _, err := c.ListVersions(ctx, "b", "", "", "", 1000)
	if err != nil || len(vs) != 400 {
		t.Fatalf("want 400 versions, got %d (%v)", len(vs), err)
	}
	if st, _ := c.Stats(ctx); st.Objects != 410 || st.Bytes != 4100 {
		t.Fatalf("stats after compaction: %+v", st)
	}
}

func TestSpillSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	f, c, _ := spillFSM(t)
	c.CreateBucket(ctx, "b")
	c.EnableVersioning(ctx, "b")
	for r := 0; r < 3; r++ {
		for i := 0; i < 5; i++ {
			c.CommitObject(ctx, obj("b", fmt.Sprintf("k%d", i), fmt.Sprintf("v%d-%d", r, i)), nil)
		}
	}
	c.DeleteObject(ctx, "b", "k1")
	c.Relocate(ctx, "v0-2", 2, "c", "q", false)

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	g, err := NewFSMAt(filepath.Join(t.TempDir(), "spill"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.Close)
	if err := g.Restore(snap); err != nil {
		t.Fatal(err)
	}
	d := &Client{direct{g}}

	want, _, _, _ := c.ListVersions(ctx, "b", "", "", "", 1000)
	got, _, _, _ := d.ListVersions(ctx, "b", "", "", "", 1000)
	if len(want) != len(got) {
		t.Fatalf("versions: %d vs %d", len(want), len(got))
	}
	for i := range want {
		if want[i].Key != got[i].Key || want[i].Version != got[i].Version || want[i].DeleteMarker != got[i].DeleteMarker {
			t.Fatalf("row %d differs: %+v vs %+v", i, want[i], got[i])
		}
	}
	o, err := d.GetVersion(ctx, "v0-2")
	if err != nil || o.Shards[2].Node != "q" {
		t.Fatalf("relocation lost across snapshot: %+v %v", o, err)
	}
}
