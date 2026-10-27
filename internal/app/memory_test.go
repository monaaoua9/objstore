package app

import (
	"crypto/sha256"
	"encoding/binary"
	"io"
	"net/http"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// deterministic byte stream; lets us push gigabytes without ever holding them
type genReader struct {
	left int64
	ctr  uint64
	h    interface{ Write([]byte) (int, error) }
}

func (g *genReader) Read(p []byte) (int, error) {
	if g.left <= 0 {
		return 0, io.EOF
	}
	n := int64(len(p))
	if n > g.left {
		n = g.left
	}
	for i := int64(0); i+8 <= n; i += 8 {
		g.ctr = g.ctr*6364136223846793005 + 1442695040888963407
		binary.LittleEndian.PutUint64(p[i:], g.ctr)
	}
	g.left -= n
	if g.h != nil {
		g.h.Write(p[:n])
	}
	return int(n), nil
}

func peakHeap(stop <-chan struct{}, out *atomic.Uint64) {
	var ms runtime.MemStats
	for {
		select {
		case <-stop:
			return
		case <-time.After(10 * time.Millisecond):
		}
		runtime.ReadMemStats(&ms)
		if ms.HeapAlloc > out.Load() {
			out.Store(ms.HeapAlloc)
		}
	}
}

// 256 MiB through an in-process 5-node cluster must not cost anywhere near 256 MiB of heap,
// even though all five nodes plus the client share this one process.
func TestLargeObjectStreamsWithBoundedMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	const size = 256 << 20
	c := newCluster(t, 3, 2, 3, 2)
	c.mkbucket("n1", "big")
	runtime.GC()

	var peak atomic.Uint64
	stop := make(chan struct{})
	go peakHeap(stop, &peak)
	defer close(stop)

	var base runtime.MemStats
	runtime.ReadMemStats(&base)

	wantSum := sha256.New()
	src := &genReader{left: size, h: wantSum}
	req, _ := http.NewRequest("PUT", c.base("n4")+"/api/v1/buckets/big/objects/blob", src)
	req.ContentLength = size
	resp, err := c.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("put: %d", resp.StatusCode)
	}

	resp, err = c.client.Get(c.base("n5") + "/api/v1/buckets/big/objects/blob")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got := sha256.New()
	n, err := io.Copy(got, resp.Body)
	if err != nil || n != size {
		t.Fatalf("get: %d bytes, %v", n, err)
	}
	if string(got.Sum(nil)) != string(wantSum.Sum(nil)) {
		t.Fatal("256 MiB round trip corrupted")
	}

	used := int64(peak.Load()) - int64(base.HeapAlloc)
	t.Logf("peak heap growth during 256 MiB put+get: %d MiB", used>>20)
	if used > 96<<20 {
		t.Fatalf("heap grew by %d MiB, streaming is buffering", used>>20)
	}
}
