package ec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/monaaoua9/objstore/internal/frame"
)

type memShards struct {
	data [][]byte
	// per-shard fault injection
	missing map[int]bool
}

func (m *memShards) OpenShard(_ context.Context, idx int, off int64) (io.ReadCloser, error) {
	if m.missing[idx] {
		return nil, errors.New("node down")
	}
	if off > int64(len(m.data[idx])) {
		return nil, errors.New("offset past end")
	}
	return io.NopCloser(bytes.NewReader(m.data[idx][off:])), nil
}

type failingWriter struct{ after int }

func (f *failingWriter) Write(p []byte) (int, error) {
	if f.after <= 0 {
		return 0, errors.New("boom")
	}
	f.after--
	return len(p), nil
}

func encodeObject(t *testing.T, k, m, shardSize int, payload []byte) (*memShards, Layout, StreamResult) {
	t.Helper()
	enc, _ := EncoderFor(k, m)
	bufs := make([]*bytes.Buffer, k+m)
	sinks := make([]io.Writer, k+m)
	for i := range bufs {
		bufs[i] = new(bytes.Buffer)
		sinks[i] = bufs[i]
	}
	res, err := Stream(context.Background(), enc, bytes.NewReader(payload), shardSize, sinks, k+1)
	if err != nil {
		t.Fatal(err)
	}
	ms := &memShards{missing: map[int]bool{}}
	for _, b := range bufs {
		ms.data = append(ms.data, b.Bytes())
	}
	lay := Layout{K: k, M: m, ShardSize: shardSize, Stripes: res.Stripes, Size: res.Size}
	return ms, lay, res
}

func readAll(t *testing.T, d *Decoder, lay Layout, skip int64) []byte {
	t.Helper()
	var out []byte
	for {
		data, n, err := d.Next()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		var stripe []byte
		for _, s := range data {
			stripe = append(stripe, s...)
		}
		out = append(out, stripe[:n]...)
	}
}

func payloadOf(n int) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(int64(n))).Read(b)
	return b
}

func TestRoundTripSizes(t *testing.T) {
	const k, m, ss = 4, 2, 1024
	for _, n := range []int{0, 1, 3, 4, 5, ss*k - 1, ss * k, ss*k + 1, 3*ss*k + 17, 10 * ss * k} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			p := payloadOf(n)
			ms, lay, res := encodeObject(t, k, m, ss, p)
			sum := sha256.Sum256(p)
			if res.ETag != hex.EncodeToString(sum[:]) || res.Size != int64(n) {
				t.Fatal("etag/size wrong")
			}
			for i, s := range ms.data {
				if int64(len(s)) != lay.ShardFileSize() {
					t.Fatalf("shard %d is %d bytes, layout says %d", i, len(s), lay.ShardFileSize())
				}
			}
			d, err := NewDecoder(context.Background(), lay, ms, 0, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if got := readAll(t, d, lay, 0); !bytes.Equal(got, p) {
				t.Fatal("round trip mismatch")
			}
			if len(d.Bad()) != 0 {
				t.Fatalf("healthy read reported bad shards: %v", d.Bad())
			}
		})
	}
}

func TestDegradedReads(t *testing.T) {
	const k, m, ss = 4, 2, 512
	p := payloadOf(7*ss*k + 100)
	cases := map[string][]int{
		"one data":      {1},
		"two data":      {0, 3},
		"data+parity":   {2, 5},
		"both parities": {4, 5},
	}
	for name, down := range cases {
		t.Run(name, func(t *testing.T) {
			ms, lay, _ := encodeObject(t, k, m, ss, p)
			for _, i := range down {
				ms.missing[i] = true
			}
			d, err := NewDecoder(context.Background(), lay, ms, 0, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if !bytes.Equal(readAll(t, d, lay, 0), p) {
				t.Fatal("degraded read returned wrong bytes")
			}
			// only shards we actually tried to open can be reported bad
			want := 0
			for _, i := range down {
				if i < k {
					want++
				}
			}
			if len(d.Bad()) != want {
				t.Fatalf("bad = %v, want %d entries", d.Bad(), want)
			}
		})
	}
}

func TestBitrotMidStreamFallsBackToParity(t *testing.T) {
	const k, m, ss = 3, 2, 256
	p := payloadOf(10 * ss * k)
	ms, lay, _ := encodeObject(t, k, m, ss, p)
	// flip a bit inside stripe 6 of data shard 1: early stripes read fine, then the lane dies
	ms.data[1][6*int(frame.Stride(ss))+frame.HeaderSize+5] ^= 0x40

	d, _ := NewDecoder(context.Background(), lay, ms, 0, nil)
	defer d.Close()
	if !bytes.Equal(readAll(t, d, lay, 0), p) {
		t.Fatal("wrong bytes after mid-stream corruption")
	}
	bad := d.Bad()
	if len(bad) != 1 || bad[0].Index != 1 || !errors.Is(bad[0].Err, frame.ErrCorrupt) {
		t.Fatalf("bad = %+v", bad)
	}
}

func TestTooManyFailures(t *testing.T) {
	const k, m, ss = 4, 2, 256
	ms, lay, _ := encodeObject(t, k, m, ss, payloadOf(5*ss*k))
	ms.missing[0], ms.missing[1], ms.missing[5] = true, true, true
	d, _ := NewDecoder(context.Background(), lay, ms, 0, nil)
	defer d.Close()
	var err error
	for err == nil {
		_, _, err = d.Next()
	}
	if !errors.Is(err, ErrUnrecoverable) {
		t.Fatalf("want ErrUnrecoverable, got %v", err)
	}
}

func TestRangeStart(t *testing.T) {
	const k, m, ss = 4, 2, 128
	p := payloadOf(9*ss*k + 5)
	ms, lay, _ := encodeObject(t, k, m, ss, p)
	ms.missing[2] = true // force reconstruction on the ranged read too
	d, _ := NewDecoder(context.Background(), lay, ms, 5, nil)
	defer d.Close()
	got := readAll(t, d, lay, 0)
	if !bytes.Equal(got, p[5*ss*k:]) {
		t.Fatal("ranged decode mismatch")
	}
}

func TestRebuildEveryShard(t *testing.T) {
	const k, m, ss = 4, 3, 300
	p := payloadOf(6*ss*k + 33)
	ms, lay, _ := encodeObject(t, k, m, ss, p)
	for target := 0; target < k+m; target++ {
		var out bytes.Buffer
		if _, err := Rebuild(context.Background(), lay, ms, target, &out); err != nil {
			t.Fatalf("target %d: %v", target, err)
		}
		if !bytes.Equal(out.Bytes(), ms.data[target]) {
			t.Fatalf("rebuilt shard %d differs from original", target)
		}
	}
	// and with a couple of sources gone
	ms.missing[0], ms.missing[6] = true, true
	var out bytes.Buffer
	if _, err := Rebuild(context.Background(), lay, ms, 3, &out); err != nil || !bytes.Equal(out.Bytes(), ms.data[3]) {
		t.Fatalf("rebuild with failures: %v", err)
	}
}

func TestStreamSurvivesFailedSinksUntilQuorum(t *testing.T) {
	const k, m, ss = 4, 2, 256
	enc, _ := EncoderFor(k, m)
	mk := func(failing ...int) []io.Writer {
		sinks := make([]io.Writer, k+m)
		for i := range sinks {
			sinks[i] = io.Discard
		}
		for _, f := range failing {
			sinks[f] = &failingWriter{after: 2}
		}
		return sinks
	}
	src := func() io.Reader { return bytes.NewReader(payloadOf(20 * ss * k)) }

	res, err := Stream(context.Background(), enc, src(), ss, mk(1), k+1)
	if err != nil || !res.Failed[1] {
		t.Fatalf("one failure should be tolerated: %v %v", err, res.Failed)
	}
	res, err = Stream(context.Background(), enc, src(), ss, mk(0, 2), k+1)
	if !errors.Is(err, ErrQuorum) {
		t.Fatalf("expected quorum error, got %v", err)
	}
}

func TestStreamPropagatesReadError(t *testing.T) {
	enc, _ := EncoderFor(2, 1)
	r := io.MultiReader(bytes.NewReader(payloadOf(5000)), errReader{})
	sinks := []io.Writer{io.Discard, io.Discard, io.Discard}
	if _, err := Stream(context.Background(), enc, r, 1024, sinks, 3); err == nil || err.Error() != "disk on fire" {
		t.Fatalf("got %v", err)
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("disk on fire") }

func TestStreamHonoursCancel(t *testing.T) {
	enc, _ := EncoderFor(2, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sinks := []io.Writer{io.Discard, io.Discard, io.Discard}
	if _, err := Stream(ctx, enc, bytes.NewReader(payloadOf(100000)), 1024, sinks, 3); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}
