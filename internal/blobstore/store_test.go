package blobstore

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/monaaoua9/objstore/internal/frame"
)

const ver = "00112233445566778899aabbccddeeff"

func framed(t *testing.T, payloads ...[]byte) []byte {
	t.Helper()
	var b bytes.Buffer
	for _, p := range payloads {
		if err := frame.Write(&b, p); err != nil {
			t.Fatal(err)
		}
	}
	return b.Bytes()
}

func rnd(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func TestPutOpenRoundTrip(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	a, b := rnd(1000), rnd(400)
	raw := framed(t, a, b)
	frames, n, err := s.Put(ver, 3, bytes.NewReader(raw))
	if err != nil || frames != 2 || n != int64(len(raw)) {
		t.Fatalf("put: %d %d %v", frames, n, err)
	}
	if s.Count() != 1 || s.Bytes() != n {
		t.Fatalf("counters: %d %d", s.Count(), s.Bytes())
	}
	f, err := s.Open(ver, 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got, _ := io.ReadAll(f)
	if !bytes.Equal(got, raw) {
		t.Fatal("content differs")
	}

	// offset reads land on the second frame
	f2, _ := s.Open(ver, 3, frame.Stride(1000))
	defer f2.Close()
	fr := frame.NewReader(f2)
	p, err := fr.Next(nil)
	if err != nil || !bytes.Equal(p, b) {
		t.Fatalf("offset read: %v", err)
	}
}

func TestPutRejectsCorruptAndTorn(t *testing.T) {
	s, _ := Open(t.TempDir())
	raw := framed(t, rnd(500), rnd(500))

	bad := append([]byte(nil), raw...)
	bad[frame.HeaderSize+10] ^= 0xff
	if _, _, err := s.Put(ver, 0, bytes.NewReader(bad)); !errors.Is(err, frame.ErrCorrupt) {
		t.Fatalf("corrupt frame accepted: %v", err)
	}
	if _, _, err := s.Put(ver, 1, bytes.NewReader(raw[:len(raw)-7])); err == nil {
		t.Fatal("torn upload accepted")
	}
	if s.Count() != 0 {
		t.Fatal("rejected upload left a shard")
	}
	ents, _ := os.ReadDir(s.Root() + "/tmp")
	if len(ents) != 0 {
		t.Fatalf("temp files leaked: %d", len(ents))
	}
}

func TestVerifyFindsBitrot(t *testing.T) {
	s, _ := Open(t.TempDir())
	s.Put(ver, 0, bytes.NewReader(framed(t, rnd(4096), rnd(4096), rnd(100))))
	if n, err := s.Verify(ver, 0, nil); err != nil || n != 3 {
		t.Fatalf("clean verify: %d %v", n, err)
	}

	p := s.path(ver, 0)
	b, _ := os.ReadFile(p)
	b[len(b)/2] ^= 0x01 // one flipped bit in the middle
	os.WriteFile(p, b, 0o644)

	if _, err := s.Verify(ver, 0, nil); !errors.Is(err, frame.ErrCorrupt) {
		t.Fatalf("bitrot not detected: %v", err)
	}
	if err := s.Quarantine(ver, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open(ver, 0, 0); !errors.Is(err, ErrNotFound) {
		t.Fatal("quarantined shard still readable")
	}
	if s.Count() != 0 {
		t.Fatal("count after quarantine")
	}
}

func TestBadNamesRejected(t *testing.T) {
	s, _ := Open(t.TempDir())
	for _, v := range []string{"../../etc/passwd", "short", "ZZZZZZZZZZZZZZZZ", ""} {
		if _, _, err := s.Put(v, 0, bytes.NewReader(nil)); !errors.Is(err, ErrBadName) {
			t.Errorf("%q accepted", v)
		}
	}
	if _, _, err := s.Put(ver, 999, bytes.NewReader(nil)); !errors.Is(err, ErrBadName) {
		t.Error("idx 999 accepted")
	}
}

func TestDeleteAndWalk(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	for i := 0; i < 3; i++ {
		s.Put(ver, i, bytes.NewReader(framed(t, rnd(10))))
	}
	if err := s.Delete(ver, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ver, 1); err != nil {
		t.Fatal("delete not idempotent")
	}
	seen := 0
	s.Walk(func(e Entry) error { seen++; return nil })
	if seen != 2 {
		t.Fatalf("walk saw %d", seen)
	}
	// reopening recounts from disk
	s2, _ := Open(dir)
	if s2.Count() != 2 {
		t.Fatalf("recount = %d", s2.Count())
	}
}

func TestLimiterSlowsReads(t *testing.T) {
	l := NewLimiter(1 << 20) // 1 MiB/s
	data := rnd(300 << 10)
	start := time.Now()
	io.Copy(io.Discard, l.Reader(bytes.NewReader(data)))
	if el := time.Since(start); el < 150*time.Millisecond {
		t.Fatalf("finished in %v, limiter did nothing", el)
	}
}
