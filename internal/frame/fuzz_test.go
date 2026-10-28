package frame

import (
	"bytes"
	"io"
	"testing"
)

func FuzzReaderNeverPanics(f *testing.F) {
	var good bytes.Buffer
	Write(&good, []byte("hello"))
	Write(&good, nil)
	f.Add(good.Bytes())
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add(good.Bytes()[:20])
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data))
		var buf []byte
		for i := 0; i < 1000; i++ {
			p, err := r.Next(buf)
			if err != nil {
				return
			}
			buf = p
		}
	})
}

func FuzzRoundTrip(f *testing.F) {
	f.Add([]byte("abc"), []byte{})
	f.Fuzz(func(t *testing.T, a, b []byte) {
		var w bytes.Buffer
		if Write(&w, a) != nil || Write(&w, b) != nil {
			t.Fatal("write to buffer failed")
		}
		raw := w.Bytes()
		r := NewReader(bytes.NewReader(raw))
		for _, want := range [][]byte{a, b} {
			got, err := r.Next(nil)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("round trip: %v", err)
			}
		}
		if _, err := r.Next(nil); err != io.EOF {
			t.Fatalf("want clean EOF, got %v", err)
		}
		// a flipped byte anywhere must be noticed, never silently accepted
		for _, at := range []int{0, len(raw) / 2, len(raw) - 1} {
			bad := append([]byte(nil), raw...)
			bad[at] ^= 0x40
			rr := NewReader(bytes.NewReader(bad))
			ok := true
			for _, want := range [][]byte{a, b} {
				got, err := rr.Next(nil)
				if err != nil || !bytes.Equal(got, want) {
					ok = false
					break
				}
			}
			if ok {
				t.Fatalf("corruption at byte %d went unnoticed", at)
			}
		}
	})
}
