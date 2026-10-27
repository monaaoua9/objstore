package frame

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestRoundTripAndEOF(t *testing.T) {
	var b bytes.Buffer
	for _, p := range []string{"hello", "", "world!"} {
		if err := Write(&b, []byte(p)); err != nil {
			t.Fatal(err)
		}
	}
	r := NewReader(&b)
	var buf []byte
	for _, want := range []string{"hello", "", "world!"} {
		got, err := r.Next(buf)
		if err != nil || string(got) != want {
			t.Fatalf("got %q, %v want %q", got, err, want)
		}
	}
	if _, err := r.Next(nil); err != io.EOF {
		t.Fatalf("want clean EOF, got %v", err)
	}
	if r.Count() != 3 {
		t.Fatal(r.Count())
	}
}

func TestDetectsDamage(t *testing.T) {
	var b bytes.Buffer
	Write(&b, []byte("0123456789"))
	raw := b.Bytes()

	flip := append([]byte(nil), raw...)
	flip[HeaderSize+3] ^= 1
	if _, err := NewReader(bytes.NewReader(flip)).Next(nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("payload flip: %v", err)
	}
	flipSum := append([]byte(nil), raw...)
	flipSum[10] ^= 1
	if _, err := NewReader(bytes.NewReader(flipSum)).Next(nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("checksum flip: %v", err)
	}
	for _, cut := range []int{3, HeaderSize, len(raw) - 1} {
		if _, err := NewReader(bytes.NewReader(raw[:cut])).Next(nil); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("cut at %d: %v", cut, err)
		}
	}
	huge := append([]byte(nil), raw...)
	huge[3] = 0x7f // absurd length must not allocate gigabytes
	if _, err := NewReader(bytes.NewReader(huge)).Next(nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("huge length: %v", err)
	}
}
