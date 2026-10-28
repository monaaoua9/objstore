package gateway

import "testing"

func FuzzParseRange(f *testing.F) {
	for _, s := range []string{"bytes=0-9", "bytes=-5", "bytes=5-", "bytes=9-3", "bytes=0-0,5-6", "", "bytes=-", "bytes=99999999999999999999-"} {
		f.Add(s, int64(100))
	}
	f.Fuzz(func(t *testing.T, h string, size int64) {
		if size < 0 {
			return
		}
		start, end, partial, ok := ParseRange(h, size)
		if !ok {
			return
		}
		if size == 0 {
			return
		}
		if start < 0 || end < start || end >= size {
			t.Fatalf("%q on %d bytes -> %d-%d", h, size, start, end)
		}
		if !partial && (start != 0 || end != size-1) {
			t.Fatalf("%q: full response with a sub-range %d-%d", h, start, end)
		}
	})
}
