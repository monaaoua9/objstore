package gateway

import "testing"

func TestParseRange(t *testing.T) {
	cases := []struct {
		h             string
		size          int64
		start, end    int64
		partial, good bool
	}{
		{"", 100, 0, 99, false, true},
		{"bytes=0-9", 100, 0, 9, true, true},
		{"bytes=90-", 100, 90, 99, true, true},
		{"bytes=90-500", 100, 90, 99, true, true}, // end clamps
		{"bytes=-10", 100, 90, 99, true, true},
		{"bytes=-500", 100, 0, 99, true, true}, // suffix bigger than object
		{"bytes=100-", 100, 0, 0, false, false},
		{"bytes=50-40", 100, 0, 0, false, false},
		{"bytes=-0", 100, 0, 0, false, false},
		{"bytes=0-1,5-6", 100, 0, 99, false, true}, // multi-range: ignored
		{"items=1-2", 100, 0, 99, false, true},
		{"bytes=0-5", 0, 0, 0, false, true}, // empty object
	}
	for _, c := range cases {
		s, e, p, ok := ParseRange(c.h, c.size)
		if ok != c.good || (ok && (s != c.start || e != c.end || p != c.partial)) {
			t.Errorf("%q/%d: got (%d,%d,%v,%v)", c.h, c.size, s, e, p, ok)
		}
	}
}

func TestBucketNames(t *testing.T) {
	for _, ok := range []string{"abc", "my-bucket", "a.b.c", "x1y"} {
		if !bucketRe.MatchString(ok) {
			t.Errorf("%q rejected", ok)
		}
	}
	for _, bad := range []string{"ab", "-abc", "abc-", "UPPER", "has_underscore", "a/b", ""} {
		if bucketRe.MatchString(bad) {
			t.Errorf("%q accepted", bad)
		}
	}
}
