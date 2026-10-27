package erasure

import (
	"bytes"
	"math/rand"
	"testing"
)

func randShards(r *rand.Rand, k, m, size int) [][]byte {
	s := make([][]byte, k+m)
	for i := range s {
		s[i] = make([]byte, size)
		if i < k {
			r.Read(s[i])
		}
	}
	return s
}

func clone(s [][]byte) [][]byte {
	out := make([][]byte, len(s))
	for i := range s {
		out[i] = append([]byte(nil), s[i]...)
	}
	return out
}

func TestGFLaws(t *testing.T) {
	for a := 1; a < 256; a++ {
		if mulT[a][gfInv(byte(a))] != 1 {
			t.Fatalf("a*inv(a) != 1 for %d", a)
		}
	}
	// distributive
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 1000; i++ {
		a, b, c := byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256))
		if mulT[a][b^c] != mulT[a][b]^mulT[a][c] {
			t.Fatal("not distributive")
		}
	}
}

// every subset of up to m erasures must be recoverable
func TestAllErasurePatterns(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	for _, kc := range [][2]int{{2, 1}, {3, 2}, {4, 2}, {5, 3}, {6, 3}} {
		k, m := kc[0], kc[1]
		e, err := New(k, m)
		if err != nil {
			t.Fatal(err)
		}
		orig := randShards(r, k, m, 1021) // odd on purpose
		if err := e.Encode(orig); err != nil {
			t.Fatal(err)
		}
		if ok, _ := e.Verify(orig); !ok {
			t.Fatal("verify failed after encode")
		}
		n := k + m
		for mask := 0; mask < 1<<n; mask++ {
			lost := 0
			for b := 0; b < n; b++ {
				if mask>>b&1 == 1 {
					lost++
				}
			}
			if lost > m {
				continue
			}
			got := clone(orig)
			for b := 0; b < n; b++ {
				if mask>>b&1 == 1 {
					got[b] = nil
				}
			}
			if err := e.Reconstruct(got); err != nil {
				t.Fatalf("k=%d m=%d mask=%b: %v", k, m, mask, err)
			}
			for i := range got {
				if !bytes.Equal(got[i], orig[i]) {
					t.Fatalf("k=%d m=%d mask=%b shard %d differs", k, m, mask, i)
				}
			}
		}
	}
}

func TestTooManyLost(t *testing.T) {
	e, _ := New(4, 2)
	s := randShards(rand.New(rand.NewSource(7)), 4, 2, 64)
	e.Encode(s)
	s[0], s[1], s[5] = nil, nil, nil
	if err := e.Reconstruct(s); err == nil {
		t.Fatal("expected error")
	}
}

func TestLargeParallelMatchesSerial(t *testing.T) {
	e, _ := New(10, 4)
	r := rand.New(rand.NewSource(3))
	big := randShards(r, 10, 4, 1<<20)
	small := clone(big)
	e.Encode(big)

	// serial reference through mulRange directly
	e.mulRows(e.enc[10:], small[:10], small[10:])
	for i := 10; i < 14; i++ {
		if !bytes.Equal(big[i], small[i]) {
			t.Fatalf("parity %d mismatch", i)
		}
	}
	big[2], big[11], big[0], big[13] = nil, nil, nil, nil
	if err := e.Reconstruct(big); err != nil {
		t.Fatal(err)
	}
	for i := range big {
		if !bytes.Equal(big[i], small[i]) {
			t.Fatalf("shard %d wrong after reconstruct", i)
		}
	}
}

func TestBadParams(t *testing.T) {
	if _, err := New(0, 1); err == nil {
		t.Fatal("k=0 accepted")
	}
	if _, err := New(200, 100); err == nil {
		t.Fatal("k+m>256 accepted")
	}
}

func BenchmarkEncode10x4(b *testing.B) {
	e, _ := New(10, 4)
	s := randShards(rand.New(rand.NewSource(1)), 10, 4, 1<<20)
	b.SetBytes(10 << 20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Encode(s)
	}
}

func BenchmarkReconstruct10x4(b *testing.B) {
	e, _ := New(10, 4)
	s := randShards(rand.New(rand.NewSource(1)), 10, 4, 1<<20)
	e.Encode(s)
	b.SetBytes(10 << 20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x := append([][]byte(nil), s...)
		x[1], x[3], x[5], x[12] = nil, nil, nil, nil
		e.Reconstruct(x)
	}
}
