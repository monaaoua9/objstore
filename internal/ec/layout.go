// Package ec turns byte streams into striped, erasure-coded shard streams and back.
// Memory use is bounded by a few stripes per request regardless of object size.
package ec

import (
	"sync"

	"github.com/monaaoua9/objstore/internal/erasure"
	"github.com/monaaoua9/objstore/internal/frame"
	"github.com/monaaoua9/objstore/internal/meta"
)

type Layout struct {
	K, M      int
	ShardSize int // shard bytes in a full stripe
	Stripes   int
	Size      int64
}

func LayoutOf(o *meta.Object) Layout {
	return Layout{K: o.K, M: o.M, ShardSize: o.ShardSize, Stripes: o.Stripes, Size: o.Size}
}

func (l Layout) StripeBytes() int64 { return int64(l.K) * int64(l.ShardSize) }

// StripeData is how many real payload bytes stripe s carries.
func (l Layout) StripeData(s int) int {
	if s < l.Stripes-1 {
		return int(l.StripeBytes())
	}
	return int(l.Size - int64(l.Stripes-1)*l.StripeBytes())
}

// FrameLen is the shard payload length in stripe s (last stripe is zero-padded up to a multiple of K).
func (l Layout) FrameLen(s int) int {
	return erasure.ShardLen(l.StripeData(s), l.K)
}

// Offset is where stripe s begins inside any shard file.
func (l Layout) Offset(s int) int64 { return int64(s) * frame.Stride(l.ShardSize) }

// ShardFileSize is the exact on-disk size every shard of this object must have.
func (l Layout) ShardFileSize() int64 {
	if l.Stripes == 0 {
		return 0
	}
	full := int64(l.Stripes-1) * frame.Stride(l.ShardSize)
	return full + int64(frame.HeaderSize+l.FrameLen(l.Stripes-1))
}

func StripesFor(size int64, k, shardSize int) int {
	sb := int64(k) * int64(shardSize)
	return int((size + sb - 1) / sb)
}

// ---- shared helpers ----

var encoders sync.Map // [2]int -> *erasure.Encoder

func EncoderFor(k, m int) (*erasure.Encoder, error) {
	key := [2]int{k, m}
	if e, ok := encoders.Load(key); ok {
		return e.(*erasure.Encoder), nil
	}
	e, err := erasure.New(k, m)
	if err != nil {
		return nil, err
	}
	act, _ := encoders.LoadOrStore(key, e)
	return act.(*erasure.Encoder), nil
}

var pools sync.Map // int -> *sync.Pool

func getBuf(n int) []byte {
	p, ok := pools.Load(n)
	if !ok {
		p, _ = pools.LoadOrStore(n, &sync.Pool{})
	}
	if b, ok := p.(*sync.Pool).Get().(*[]byte); ok {
		return (*b)[:n]
	}
	return make([]byte, n)
}

func putBuf(b []byte) {
	n := cap(b)
	if n == 0 {
		return
	}
	b = b[:n]
	p, ok := pools.Load(n)
	if !ok {
		p, _ = pools.LoadOrStore(n, &sync.Pool{})
	}
	p.(*sync.Pool).Put(&b)
}
