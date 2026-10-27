package ec

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/monaaoua9/objstore/internal/erasure"
	"github.com/monaaoua9/objstore/internal/frame"
)

var ErrUnrecoverable = errors.New("ec: fewer than k healthy shards")

// Opener hands out a raw framed stream for shard idx starting at a byte offset.
type Opener interface {
	OpenShard(ctx context.Context, idx int, offset int64) (io.ReadCloser, error)
}

type BadShard struct {
	Index int
	Err   error
}

type laneMsg struct {
	buf []byte
	err error
}

type lane struct {
	idx    int
	ch     chan laneMsg
	cancel context.CancelFunc
}

// Decoder yields the K data shards of consecutive stripes, using data shards when it can and
// swapping in parity lanes (and reconstructing) the moment a shard goes bad.
type Decoder struct {
	ctx    context.Context
	cancel context.CancelFunc
	lay    Layout
	enc    *erasure.Encoder
	op     Opener

	stripe int
	active []*lane
	spare  []int
	bad    []BadShard
	pooled [][]byte
}

// NewDecoder starts at startStripe. exclude lists shard indexes that must not be read (e.g. the one being rebuilt).
func NewDecoder(ctx context.Context, lay Layout, op Opener, startStripe int, exclude map[int]bool) (*Decoder, error) {
	enc, err := EncoderFor(lay.K, lay.M)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	d := &Decoder{ctx: ctx, cancel: cancel, lay: lay, enc: enc, op: op, stripe: startStripe}
	var order []int
	for i := 0; i < lay.K+lay.M; i++ {
		if !exclude[i] {
			order = append(order, i)
		}
	}
	if len(order) < lay.K {
		cancel()
		return nil, ErrUnrecoverable
	}
	for _, i := range order[:lay.K] {
		d.active = append(d.active, d.startLane(i))
	}
	d.spare = order[lay.K:]
	return d, nil
}

func (d *Decoder) startLane(idx int) *lane {
	ctx, cancel := context.WithCancel(d.ctx)
	l := &lane{idx: idx, ch: make(chan laneMsg, 2), cancel: cancel}
	from := d.stripe
	lay := d.lay
	go func() {
		defer close(l.ch)
		send := func(m laneMsg) bool {
			select {
			case l.ch <- m:
				return true
			case <-ctx.Done():
				return false
			}
		}
		rc, err := d.op.OpenShard(ctx, idx, lay.Offset(from))
		if err != nil {
			send(laneMsg{err: err})
			return
		}
		defer rc.Close()
		fr := frame.NewReader(bufio.NewReaderSize(rc, 64<<10))
		for s := from; s < lay.Stripes; s++ {
			b, err := fr.Next(getBuf(lay.ShardSize)[:0])
			if err == io.EOF {
				err = io.ErrUnexpectedEOF // shard shorter than metadata claims
			}
			if err == nil && len(b) != lay.FrameLen(s) {
				err = fmt.Errorf("stripe %d: frame is %d bytes, want %d: %w", s, len(b), lay.FrameLen(s), frame.ErrCorrupt)
			}
			if err != nil {
				send(laneMsg{err: err})
				return
			}
			if !send(laneMsg{buf: b}) {
				putBuf(b)
				return
			}
		}
	}()
	return l
}

// Next returns the K data shards for the next stripe plus how many payload bytes they carry.
// The slices are only valid until the following Next or Close.
func (d *Decoder) Next() ([][]byte, int, error) {
	if d.stripe >= d.lay.Stripes {
		return nil, 0, io.EOF
	}
	for _, b := range d.pooled {
		putBuf(b)
	}
	d.pooled = d.pooled[:0]

	k := d.lay.K
	shards := make([][]byte, k+d.lay.M)
	for i := 0; i < len(d.active); {
		l := d.active[i]
		var msg laneMsg
		select {
		case msg = <-l.ch:
		case <-d.ctx.Done():
			return nil, 0, d.ctx.Err()
		}
		if msg.err == nil && msg.buf != nil {
			shards[l.idx] = msg.buf
			d.pooled = append(d.pooled, msg.buf)
			i++
			continue
		}
		if msg.err == nil { // channel closed without a value: lane ended early
			msg.err = io.ErrUnexpectedEOF
		}
		d.drop(i, msg.err)
		if len(d.spare) == 0 {
			return nil, 0, fmt.Errorf("%w (stripe %d, bad: %v)", ErrUnrecoverable, d.stripe, d.bad)
		}
		d.active = append(d.active, d.startLane(d.spare[0]))
		d.spare = d.spare[1:]
	}

	need := false
	for i := 0; i < k; i++ {
		if shards[i] == nil {
			need = true
			break
		}
	}
	if need {
		if err := d.enc.ReconstructData(shards); err != nil {
			return nil, 0, err
		}
	}
	n := d.lay.StripeData(d.stripe)
	d.stripe++
	return shards[:k], n, nil
}

func (d *Decoder) drop(i int, err error) {
	l := d.active[i]
	l.cancel()
	d.bad = append(d.bad, BadShard{l.idx, err})
	d.active = append(d.active[:i], d.active[i+1:]...)
}

// Bad lists shards that failed so far, in the order they failed.
func (d *Decoder) Bad() []BadShard { return d.bad }

func (d *Decoder) Close() {
	d.cancel()
	for _, b := range d.pooled {
		putBuf(b)
	}
	d.pooled = nil
}

// Rebuild regenerates shard `target` for the whole object and writes its frames to w.
// It reads k healthy shards other than target.
func Rebuild(ctx context.Context, lay Layout, op Opener, target int, w io.Writer) (*Decoder, error) {
	d, err := NewDecoder(ctx, lay, op, 0, map[int]bool{target: true})
	if err != nil {
		return nil, err
	}
	defer d.Close()
	enc := d.enc
	for {
		data, _, err := d.Next()
		if err == io.EOF {
			return d, nil
		}
		if err != nil {
			return d, err
		}
		if target < lay.K {
			err = frame.Write(w, data[target])
		} else {
			full := make([][]byte, lay.K+lay.M)
			copy(full, data)
			sz := len(data[0])
			for i := lay.K; i < lay.K+lay.M; i++ {
				full[i] = make([]byte, sz)
			}
			if err = enc.Encode(full); err == nil {
				err = frame.Write(w, full[target])
			}
		}
		if err != nil {
			return d, err
		}
	}
}
