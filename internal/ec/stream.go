package ec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/monaaoua9/objstore/internal/erasure"
	"github.com/monaaoua9/objstore/internal/frame"
)

var ErrQuorum = errors.New("ec: too many shard writers failed")

type StreamResult struct {
	Size    int64
	ETag    string
	Stripes int
	Failed  []bool // per shard index: that writer errored at some point
}

// one encoded stripe, shared by all sink workers; the last one to finish returns the buffer
type job struct {
	buf    []byte
	shards [][]byte
	refs   atomic.Int32
}

func (j *job) release() {
	if j.refs.Add(-1) == 0 {
		putBuf(j.buf)
	}
}

// Stream reads src stripe by stripe, encodes, and fans the frames out to sinks (one per shard index).
// A sink that errors is dropped but the others keep going, as long as at least minAlive remain.
// Sinks are never closed here; the caller commits or aborts them based on the result.
func Stream(ctx context.Context, enc *erasure.Encoder, src io.Reader, shardSize int, sinks []io.Writer, minAlive int) (StreamResult, error) {
	k, m := enc.DataShards(), enc.ParityShards()
	if len(sinks) != k+m {
		return StreamResult{}, fmt.Errorf("ec: need %d sinks, got %d", k+m, len(sinks))
	}
	res := StreamResult{Failed: make([]bool, k+m)}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var alive atomic.Int32
	alive.Store(int32(k + m))
	var failMu sync.Mutex

	chans := make([]chan *job, k+m)
	var wg sync.WaitGroup
	for i := range sinks {
		chans[i] = make(chan *job, 2)
		wg.Add(1)
		go func() {
			defer wg.Done()
			dead := false
			for j := range chans[i] {
				if !dead {
					if err := frame.Write(sinks[i], j.shards[i]); err != nil {
						dead = true
						failMu.Lock()
						res.Failed[i] = true
						failMu.Unlock()
						if int(alive.Add(-1)) < minAlive {
							cancel()
						}
					}
				}
				j.release()
			}
		}()
	}

	stop := func() {
		for _, c := range chans {
			close(c)
		}
		wg.Wait()
	}

	h := sha256.New()
	stripeBytes := k * shardSize
	var err error
loop:
	for {
		if ctx.Err() != nil {
			err = ctx.Err()
			break
		}
		buf := getBuf((k + m) * shardSize)
		n, rerr := io.ReadFull(src, buf[:stripeBytes])
		if n == 0 {
			putBuf(buf)
			if rerr != io.EOF && rerr != io.ErrUnexpectedEOF && rerr != nil {
				err = rerr
			}
			break
		}
		if rerr != nil && rerr != io.ErrUnexpectedEOF && rerr != io.EOF {
			putBuf(buf)
			err = rerr
			break
		}
		h.Write(buf[:n])
		res.Size += int64(n)
		res.Stripes++

		per := erasure.ShardLen(n, k)
		// pooled buffers are dirty; the short last stripe needs real zero padding or parity is garbage
		clear(buf[n : k*per])
		j := &job{buf: buf, shards: make([][]byte, k+m)}
		for i := 0; i < k+m; i++ {
			j.shards[i] = buf[i*per : (i+1)*per : (i+1)*per]
		}
		if encErr := enc.Encode(j.shards); encErr != nil {
			putBuf(buf)
			err = encErr
			break
		}
		j.refs.Store(int32(k + m))
		for i := range chans {
			select {
			case chans[i] <- j:
			case <-ctx.Done():
				j.release()
			}
		}
		if rerr == io.ErrUnexpectedEOF || rerr == io.EOF {
			break loop
		}
	}
	stop()

	// our own cancel() on quorum loss surfaces as context.Canceled; report the real reason
	if int(alive.Load()) < minAlive && (err == nil || errors.Is(err, context.Canceled)) {
		err = ErrQuorum
	}
	if err == nil {
		err = ctx.Err() // parent cancelled mid-flight
	}
	res.ETag = hex.EncodeToString(h.Sum(nil))
	return res, err
}
