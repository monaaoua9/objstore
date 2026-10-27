// Package erasure implements systematic Reed-Solomon coding over GF(2^8).
package erasure

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
)

var (
	ErrTooFewShards = errors.New("erasure: not enough shards to reconstruct")
	ErrShardSize    = errors.New("erasure: shards must all be the same non-zero size")
)

const (
	parThreshold = 256 << 10 // below this, goroutine fan-out costs more than it saves
	minChunk     = 64 << 10
	invCacheMax  = 512
)

type Encoder struct {
	k, m int
	enc  matrix // (k+m) x k, top k rows are identity

	mu  sync.Mutex
	inv map[string]matrix
}

func New(dataShards, parityShards int) (*Encoder, error) {
	if dataShards < 1 || parityShards < 1 {
		return nil, errors.New("erasure: need at least one data and one parity shard")
	}
	if dataShards+parityShards > 256 {
		return nil, errors.New("erasure: k+m must be <= 256")
	}
	e := &Encoder{k: dataShards, m: parityShards, inv: map[string]matrix{}}
	e.enc = newMatrix(dataShards+parityShards, dataShards)
	for i := 0; i < dataShards; i++ {
		e.enc[i][i] = 1
	}
	copy(e.enc[dataShards:], cauchy(dataShards, parityShards))
	return e, nil
}

func (e *Encoder) DataShards() int   { return e.k }
func (e *Encoder) ParityShards() int { return e.m }
func (e *Encoder) TotalShards() int  { return e.k + e.m }

// Encode fills shards[k:] from shards[:k]. All shards must have the same length.
func (e *Encoder) Encode(shards [][]byte) error {
	if err := e.checkSizes(shards, false); err != nil {
		return err
	}
	e.mulRows(e.enc[e.k:], shards[:e.k], shards[e.k:])
	return nil
}

// Verify recomputes parity and compares. Only for tests / offline audits, the hot path uses checksums.
func (e *Encoder) Verify(shards [][]byte) (bool, error) {
	if err := e.checkSizes(shards, false); err != nil {
		return false, err
	}
	tmp := make([][]byte, e.m)
	for i := range tmp {
		tmp[i] = make([]byte, len(shards[0]))
	}
	e.mulRows(e.enc[e.k:], shards[:e.k], tmp)
	for i := range tmp {
		if string(tmp[i]) != string(shards[e.k+i]) {
			return false, nil
		}
	}
	return true, nil
}

// Reconstruct rebuilds every missing shard (nil or empty slice == missing), data and parity.
func (e *Encoder) Reconstruct(shards [][]byte) error { return e.reconstruct(shards, false) }

// ReconstructData only rebuilds the data shards; cheaper on the read path.
func (e *Encoder) ReconstructData(shards [][]byte) error { return e.reconstruct(shards, true) }

func (e *Encoder) reconstruct(shards [][]byte, dataOnly bool) error {
	if err := e.checkSizes(shards, true); err != nil {
		return err
	}
	size := 0
	present := make([]int, 0, e.k)
	for i, s := range shards {
		if len(s) > 0 {
			size = len(s)
			if len(present) < e.k {
				present = append(present, i)
			}
		}
	}
	nPresent := 0
	for _, s := range shards {
		if len(s) > 0 {
			nPresent++
		}
	}
	if nPresent < e.k {
		return fmt.Errorf("%w: have %d, need %d", ErrTooFewShards, nPresent, e.k)
	}

	var missData []int
	for i := 0; i < e.k; i++ {
		if len(shards[i]) == 0 {
			missData = append(missData, i)
		}
	}
	if len(missData) > 0 {
		dec, err := e.decodeMatrix(present)
		if err != nil {
			return err
		}
		in := make([][]byte, e.k)
		for i, p := range present {
			in[i] = shards[p]
		}
		rows := make(matrix, len(missData))
		out := make([][]byte, len(missData))
		for i, d := range missData {
			rows[i] = dec[d]
			shards[d] = make([]byte, size)
			out[i] = shards[d]
		}
		e.mulRows(rows, in, out)
	}
	if dataOnly {
		return nil
	}

	var missPar []int
	for i := e.k; i < e.k+e.m; i++ {
		if len(shards[i]) == 0 {
			missPar = append(missPar, i)
		}
	}
	if len(missPar) > 0 {
		rows := make(matrix, len(missPar))
		out := make([][]byte, len(missPar))
		for i, p := range missPar {
			rows[i] = e.enc[p]
			shards[p] = make([]byte, size)
			out[i] = shards[p]
		}
		e.mulRows(rows, shards[:e.k], out)
	}
	return nil
}

func (e *Encoder) decodeMatrix(present []int) (matrix, error) {
	key := string(intsToBytes(present))
	e.mu.Lock()
	if m, ok := e.inv[key]; ok {
		e.mu.Unlock()
		return m, nil
	}
	e.mu.Unlock()

	inv, err := e.enc.rows(present).invert()
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	if len(e.inv) >= invCacheMax {
		clear(e.inv) // crude, but C(n,k) can be huge and this is only a perf hint
	}
	e.inv[key] = inv
	e.mu.Unlock()
	return inv, nil
}

func (e *Encoder) checkSizes(shards [][]byte, allowEmpty bool) error {
	if len(shards) != e.k+e.m {
		return fmt.Errorf("erasure: got %d shards, want %d", len(shards), e.k+e.m)
	}
	size := -1
	for _, s := range shards {
		if len(s) == 0 && allowEmpty {
			continue
		}
		if size == -1 {
			size = len(s)
		} else if len(s) != size {
			return ErrShardSize
		}
	}
	if size <= 0 {
		if allowEmpty {
			return ErrTooFewShards
		}
		return ErrShardSize
	}
	return nil
}

// out[r] = sum_c rows[r][c] * in[c], split by byte range across cores for big shards.
func (e *Encoder) mulRows(rows matrix, in, out [][]byte) {
	size := len(in[0])
	workers := runtime.GOMAXPROCS(0)
	if size < parThreshold || workers == 1 {
		mulRange(rows, in, out, 0, size)
		return
	}
	chunk := (size + workers - 1) / workers
	if chunk < minChunk {
		chunk = minChunk
	}
	var wg sync.WaitGroup
	for lo := 0; lo < size; lo += chunk {
		hi := min(lo+chunk, size)
		wg.Add(1)
		go func() {
			defer wg.Done()
			mulRange(rows, in, out, lo, hi)
		}()
	}
	wg.Wait()
}

func mulRange(rows matrix, in, out [][]byte, lo, hi int) {
	// blocks keep the output slice hot in L1 while we sweep over all inputs
	const blk = 16 << 10
	for off := lo; off < hi; off += blk {
		end := min(off+blk, hi)
		for r, row := range rows {
			dst := out[r][off:end]
			mulSet(dst, in[0][off:end], row[0])
			for c := 1; c < len(in); c++ {
				mulAdd(dst, in[c][off:end], row[c])
			}
		}
	}
}

func intsToBytes(v []int) []byte {
	b := make([]byte, len(v)*2)
	for i, x := range v {
		b[2*i] = byte(x >> 8)
		b[2*i+1] = byte(x)
	}
	return b
}

// ShardLen is the per-shard size for n bytes of payload split across k data shards.
func ShardLen(n, k int) int { return (n + k - 1) / k }
