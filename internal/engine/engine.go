// Package engine is the data path: it turns object PUT/GET/DELETE into shard streams on storage nodes
// plus metadata commits, with no HTTP in sight.
package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/monaaoua9/objstore/internal/cluster"
	"github.com/monaaoua9/objstore/internal/ec"
	"github.com/monaaoua9/objstore/internal/frame"
	"github.com/monaaoua9/objstore/internal/meta"
	"github.com/monaaoua9/objstore/internal/metrics"
	"github.com/monaaoua9/objstore/internal/raft"
	"github.com/monaaoua9/objstore/internal/shardrpc"
)

var (
	ErrTooLarge = errors.New("object exceeds the configured size limit")
	ErrBadRange = errors.New("unsatisfiable range")
)

type Config struct {
	K, M          int
	ShardSize     int   // bytes per shard per stripe
	WriteQuorum   int   // shards that must be durable before we commit; 0 = K+1 (capped at K+M)
	MaxObjectSize int64 // 0 = unlimited
}

func (c *Config) setDefaults() {
	if c.ShardSize == 0 {
		c.ShardSize = 256 << 10
	}
	if c.WriteQuorum == 0 {
		c.WriteQuorum = min(c.K+1, c.K+c.M)
	}
}

type Metrics struct {
	Puts, Gets, Deletes        *metrics.Counter
	PutBytes, GetBytes         *metrics.Counter
	DegradedPuts, DegradedGets *metrics.Counter
	PutErrors, GetErrors       *metrics.Counter
	ReadRepairHints            *metrics.Counter
	PutLatency, GetFirstByte   *metrics.Histogram
}

func NewMetrics(r *metrics.Registry) *Metrics {
	return &Metrics{
		Puts:            r.Counter("objstore_put_total", "Completed object uploads."),
		Gets:            r.Counter("objstore_get_total", "Completed object downloads."),
		Deletes:         r.Counter("objstore_delete_total", "Deleted objects."),
		PutBytes:        r.Counter("objstore_put_bytes_total", "Payload bytes accepted."),
		GetBytes:        r.Counter("objstore_get_bytes_total", "Payload bytes served."),
		DegradedPuts:    r.Counter("objstore_put_degraded_total", "Uploads committed with fewer than k+m shards written."),
		DegradedGets:    r.Counter("objstore_get_degraded_total", "Downloads that had to route around a bad shard."),
		PutErrors:       r.Counter("objstore_put_errors_total", "Failed uploads."),
		GetErrors:       r.Counter("objstore_get_errors_total", "Failed downloads."),
		ReadRepairHints: r.Counter("objstore_read_repair_hints_total", "Bad shards reported to the repair queue from the read path."),
		PutLatency:      r.Histogram("objstore_put_seconds", "Upload wall time.", nil),
		GetFirstByte:    r.Histogram("objstore_get_first_byte_seconds", "Time until the first stripe is ready.", nil),
	}
}

type Engine struct {
	Meta *meta.Client
	View *cluster.View
	RPC  *shardrpc.Client
	Cfg  Config
	M    *Metrics
	Log  *slog.Logger

	bg sync.WaitGroup // fire-and-forget cleanups; Wait() on shutdown
}

func New(mc *meta.Client, v *cluster.View, rpc *shardrpc.Client, cfg Config, m *Metrics, lg *slog.Logger) (*Engine, error) {
	cfg.setDefaults()
	if _, err := ec.EncoderFor(cfg.K, cfg.M); err != nil {
		return nil, err
	}
	if cfg.WriteQuorum < cfg.K || cfg.WriteQuorum > cfg.K+cfg.M {
		return nil, fmt.Errorf("write quorum %d outside [%d,%d]", cfg.WriteQuorum, cfg.K, cfg.K+cfg.M)
	}
	if lg == nil {
		lg = slog.Default()
	}
	return &Engine{Meta: mc, View: v, RPC: rpc, Cfg: cfg, M: m, Log: lg}, nil
}

func (e *Engine) Wait() { e.bg.Wait() }

// NewID is a random 128-bit hex id, used for object versions and upload sessions.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

type PutOptions struct {
	ContentType string
	UserMeta    map[string]string
}

// ---- write path ----

type deadSink struct{ err error }

func (d deadSink) Write([]byte) (int, error) { return 0, d.err }

type limitReader struct {
	r io.Reader
	n int64
}

func (l *limitReader) Read(p []byte) (int, error) {
	if l.n < 0 {
		return 0, ErrTooLarge
	}
	n, err := l.r.Read(p)
	l.n -= int64(n)
	if l.n < 0 {
		return n, ErrTooLarge
	}
	return n, err
}

func (e *Engine) Put(ctx context.Context, bucket, key string, body io.Reader, opt PutOptions) (*meta.Object, error) {
	start := time.Now()
	obj, err := e.put(ctx, bucket, key, body, opt, bucket != meta.MPUBucket)
	if err != nil {
		e.M.PutErrors.Inc()
		return nil, err
	}
	e.M.Puts.Inc()
	e.M.PutBytes.Add(uint64(obj.Size))
	e.M.PutLatency.Since(start)
	return obj, nil
}

func (e *Engine) put(ctx context.Context, bucket, key string, body io.Reader, opt PutOptions, checkBucket bool) (*meta.Object, error) {
	k, m := e.Cfg.K, e.Cfg.M
	n := k + m

	// fail early on a missing bucket rather than after streaming gigabytes
	if checkBucket {
		if bs, err := e.Meta.ListBuckets(ctx); err != nil {
			return nil, err
		} else if !hasBucket(bs, bucket) {
			return nil, meta.ErrBucketNotFound
		}
	}

	enc, _ := ec.EncoderFor(k, m)
	version := NewID()
	nodes, err := e.View.Place(version, n)
	if err != nil {
		return nil, err
	}

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	streams := make([]*shardrpc.PutStream, n)
	sinks := make([]io.Writer, n)
	for i, id := range nodes {
		addr, ok := e.View.Addr(id)
		if !ok {
			sinks[i] = deadSink{fmt.Errorf("node %s not registered", id)}
			continue
		}
		streams[i] = e.RPC.OpenPut(sctx, addr, version, i)
		sinks[i] = streams[i]
	}

	if e.Cfg.MaxObjectSize > 0 {
		body = &limitReader{r: body, n: e.Cfg.MaxObjectSize}
	}
	res, err := ec.Stream(sctx, enc, body, e.Cfg.ShardSize, sinks, e.Cfg.WriteQuorum)
	if err != nil {
		e.abortAll(streams, err)
		return nil, err
	}

	// commit each shard file on its node (fsync + rename happens when the request body ends)
	ok := make([]bool, n)
	var wg sync.WaitGroup
	for i, s := range streams {
		if s == nil || res.Failed[i] {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Close(); err != nil {
				e.Log.Warn("shard commit failed", "node", nodes[i], "shard", i, "err", err)
				e.noteNodeError(nodes[i], err)
				return
			}
			ok[i] = true
		}()
	}
	wg.Wait()
	for i, s := range streams { // sinks that already failed mid-stream still need their goroutines reaped
		if s != nil && res.Failed[i] {
			s.Abort(nil)
		}
	}

	good := 0
	var degraded []int
	for i := range ok {
		if ok[i] {
			good++
		} else {
			degraded = append(degraded, i)
		}
	}
	if good < e.Cfg.WriteQuorum {
		e.cleanup(version, nodes, ok)
		return nil, fmt.Errorf("%w: %d of %d shards durable, need %d", ec.ErrQuorum, good, n, e.Cfg.WriteQuorum)
	}

	obj := &meta.Object{
		Bucket: bucket, Key: key, Version: version,
		Size: res.Size, ETag: res.ETag, ContentType: opt.ContentType,
		Created: time.Now().UTC().Truncate(time.Millisecond),
		K:       k, M: m, ShardSize: e.Cfg.ShardSize, Stripes: res.Stripes,
		UserMeta: opt.UserMeta,
	}
	for i, id := range nodes {
		obj.Shards = append(obj.Shards, meta.ShardLoc{Index: i, Node: id})
	}
	if err := e.Meta.CommitObject(ctx, obj, degraded); err != nil {
		// outcome may be unknown (lost leader); GC is safe because queued deletes only target this version
		if !errors.Is(err, raft.ErrLostLeader) {
			e.cleanup(version, nodes, ok)
		}
		return nil, err
	}
	if len(degraded) > 0 {
		e.M.DegradedPuts.Inc()
		e.Log.Warn("object committed degraded", "bucket", bucket, "key", key, "missing_shards", degraded)
	}
	return obj, nil
}

func (e *Engine) abortAll(streams []*shardrpc.PutStream, cause error) {
	var wg sync.WaitGroup
	for _, s := range streams {
		if s == nil {
			continue
		}
		wg.Add(1)
		go func() { defer wg.Done(); s.Abort(cause) }()
	}
	wg.Wait()
}

// cleanup removes shard files of a version that never made it into metadata.
func (e *Engine) cleanup(version string, nodes []string, written []bool) {
	e.bg.Add(1)
	go func() {
		defer e.bg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for i, id := range nodes {
			if !written[i] {
				continue
			}
			if addr, ok := e.View.Addr(id); ok {
				e.RPC.Delete(ctx, addr, version, i)
			}
		}
	}()
}

func (e *Engine) noteNodeError(id string, err error) {
	if errors.Is(err, shardrpc.ErrNotFound) || errors.Is(err, frame.ErrCorrupt) {
		return // node answered, it just doesn't like the data
	}
	e.View.Suspect(id)
}

func hasBucket(bs []meta.Bucket, name string) bool {
	for _, b := range bs {
		if b.Name == name {
			return true
		}
	}
	return false
}

// PutPart stores one multipart part as an ordinary erasure-coded object in the hidden staging bucket.
// Re-uploading a part number replaces (and garbage-collects) the earlier attempt.
func (e *Engine) PutPart(ctx context.Context, uploadID string, n int, body io.Reader) (*meta.Object, error) {
	return e.Put(ctx, meta.MPUBucket, meta.PartKey(uploadID, n), body, PutOptions{})
}

// ---- read path ----

type opener struct {
	e   *Engine
	obj *meta.Object
}

func (o opener) OpenShard(ctx context.Context, idx int, offset int64) (io.ReadCloser, error) {
	id := o.obj.Shards[idx].Node
	if !o.e.View.Usable(id) {
		return nil, fmt.Errorf("node %s unavailable", id)
	}
	addr, _ := o.e.View.Addr(id)
	rc, err := o.e.RPC.Get(ctx, addr, o.obj.Version, idx, offset)
	if err != nil {
		o.e.noteNodeError(id, err)
		return nil, err
	}
	return rc, nil
}

// OpenShardOn reads from an explicit node instead of the recorded placement; used by repair.
func (e *Engine) ShardOpener(obj *meta.Object) ec.Opener { return opener{e, obj} }

type ObjectReader struct {
	e        *Engine
	obj      *meta.Object
	dec      *ec.Decoder
	lay      ec.Layout
	skip     int64
	remain   int64
	pending  [][]byte
	pendingN int
}

// NewReader positions a reader at [start, end] (inclusive) and fetches the first stripe, so a failure
// surfaces before the caller has committed to a status line.
func (e *Engine) NewReader(ctx context.Context, obj *meta.Object, start, end int64) (Reader, error) {
	if obj.Size == 0 {
		return &ObjectReader{e: e, obj: obj}, nil
	}
	if start < 0 || end < start || end >= obj.Size {
		return nil, ErrBadRange
	}
	if len(obj.Parts) > 0 {
		return e.newMultiReader(ctx, obj, start, end)
	}
	return e.newReader(ctx, obj, start, end)
}

// Reader streams an object (or a byte range of it) into a writer.
type Reader interface {
	WriteTo(w io.Writer) (int64, error)
	Close()
}

func (e *Engine) newReader(ctx context.Context, obj *meta.Object, start, end int64) (*ObjectReader, error) {
	t0 := time.Now()
	if start < 0 || end < start || end >= obj.Size {
		return nil, ErrBadRange
	}
	lay := ec.LayoutOf(obj)
	first := int(start / lay.StripeBytes())
	dec, err := ec.NewDecoder(ctx, lay, opener{e, obj}, first, nil)
	if err != nil {
		return nil, err
	}
	r := &ObjectReader{e: e, obj: obj, dec: dec, lay: lay, skip: start - int64(first)*lay.StripeBytes(), remain: end - start + 1}
	r.pending, r.pendingN, err = dec.Next()
	if err != nil {
		r.Close()
		e.M.GetErrors.Inc()
		return nil, err
	}
	e.M.GetFirstByte.Since(t0)
	return r, nil
}

func (r *ObjectReader) Size() int64 { return r.remain }

func (r *ObjectReader) WriteTo(w io.Writer) (int64, error) {
	if r.dec == nil {
		return 0, nil
	}
	var written int64
	for r.remain > 0 {
		if r.pending == nil {
			var err error
			r.pending, r.pendingN, err = r.dec.Next()
			if err != nil {
				r.e.M.GetErrors.Inc()
				if err == io.EOF {
					err = io.ErrUnexpectedEOF
				}
				return written, err
			}
		}
		left := r.pendingN
		for _, sh := range r.pending {
			if left <= 0 || r.remain <= 0 {
				break
			}
			chunk := sh
			if len(chunk) > left {
				chunk = chunk[:left]
			}
			left -= len(chunk)
			if r.skip > 0 {
				if r.skip >= int64(len(chunk)) {
					r.skip -= int64(len(chunk))
					continue
				}
				chunk = chunk[r.skip:]
				r.skip = 0
			}
			if int64(len(chunk)) > r.remain {
				chunk = chunk[:r.remain]
			}
			n, err := w.Write(chunk)
			written += int64(n)
			r.remain -= int64(n)
			if err != nil {
				return written, err
			}
		}
		r.pending = nil
	}
	r.e.M.Gets.Inc()
	r.e.M.GetBytes.Add(uint64(written))
	return written, nil
}

// Close releases lanes and files bad shards the read path stumbled on into the repair queue.
func (r *ObjectReader) Close() {
	if r.dec == nil {
		return
	}
	bad := r.dec.Bad()
	r.dec.Close()
	if len(bad) == 0 {
		return
	}
	r.e.M.DegradedGets.Inc()
	for _, b := range bad {
		reason := classify(b.Err)
		if reason == "" {
			continue // network blip or cancellation: the prober decides about dead nodes
		}
		r.e.M.ReadRepairHints.Inc()
		v, idx := r.obj.Version, b.Index
		r.e.bg.Add(1)
		go func() {
			defer r.e.bg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := r.e.Meta.MarkDamaged(ctx, v, idx, reason); err != nil {
				r.e.Log.Debug("could not queue read-repair", "err", err)
			}
		}()
	}
}

func classify(err error) string {
	switch {
	case errors.Is(err, frame.ErrCorrupt):
		return "read-bitrot"
	case errors.Is(err, shardrpc.ErrNotFound):
		return "read-missing"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "read-truncated"
	}
	return ""
}

// ---- delete ----

// Delete removes key; in a versioned bucket it returns the delete marker that now hides it.
func (e *Engine) Delete(ctx context.Context, bucket, key string) (*meta.Object, error) {
	m, err := e.Meta.DeleteObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	e.M.Deletes.Inc()
	return m, nil
}

// DeleteVersion permanently removes one version or delete marker.
func (e *Engine) DeleteVersion(ctx context.Context, bucket, key, version string) error {
	if err := e.Meta.DeleteVersion(ctx, bucket, key, version); err != nil {
		return err
	}
	e.M.Deletes.Inc()
	return nil
}

// multiReader walks the parts of a completed multipart object in order. Each part is read through
// the normal single-object path, so degraded reads and read-repair work per part unchanged.
type multiReader struct {
	e    *Engine
	ctx  context.Context
	segs []segment
	cur  Reader
	curI int
}

type segment struct {
	ref    meta.PartRef
	lo, hi int64 // inclusive byte range inside the part
}

func (e *Engine) newMultiReader(ctx context.Context, obj *meta.Object, start, end int64) (*multiReader, error) {
	m := &multiReader{e: e, ctx: ctx}
	var off int64
	for _, p := range obj.Parts {
		pStart, pEnd := off, off+p.Size-1
		off += p.Size
		if p.Size == 0 || pEnd < start || pStart > end {
			continue
		}
		m.segs = append(m.segs, segment{p, max(start, pStart) - pStart, min(end, pEnd) - pStart})
	}
	// open the first segment now so failures surface before the response headers go out
	if len(m.segs) > 0 {
		r, err := m.open(0)
		if err != nil {
			return nil, err
		}
		m.cur, m.curI = r, 0
	}
	return m, nil
}

func (m *multiReader) open(i int) (Reader, error) {
	s := m.segs[i]
	po, err := m.e.Meta.GetVersion(m.ctx, s.ref.Version)
	if err != nil {
		return nil, fmt.Errorf("part %d: %w", s.ref.Number, err)
	}
	return m.e.newReader(m.ctx, po, s.lo, s.hi)
}

func (m *multiReader) WriteTo(w io.Writer) (int64, error) {
	var total int64
	for i := range m.segs {
		if m.cur == nil || m.curI != i {
			r, err := m.open(i)
			if err != nil {
				return total, err
			}
			m.cur, m.curI = r, i
		}
		n, err := m.cur.WriteTo(w)
		total += n
		m.cur.Close()
		m.cur = nil
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (m *multiReader) Close() {
	if m.cur != nil {
		m.cur.Close()
		m.cur = nil
	}
}
