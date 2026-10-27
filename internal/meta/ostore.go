package meta

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// oref points at one encoded Object in the store. size rides along so Stats never has to read.
type oref struct {
	off  int64
	size int64
	n    uint32
	mp   bool // manifest of a multipart upload: has parts to chase
}

type backing interface {
	io.ReaderAt
	io.WriterAt
}

type membuf struct {
	mu sync.RWMutex
	b  []byte
}

func (m *membuf) ReadAt(p []byte, off int64) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if off >= int64(len(m.b)) {
		return 0, io.EOF
	}
	n := copy(p, m.b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *membuf) WriteAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if need := off + int64(len(p)); need > int64(len(m.b)) {
		m.b = append(m.b, make([]byte, need-int64(len(m.b)))...)
	}
	return copy(m.b[off:], p), nil
}

// ostore keeps encoded objects out of the Go heap. Only the FSM's index holds orefs; the records
// themselves live in a file (or a byte slice in tests). It is a cache of raft state, never the
// source of truth: it is truncated on start and rebuilt from snapshot + log replay.
type ostore struct {
	bk   backing
	path string // empty: in memory
	end  int64

	written int64 // bytes appended since the last compaction
	base    int64 // size right after it

	closed atomic.Bool

	mu    sync.Mutex // guards cache only; reads run under the FSM's read lock, concurrently
	cache map[int64]*Object
}

const cacheMax = 4096

// compactMin is a var so tests can trigger compaction without writing 64 MiB
var compactMin int64 = 64 << 20

func newStore(path string) (*ostore, error) {
	s := &ostore{path: path, cache: map[int64]*Object{}}
	if path == "" {
		s.bk = &membuf{}
		return s, nil
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	s.bk = f
	return s, nil
}

// the disk is gone or full; the FSM can't skip a command, so there is nothing sensible to return
func die(err error) { panic(fmt.Errorf("meta store: %w", err)) }

func (s *ostore) put(o *Object) oref {
	b, err := json.Marshal(o)
	if err != nil {
		die(err)
	}
	r := oref{off: s.end, size: o.Size, n: uint32(len(b)), mp: len(o.Parts) > 0}
	if _, err := s.bk.WriteAt(b, s.end); err != nil {
		die(err)
	}
	s.end += int64(len(b))
	s.written += int64(len(b))
	s.remember(r.off, o)
	return r
}

func (s *ostore) raw(r oref) []byte {
	buf := make([]byte, r.n)
	if _, err := s.bk.ReadAt(buf, r.off); err != nil {
		if s.closed.Load() { // a request that was in flight when the node shut down
			return nil
		}
		die(err)
	}
	return buf
}

func (s *ostore) get(r oref) *Object {
	s.mu.Lock()
	o := s.cache[r.off]
	s.mu.Unlock()
	if o != nil {
		return o
	}
	buf := s.raw(r)
	if buf == nil {
		return new(Object)
	}
	o = new(Object)
	if err := json.Unmarshal(buf, o); err != nil {
		die(err)
	}
	s.remember(r.off, o)
	return o
}

func (s *ostore) remember(off int64, o *Object) {
	s.mu.Lock()
	if len(s.cache) >= cacheMax {
		for k := range s.cache { // map order is random enough
			delete(s.cache, k)
			break
		}
	}
	s.cache[off] = o
	s.mu.Unlock()
}

func (s *ostore) clear() {
	s.end, s.written, s.base = 0, 0, 0
	s.mu.Lock()
	s.cache = map[int64]*Object{}
	s.mu.Unlock()
	if f, ok := s.bk.(*os.File); ok {
		if err := f.Truncate(0); err != nil {
			die(err)
		}
	} else {
		s.bk = &membuf{}
	}
}

func (s *ostore) close() {
	s.closed.Store(true)
	if c, ok := s.bk.(io.Closer); ok {
		c.Close()
	}
}

func (s *ostore) dropCache() {
	s.mu.Lock()
	s.cache = map[int64]*Object{}
	s.mu.Unlock()
}

func (s *ostore) newPath() string {
	if s.path == "" {
		return ""
	}
	return s.path + ".new"
}

func (s *ostore) wantCompact() bool {
	return s.written > compactMin && s.written > 2*s.base
}

// copyRaw moves a record without decoding it
func (s *ostore) copyRaw(from *ostore, r oref) oref {
	buf := make([]byte, r.n)
	if _, err := from.bk.ReadAt(buf, r.off); err != nil {
		die(err)
	}
	n := oref{off: s.end, size: r.size, n: r.n, mp: r.mp}
	if _, err := s.bk.WriteAt(buf, s.end); err != nil {
		die(err)
	}
	s.end += int64(r.n)
	return n
}

// settle makes ns take over from s after a compaction
func (s *ostore) settle(ns *ostore) {
	if s.path != "" {
		s.bk.(*os.File).Close()
		ns.bk.(*os.File).Close()
		if err := os.Rename(ns.path, s.path); err != nil {
			die(err)
		}
		f, err := os.OpenFile(s.path, os.O_RDWR, 0o644)
		if err != nil {
			die(err)
		}
		ns.bk = f
	}
	s.bk, s.end = ns.bk, ns.end
	s.written, s.base = 0, ns.end
	s.mu.Lock()
	s.cache = map[int64]*Object{}
	s.mu.Unlock()
}
