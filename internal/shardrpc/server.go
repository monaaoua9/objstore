// Package shardrpc is the node-to-node shard protocol: streaming PUT/GET of framed shards over HTTP/2.
package shardrpc

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/monaaoua9/objstore/internal/blobstore"
	"github.com/monaaoua9/objstore/internal/frame"
	"github.com/monaaoua9/objstore/internal/metrics"
)

type Server struct {
	NodeID string
	Store  *blobstore.Store

	// optional instrumentation
	BytesIn  *metrics.Counter
	BytesOut *metrics.Counter
	Rejected *metrics.Counter
}

type PutResult struct {
	Frames int   `json:"frames"`
	Bytes  int64 `json:"bytes"`
}

type VerifyResult struct {
	OK      bool   `json:"ok"`
	Frames  int    `json:"frames"`
	Corrupt bool   `json:"corrupt,omitempty"`
	Error   string `json:"error,omitempty"`
}

type Info struct {
	ID     string `json:"id"`
	Shards int64  `json:"shards"`
	Bytes  int64  `json:"bytes"`
}

func (s *Server) Mount(mux *http.ServeMux, wrap func(http.Handler) http.Handler) {
	if wrap == nil {
		wrap = func(h http.Handler) http.Handler { return h }
	}
	h := func(pat string, f http.HandlerFunc) { mux.Handle(pat, wrap(f)) }
	h("PUT /internal/shard/{version}/{idx}", s.put)
	h("GET /internal/shard/{version}/{idx}", s.get)
	h("HEAD /internal/shard/{version}/{idx}", s.head)
	h("DELETE /internal/shard/{version}/{idx}", s.del)
	h("POST /internal/shard/{version}/{idx}/verify", s.verify)
	h("GET /internal/node", s.info)
}

func ident(r *http.Request) (string, int, bool) {
	idx, err := strconv.Atoi(r.PathValue("idx"))
	return r.PathValue("version"), idx, err == nil
}

func (s *Server) put(w http.ResponseWriter, r *http.Request) {
	v, idx, ok := ident(r)
	if !ok {
		http.Error(w, "bad shard index", http.StatusBadRequest)
		return
	}
	body := io.Reader(r.Body)
	if s.BytesIn != nil {
		body = &countingReader{r: r.Body, c: s.BytesIn}
	}
	frames, n, err := s.Store.Put(v, idx, body)
	switch {
	case errors.Is(err, blobstore.ErrBadName):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, frame.ErrCorrupt):
		if s.Rejected != nil {
			s.Rejected.Inc()
		}
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(PutResult{frames, n})
	}
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	v, idx, ok := ident(r)
	if !ok {
		http.Error(w, "bad shard index", http.StatusBadRequest)
		return
	}
	var off int64
	if q := r.URL.Query().Get("offset"); q != "" {
		var err error
		if off, err = strconv.ParseInt(q, 10, 64); err != nil || off < 0 {
			http.Error(w, "bad offset", http.StatusBadRequest)
			return
		}
	}
	f, err := s.Store.Open(v, idx, off)
	if errors.Is(err, blobstore.ErrNotFound) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(f.Size-off, 10))
	w.Header().Set("X-Shard-Size", strconv.FormatInt(f.Size, 10))
	out := io.Writer(w)
	if s.BytesOut != nil {
		out = &countingWriter{w: w, c: s.BytesOut}
	}
	io.CopyBuffer(out, f.File, make([]byte, 256<<10))
}

func (s *Server) head(w http.ResponseWriter, r *http.Request) {
	v, idx, ok := ident(r)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	n, err := s.Store.Stat(v, idx)
	if err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, blobstore.ErrNotFound) {
			code = http.StatusNotFound
		}
		w.WriteHeader(code)
		return
	}
	w.Header().Set("Content-Length", strconv.FormatInt(n, 10))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) del(w http.ResponseWriter, r *http.Request) {
	v, idx, ok := ident(r)
	if !ok {
		http.Error(w, "bad shard index", http.StatusBadRequest)
		return
	}
	if err := s.Store.Delete(v, idx); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) verify(w http.ResponseWriter, r *http.Request) {
	v, idx, ok := ident(r)
	if !ok {
		http.Error(w, "bad shard index", http.StatusBadRequest)
		return
	}
	frames, err := s.Store.Verify(v, idx, nil)
	if errors.Is(err, blobstore.ErrNotFound) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	res := VerifyResult{OK: err == nil, Frames: frames}
	if err != nil {
		res.Error = err.Error()
		res.Corrupt = errors.Is(err, frame.ErrCorrupt) || errors.Is(err, io.ErrUnexpectedEOF)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

func (s *Server) info(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Info{s.NodeID, s.Store.Count(), s.Store.Bytes()})
}

type countingReader struct {
	r io.Reader
	c *metrics.Counter
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.c.Add(uint64(n))
	return n, err
}

type countingWriter struct {
	w io.Writer
	c *metrics.Counter
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.c.Add(uint64(n))
	return n, err
}
