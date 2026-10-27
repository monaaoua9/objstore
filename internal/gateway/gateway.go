// Package gateway is the client-facing HTTP API (HTTP/1.1 and h2c/h2). Bodies are streamed
// straight into and out of the engine; nothing here buffers an object.
package gateway

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/monaaoua9/objstore/internal/cluster"
	"github.com/monaaoua9/objstore/internal/ec"
	"github.com/monaaoua9/objstore/internal/engine"
	"github.com/monaaoua9/objstore/internal/meta"
	"github.com/monaaoua9/objstore/internal/metrics"
	"github.com/monaaoua9/objstore/internal/raft"
)

var bucketRe = regexp.MustCompile(`^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$`)

const metaHeaderPrefix = "X-Meta-"

type Gateway struct {
	Eng   *engine.Engine
	Meta  *meta.Client
	View  *cluster.View
	Log   *slog.Logger
	Token string // bearer token for the public API; empty disables auth

	// RaftStatus and Raft are nil on storage-only nodes.
	RaftStatus func() raft.Status
	Raft       *raft.Node

	reqs    *metrics.CounterVec
	latency *metrics.Histogram
}

func New(eng *engine.Engine, mc *meta.Client, v *cluster.View, token string, reg *metrics.Registry, lg *slog.Logger) *Gateway {
	return &Gateway{
		Eng: eng, Meta: mc, View: v, Token: token, Log: lg.With("component", "gateway"),
		reqs:    reg.CounterVec("objstore_http_requests_total", "API requests by method and status class.", "code"),
		latency: reg.Histogram("objstore_http_request_seconds", "API request wall time.", nil),
	}
}

func (g *Gateway) Mount(mux *http.ServeMux) {
	h := func(pat string, f http.HandlerFunc) { mux.Handle(pat, g.wrap(f)) }
	h("GET /api/v1/buckets", g.listBuckets)
	h("PUT /api/v1/buckets/{bucket}", g.createBucket)
	h("DELETE /api/v1/buckets/{bucket}", g.deleteBucket)
	h("GET /api/v1/buckets/{bucket}/objects", g.listObjects)
	h("GET /api/v1/buckets/{bucket}/versions", g.listVersions)
	h("GET /api/v1/buckets/{bucket}/versioning", g.getVersioning)
	h("GET /api/v1/buckets/{bucket}/lifecycle", g.getLifecycle)
	h("PUT /api/v1/buckets/{bucket}/lifecycle", g.putLifecycle)
	h("DELETE /api/v1/buckets/{bucket}/lifecycle", g.clearLifecycle)
	h("PUT /api/v1/buckets/{bucket}/versioning", g.putVersioning)
	h("PUT /api/v1/buckets/{bucket}/objects/{key...}", g.putObject)
	h("GET /api/v1/buckets/{bucket}/objects/{key...}", g.getObject)
	h("HEAD /api/v1/buckets/{bucket}/objects/{key...}", g.getObject)
	h("DELETE /api/v1/buckets/{bucket}/objects/{key...}", g.deleteObject)

	h("POST /api/v1/buckets/{bucket}/uploads", g.createUpload)
	h("GET /api/v1/buckets/{bucket}/uploads/{id}", g.listParts)
	h("PUT /api/v1/buckets/{bucket}/uploads/{id}/parts/{n}", g.putPart)
	h("POST /api/v1/buckets/{bucket}/uploads/{id}/complete", g.completeUpload)
	h("DELETE /api/v1/buckets/{bucket}/uploads/{id}", g.abortUpload)

	h("GET /api/v1/cluster", g.clusterInfo)
	h("GET /api/v1/cluster/repair", g.repairQueue)
	h("POST /api/v1/cluster/nodes/{id}/drain", g.setNodeState(meta.NodeDraining))
	h("POST /api/v1/cluster/nodes/{id}/activate", g.setNodeState(meta.NodeUp))
	h("DELETE /api/v1/cluster/nodes/{id}", g.removeNode)
	h("GET /api/v1/cluster/voters", g.listVoters)
	h("PUT /api/v1/cluster/voters/{id}", g.addVoter)
	h("DELETE /api/v1/cluster/voters/{id}", g.removeVoter)
}

// ---- plumbing ----

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(c int) {
	if s.code == 0 {
		s.code = c
	}
	s.ResponseWriter.WriteHeader(c)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if s.code == 0 {
		s.code = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

func (s *statusWriter) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (g *Gateway) wrap(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		if !g.authorized(r) {
			writeErr(sw, http.StatusUnauthorized, "UNAUTHORIZED", "missing or invalid bearer token")
		} else if strings.HasPrefix(r.PathValue("bucket"), "~") {
			// internal staging namespace, never addressable from outside
			writeErr(sw, http.StatusNotFound, "BUCKET_NOT_FOUND", "bucket does not exist")
		} else {
			next(sw, r)
		}
		if sw.code == 0 {
			sw.code = http.StatusOK
		}
		g.reqs.Inc(strconv.Itoa(sw.code/100) + "xx")
		g.latency.Since(start)
		if sw.code >= 500 {
			g.Log.Warn("request failed", "method", r.Method, "path", r.URL.Path, "status", sw.code, "dur", time.Since(start))
		}
	})
}

func (g *Gateway) authorized(r *http.Request) bool {
	if g.Token == "" {
		return true
	}
	got := strings.TrimSpace(r.Header.Get("Authorization"))
	return subtle.ConstantTimeCompare([]byte(got), []byte("Bearer "+g.Token)) == 1
}

func writeJSON(w http.ResponseWriter, code int, data any, m any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{"data": data, "meta": m})
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

func (g *Gateway) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, meta.ErrBucketNotFound):
		writeErr(w, 404, "BUCKET_NOT_FOUND", "bucket does not exist")
	case errors.Is(err, meta.ErrObjectNotFound):
		writeErr(w, 404, "OBJECT_NOT_FOUND", "object does not exist")
	case errors.Is(err, meta.ErrUploadNotFound):
		writeErr(w, 404, "UPLOAD_NOT_FOUND", "multipart upload does not exist (or was completed/aborted)")
	case errors.Is(err, meta.ErrInvalidPart):
		writeErr(w, 400, "INVALID_PART", err.Error())
	case errors.Is(err, meta.ErrBucketExists):
		writeErr(w, 409, "BUCKET_EXISTS", "bucket already exists")
	case errors.Is(err, meta.ErrBucketNotEmpty):
		writeErr(w, 409, "BUCKET_NOT_EMPTY", "bucket still has objects")
	case errors.Is(err, engine.ErrTooLarge):
		writeErr(w, 413, "OBJECT_TOO_LARGE", err.Error())
	case errors.Is(err, engine.ErrBadRange):
		writeErr(w, 416, "INVALID_RANGE", "requested range not satisfiable")
	case errors.Is(err, ec.ErrQuorum), errors.Is(err, cluster.ErrNoCapacity):
		writeErr(w, 503, "INSUFFICIENT_STORAGE_NODES", err.Error())
	case errors.Is(err, ec.ErrUnrecoverable):
		writeErr(w, 503, "DATA_UNAVAILABLE", "too few healthy shards to serve this object right now")
	case errors.Is(err, raft.ErrNoLeader), errors.Is(err, raft.ErrNotLeader), errors.Is(err, raft.ErrLostLeader):
		writeErr(w, 503, "METADATA_UNAVAILABLE", "metadata leader unavailable, retry")
	case errors.Is(err, meta.ErrInvalidLifecycle):
		writeErr(w, 400, "INVALID_LIFECYCLE", err.Error())
	case errors.Is(err, raft.ErrMembership):
		writeErr(w, 400, "INVALID_MEMBERSHIP_CHANGE", err.Error())
	case errors.Is(err, raft.ErrConfigBusy):
		writeErr(w, 409, "MEMBERSHIP_CHANGE_IN_PROGRESS", "another voter change is still being committed, retry shortly")
	case errors.Is(err, context.Canceled):
		writeErr(w, 499, "CLIENT_CLOSED", "request cancelled")
	case errors.Is(err, context.DeadlineExceeded):
		writeErr(w, 504, "TIMEOUT", "operation timed out")
	default:
		g.Log.Error("internal error", "path", r.URL.Path, "err", err)
		writeErr(w, 500, "INTERNAL", "internal error")
	}
}

func validKey(k string) bool {
	return k != "" && len(k) <= 1024 && utf8.ValidString(k) && !strings.ContainsRune(k, 0)
}

// ---- buckets ----

func (g *Gateway) createBucket(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("bucket")
	if !bucketRe.MatchString(name) {
		writeErr(w, 400, "INVALID_BUCKET_NAME", "3-63 chars of a-z, 0-9, '.' and '-'")
		return
	}
	if err := g.Meta.CreateBucket(r.Context(), name); err != nil {
		g.fail(w, r, err)
		return
	}
	writeJSON(w, 201, map[string]string{"name": name}, nil)
}

func (g *Gateway) deleteBucket(w http.ResponseWriter, r *http.Request) {
	if err := g.Meta.DeleteBucket(r.Context(), r.PathValue("bucket")); err != nil {
		g.fail(w, r, err)
		return
	}
	w.WriteHeader(204)
}

func (g *Gateway) listBuckets(w http.ResponseWriter, r *http.Request) {
	bs, err := g.Meta.ListBuckets(r.Context())
	if err != nil {
		g.fail(w, r, err)
		return
	}
	if bs == nil {
		bs = []meta.Bucket{}
	}
	writeJSON(w, 200, bs, map[string]int{"count": len(bs)})
}

// ---- objects ----

func (g *Gateway) listObjects(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	items, next, err := g.Meta.ListObjects(r.Context(), r.PathValue("bucket"), q.Get("prefix"), q.Get("cursor"), limit)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	if items == nil {
		items = []meta.ObjectItem{}
	}
	m := map[string]any{"count": len(items)}
	if next != "" {
		m["next_cursor"] = next
	}
	writeJSON(w, 200, items, m)
}

func (g *Gateway) putObject(w http.ResponseWriter, r *http.Request) {
	bucket, key := r.PathValue("bucket"), r.PathValue("key")
	if !validKey(key) {
		writeErr(w, 400, "INVALID_KEY", "key must be 1-1024 bytes of valid UTF-8")
		return
	}
	if r.ContentLength > 0 && g.Eng.Cfg.MaxObjectSize > 0 && r.ContentLength > g.Eng.Cfg.MaxObjectSize {
		writeErr(w, 413, "OBJECT_TOO_LARGE", "object exceeds the configured size limit")
		return
	}
	opt := engine.PutOptions{ContentType: r.Header.Get("Content-Type")}
	for k, v := range r.Header {
		if strings.HasPrefix(k, metaHeaderPrefix) && len(v) > 0 {
			if opt.UserMeta == nil {
				opt.UserMeta = map[string]string{}
			}
			opt.UserMeta[strings.ToLower(strings.TrimPrefix(k, metaHeaderPrefix))] = v[0]
		}
	}
	obj, err := g.Eng.Put(r.Context(), bucket, key, r.Body, opt)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	w.Header().Set("ETag", `"`+obj.ETag+`"`)
	w.Header().Set("X-Object-Version", obj.Version)
	writeJSON(w, 201, map[string]any{
		"bucket": obj.Bucket, "key": obj.Key, "size": obj.Size, "etag": obj.ETag, "version": obj.Version,
	}, nil)
}

func (g *Gateway) getObject(w http.ResponseWriter, r *http.Request) {
	bucket, key := r.PathValue("bucket"), r.PathValue("key")
	var obj *meta.Object
	var err error
	if v := r.URL.Query().Get("version"); v != "" {
		obj, err = g.Meta.GetObjectVersion(r.Context(), bucket, key, v)
	} else {
		obj, err = g.Meta.GetObject(r.Context(), bucket, key)
	}
	if err != nil {
		if errors.Is(err, meta.ErrDeleteMarker) {
			w.Header().Set("X-Delete-Marker", "true")
			w.Header().Set("X-Object-Version", obj.Version)
		}
		if r.Method == http.MethodHead && errors.Is(err, meta.ErrObjectNotFound) {
			w.WriteHeader(404)
			return
		}
		g.fail(w, r, err)
		return
	}

	h := w.Header()
	etag := `"` + obj.ETag + `"`
	h.Set("ETag", etag)
	h.Set("Accept-Ranges", "bytes")
	h.Set("Last-Modified", obj.Created.UTC().Format(http.TimeFormat))
	h.Set("X-Object-Version", obj.Version)
	ct := obj.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	h.Set("Content-Type", ct)
	for k, v := range obj.UserMeta {
		h.Set(metaHeaderPrefix+k, v)
	}

	if inm := r.Header.Get("If-None-Match"); inm != "" && (inm == "*" || strings.Contains(inm, etag)) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	if im := r.Header.Get("If-Match"); im != "" && im != "*" && !strings.Contains(im, etag) {
		writeErr(w, 412, "PRECONDITION_FAILED", "ETag does not match")
		return
	}

	start, end, partial, ok := ParseRange(r.Header.Get("Range"), obj.Size)
	if !ok {
		h.Set("Content-Range", fmt.Sprintf("bytes */%d", obj.Size))
		writeErr(w, 416, "INVALID_RANGE", "requested range not satisfiable")
		return
	}
	length := end - start + 1
	if obj.Size == 0 {
		length = 0
	}

	if r.Method == http.MethodHead {
		h.Set("Content-Length", strconv.FormatInt(length, 10))
		if partial {
			h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, obj.Size))
			w.WriteHeader(206)
		}
		return
	}

	rd, err := g.Eng.NewReader(r.Context(), obj, start, end)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	defer rd.Close()

	h.Set("Content-Length", strconv.FormatInt(length, 10))
	if partial {
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, obj.Size))
		w.WriteHeader(206)
	}
	if _, err := rd.WriteTo(w); err != nil && !errors.Is(err, context.Canceled) {
		// headers are gone; the best we can do is cut the connection so the client sees a short body
		g.Log.Warn("download aborted mid-stream", "bucket", bucket, "key", key, "err", err)
		panic(http.ErrAbortHandler)
	}
}

var rangeRe = regexp.MustCompile(`^bytes=(\d*)-(\d*)$`)

// ParseRange handles a single "bytes=a-b" range. ok=false means 416.
func ParseRange(h string, size int64) (start, end int64, partial, ok bool) {
	if h == "" || size == 0 {
		return 0, max(size-1, 0), false, true
	}
	m := rangeRe.FindStringSubmatch(strings.TrimSpace(h))
	if m == nil || (m[1] == "" && m[2] == "") {
		return 0, size - 1, false, true // malformed or multi-range: ignore and serve everything, per RFC 9110
	}
	switch {
	case m[1] == "": // suffix: last n bytes
		n, _ := strconv.ParseInt(m[2], 10, 64)
		if n == 0 {
			return 0, 0, false, false
		}
		start, end = max(size-n, 0), size-1
	default:
		start, _ = strconv.ParseInt(m[1], 10, 64)
		end = size - 1
		if m[2] != "" {
			e, _ := strconv.ParseInt(m[2], 10, 64)
			end = min(e, size-1)
		}
		if start >= size || start > end {
			return 0, 0, false, false
		}
	}
	return start, end, true, true
}

func (g *Gateway) deleteObject(w http.ResponseWriter, r *http.Request) {
	bucket, key := r.PathValue("bucket"), r.PathValue("key")
	if v := r.URL.Query().Get("version"); v != "" {
		err := g.Eng.DeleteVersion(r.Context(), bucket, key, v)
		if err != nil && !errors.Is(err, meta.ErrObjectNotFound) {
			g.fail(w, r, err)
			return
		}
		w.WriteHeader(204)
		return
	}
	m, err := g.Eng.Delete(r.Context(), bucket, key)
	if err != nil && !errors.Is(err, meta.ErrObjectNotFound) { // idempotent
		g.fail(w, r, err)
		return
	}
	if m != nil {
		w.Header().Set("X-Delete-Marker", "true")
		w.Header().Set("X-Object-Version", m.Version)
	}
	w.WriteHeader(204)
}

// ---- multipart ----

func (g *Gateway) createUpload(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if !validKey(key) {
		writeErr(w, 400, "INVALID_KEY", "pass ?key= (1-1024 bytes of valid UTF-8)")
		return
	}
	u := meta.Upload{
		ID: engine.NewID(), Bucket: r.PathValue("bucket"), Key: key,
		ContentType: r.Header.Get("Content-Type"), Created: time.Now().UTC().Truncate(time.Millisecond),
	}
	for k, v := range r.Header {
		if strings.HasPrefix(k, metaHeaderPrefix) && len(v) > 0 {
			if u.UserMeta == nil {
				u.UserMeta = map[string]string{}
			}
			u.UserMeta[strings.ToLower(strings.TrimPrefix(k, metaHeaderPrefix))] = v[0]
		}
	}
	if err := g.Meta.CreateUpload(r.Context(), u); err != nil {
		g.fail(w, r, err)
		return
	}
	writeJSON(w, 201, map[string]string{"upload_id": u.ID, "bucket": u.Bucket, "key": u.Key}, nil)
}

// upload looks the session up and makes sure it belongs to the bucket in the URL
func (g *Gateway) upload(w http.ResponseWriter, r *http.Request) (*meta.Upload, bool) {
	u, err := g.Meta.GetUpload(r.Context(), r.PathValue("id"))
	if err == nil && u.Bucket != r.PathValue("bucket") {
		err = meta.ErrUploadNotFound
	}
	if err != nil {
		g.fail(w, r, err)
		return nil, false
	}
	return u, true
}

const maxParts = 10000

func (g *Gateway) putPart(w http.ResponseWriter, r *http.Request) {
	u, ok := g.upload(w, r)
	if !ok {
		return
	}
	n, err := strconv.Atoi(r.PathValue("n"))
	if err != nil || n < 1 || n > maxParts {
		writeErr(w, 400, "INVALID_PART", "part number must be 1-10000")
		return
	}
	obj, err := g.Eng.PutPart(r.Context(), u.ID, n, r.Body)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	w.Header().Set("ETag", `"`+obj.ETag+`"`)
	writeJSON(w, 201, map[string]any{"part": n, "size": obj.Size, "etag": obj.ETag}, nil)
}

func (g *Gateway) listParts(w http.ResponseWriter, r *http.Request) {
	u, ok := g.upload(w, r)
	if !ok {
		return
	}
	items, _, err := g.Meta.ListObjects(r.Context(), meta.MPUBucket, u.ID+"/", "", 10000)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	type part struct {
		Part int    `json:"part"`
		Size int64  `json:"size"`
		ETag string `json:"etag"`
	}
	parts := make([]part, 0, len(items))
	for _, it := range items {
		n, _ := strconv.Atoi(strings.TrimPrefix(it.Key, u.ID+"/"))
		parts = append(parts, part{n, it.Size, it.ETag})
	}
	writeJSON(w, 200, parts, map[string]any{"upload_id": u.ID, "key": u.Key, "count": len(parts)})
}

func (g *Gateway) completeUpload(w http.ResponseWriter, r *http.Request) {
	u, ok := g.upload(w, r)
	if !ok {
		return
	}
	var body struct {
		Parts []meta.PartSel `json:"parts"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&body); err != nil {
		writeErr(w, 400, "INVALID_BODY", `expected {"parts":[{"part":1,"etag":"..."}]}`)
		return
	}
	obj, err := g.Meta.CompleteUpload(r.Context(), u.ID, body.Parts)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	w.Header().Set("ETag", `"`+obj.ETag+`"`)
	writeJSON(w, 201, map[string]any{
		"bucket": obj.Bucket, "key": obj.Key, "size": obj.Size, "etag": obj.ETag, "parts": len(obj.Parts),
	}, nil)
}

func (g *Gateway) abortUpload(w http.ResponseWriter, r *http.Request) {
	u, ok := g.upload(w, r)
	if !ok {
		return
	}
	if err := g.Meta.AbortUpload(r.Context(), u.ID); err != nil && !errors.Is(err, meta.ErrUploadNotFound) {
		g.fail(w, r, err)
		return
	}
	w.WriteHeader(204)
}

// ---- cluster admin ----

func (g *Gateway) clusterInfo(w http.ResponseWriter, r *http.Request) {
	st, err := g.Meta.Stats(r.Context())
	if err != nil {
		g.fail(w, r, err)
		return
	}
	out := map[string]any{
		"nodes":  g.View.Nodes(),
		"stats":  st,
		"coding": map[string]int{"k": g.Eng.Cfg.K, "m": g.Eng.Cfg.M, "shard_size": g.Eng.Cfg.ShardSize, "write_quorum": g.Eng.Cfg.WriteQuorum},
	}
	if g.RaftStatus != nil {
		s := g.RaftStatus()
		out["raft"] = map[string]any{"id": s.ID, "state": s.State.String(), "term": s.Term, "leader": s.Leader, "commit": s.Commit, "applied": s.Applied, "voters": s.Peers}
	}
	writeJSON(w, 200, out, nil)
}

func (g *Gateway) repairQueue(w http.ResponseWriter, r *http.Request) {
	ds, err := g.Meta.Damaged(r.Context(), 100)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	if ds == nil {
		ds = []meta.Damage{}
	}
	writeJSON(w, 200, ds, map[string]int{"count": len(ds)})
}

func (g *Gateway) setNodeState(st meta.NodeState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if _, ok := g.View.Node(id); !ok {
			writeErr(w, 404, "NODE_NOT_FOUND", "unknown node")
			return
		}
		if err := g.Meta.SetNodeState(r.Context(), id, st); err != nil {
			g.fail(w, r, err)
			return
		}
		g.View.Refresh(r.Context())
		writeJSON(w, 200, map[string]string{"id": id, "state": string(st)}, nil)
	}
}

func (g *Gateway) removeNode(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	n, ok := g.View.Node(id)
	if !ok {
		writeErr(w, 404, "NODE_NOT_FOUND", "unknown node")
		return
	}
	if n.State == meta.NodeUp && r.URL.Query().Get("force") != "true" {
		writeErr(w, 409, "NODE_ACTIVE", "drain the node first (or pass force=true; its shards will be rebuilt from parity)")
		return
	}
	if err := g.Meta.RemoveNode(r.Context(), id); err != nil {
		g.fail(w, r, err)
		return
	}
	g.View.Refresh(r.Context())
	w.WriteHeader(204)
}

// ---- metadata voters ----

func (g *Gateway) listVoters(w http.ResponseWriter, r *http.Request) {
	if g.Raft == nil {
		writeErr(w, 501, "NOT_A_VOTER", "ask a metadata voter node")
		return
	}
	type voter struct {
		ID     string `json:"id"`
		Addr   string `json:"addr,omitempty"`
		Leader bool   `json:"leader"`
	}
	leader := g.Raft.Status().Leader
	vs := []voter{}
	for id, addr := range g.Raft.Members() {
		vs = append(vs, voter{id, addr, id == leader})
	}
	sort.Slice(vs, func(i, j int) bool { return vs[i].ID < vs[j].ID })
	writeJSON(w, 200, vs, map[string]int{"count": len(vs)})
}

func (g *Gateway) addVoter(w http.ResponseWriter, r *http.Request) {
	if g.Raft == nil {
		writeErr(w, 501, "NOT_A_VOTER", "ask a metadata voter node")
		return
	}
	var body struct {
		Addr string `json:"addr"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&body); err != nil || body.Addr == "" {
		writeErr(w, 400, "INVALID_BODY", `expected {"addr":"host:port"}`)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	id := r.PathValue("id")
	if err := g.Raft.AddVoter(ctx, id, body.Addr); err != nil {
		g.fail(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]string{"id": id, "addr": body.Addr}, nil)
}

func (g *Gateway) removeVoter(w http.ResponseWriter, r *http.Request) {
	if g.Raft == nil {
		writeErr(w, 501, "NOT_A_VOTER", "ask a metadata voter node")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if err := g.Raft.RemoveVoter(ctx, r.PathValue("id")); err != nil {
		g.fail(w, r, err)
		return
	}
	w.WriteHeader(204)
}

// ---- versioning ----

func (g *Gateway) bucketInfo(r *http.Request) (*meta.Bucket, error) {
	bs, err := g.Meta.ListBuckets(r.Context())
	if err != nil {
		return nil, err
	}
	for i := range bs {
		if bs[i].Name == r.PathValue("bucket") {
			return &bs[i], nil
		}
	}
	return nil, meta.ErrBucketNotFound
}

func (g *Gateway) getVersioning(w http.ResponseWriter, r *http.Request) {
	b, err := g.bucketInfo(r)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]bool{"enabled": b.Versioning, "suspended": b.Suspended}, nil)
}

// "enabled": false suspends rather than disables; existing versions stay addressable
func (g *Gateway) putVersioning(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1024)).Decode(&body); err != nil {
		writeErr(w, 400, "INVALID_BODY", `expected {"enabled":true} or {"enabled":false}`)
		return
	}
	var err error
	if body.Enabled {
		err = g.Meta.EnableVersioning(r.Context(), r.PathValue("bucket"))
	} else {
		err = g.Meta.SuspendVersioning(r.Context(), r.PathValue("bucket"))
	}
	if err != nil {
		g.fail(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]bool{"enabled": body.Enabled, "suspended": !body.Enabled}, nil)
}

func (g *Gateway) listVersions(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	// cursor is "key\x00version"
	ck, cv, _ := strings.Cut(q.Get("cursor"), "\x00")
	items, nk, nv, err := g.Meta.ListVersions(r.Context(), r.PathValue("bucket"), q.Get("prefix"), ck, cv, limit)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	if items == nil {
		items = []meta.VersionItem{}
	}
	m := map[string]any{"count": len(items)}
	if nk != "" {
		m["next_cursor"] = nk + "\x00" + nv
	}
	writeJSON(w, 200, items, m)
}

// ---- lifecycle ----

func (g *Gateway) getLifecycle(w http.ResponseWriter, r *http.Request) {
	b, err := g.bucketInfo(r)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	rules := b.Lifecycle
	if rules == nil {
		rules = []meta.LifecycleRule{}
	}
	writeJSON(w, 200, rules, map[string]int{"count": len(rules)})
}

func (g *Gateway) putLifecycle(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Rules []meta.LifecycleRule `json:"rules"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil {
		writeErr(w, 400, "INVALID_BODY", `expected {"rules":[{"id":"..","prefix":"..","expire_days":30}]}`)
		return
	}
	if err := g.Meta.SetLifecycle(r.Context(), r.PathValue("bucket"), body.Rules); err != nil {
		g.fail(w, r, err)
		return
	}
	writeJSON(w, 200, body.Rules, map[string]int{"count": len(body.Rules)})
}

func (g *Gateway) clearLifecycle(w http.ResponseWriter, r *http.Request) {
	if err := g.Meta.SetLifecycle(r.Context(), r.PathValue("bucket"), nil); err != nil {
		g.fail(w, r, err)
		return
	}
	w.WriteHeader(204)
}
