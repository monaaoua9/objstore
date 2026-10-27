// Package s3api speaks enough of the S3 REST protocol (path-style, SigV4) for stock SDKs and CLIs
// to use the cluster: buckets, objects, ListObjectsV2, CopyObject, DeleteObjects and multipart.
package s3api

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/monaaoua9/objstore/internal/cluster"
	"github.com/monaaoua9/objstore/internal/ec"
	"github.com/monaaoua9/objstore/internal/engine"
	"github.com/monaaoua9/objstore/internal/gateway"
	"github.com/monaaoua9/objstore/internal/meta"
	"github.com/monaaoua9/objstore/internal/raft"
	"github.com/monaaoua9/objstore/internal/sigv4"
)

const (
	xmlNS      = "http://s3.amazonaws.com/doc/2006-03-01/"
	userMetaH  = "X-Amz-Meta-"
	maxKeys    = 1000
	maxXMLBody = 4 << 20
)

type Server struct {
	Eng    *engine.Engine
	Meta   *meta.Client
	Region string
	Log    *slog.Logger
	Now    func() time.Time // tests pin this

	keys atomic.Pointer[keySet]
}

func New(eng *engine.Engine, mc *meta.Client, keys map[string]Key, region string, lg *slog.Logger) *Server {
	if region == "" {
		region = "us-east-1"
	}
	s := &Server{Eng: eng, Meta: mc, Region: region, Log: lg.With("component", "s3"), Now: time.Now}
	s.SetKeys(keys)
	return s
}

type s3err struct {
	Status int
	Code   string
	Msg    string
}

func (e *s3err) Error() string { return e.Code + ": " + e.Msg }

func errf(status int, code, msg string) *s3err { return &s3err{status, code, msg} }

func (s *Server) writeErr(w http.ResponseWriter, r *http.Request, e *s3err) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(e.Status)
	if r.Method == http.MethodHead {
		return
	}
	fmt.Fprintf(w, xml.Header+"<Error><Code>%s</Code><Message>%s</Message><Resource>%s</Resource></Error>",
		e.Code, xmlEsc(e.Msg), xmlEsc(r.URL.Path))
}

func xmlEsc(s string) string {
	var b strings.Builder
	xml.EscapeText(&b, []byte(s))
	return b.String()
}

func (s *Server) mapErr(err error) *s3err {
	var se *s3err
	switch {
	case errors.As(err, &se):
		return se
	case errors.Is(err, sigv4.ErrPayloadDigest):
		return errf(400, "XAmzContentSHA256Mismatch", err.Error())
	case errors.Is(err, sigv4.ErrBadSignature), errors.Is(err, sigv4.ErrChunk):
		return errf(403, "SignatureDoesNotMatch", err.Error())
	case errors.Is(err, meta.ErrBucketNotFound):
		return errf(404, "NoSuchBucket", "the specified bucket does not exist")
	case errors.Is(err, meta.ErrObjectNotFound):
		return errf(404, "NoSuchKey", "the specified key does not exist")
	case errors.Is(err, meta.ErrUploadNotFound):
		return errf(404, "NoSuchUpload", "the specified upload does not exist")
	case errors.Is(err, meta.ErrInvalidPart):
		return errf(400, "InvalidPart", err.Error())
	case errors.Is(err, meta.ErrBucketExists):
		return errf(409, "BucketAlreadyOwnedByYou", "bucket already exists")
	case errors.Is(err, meta.ErrBucketNotEmpty):
		return errf(409, "BucketNotEmpty", "the bucket you tried to delete is not empty")
	case errors.Is(err, engine.ErrTooLarge):
		return errf(400, "EntityTooLarge", err.Error())
	case errors.Is(err, engine.ErrBadRange):
		return errf(416, "InvalidRange", "the requested range is not satisfiable")
	case errors.Is(err, ec.ErrQuorum), errors.Is(err, cluster.ErrNoCapacity), errors.Is(err, ec.ErrUnrecoverable),
		errors.Is(err, raft.ErrNoLeader), errors.Is(err, raft.ErrNotLeader), errors.Is(err, raft.ErrLostLeader):
		return errf(503, "ServiceUnavailable", "please retry")
	case errors.Is(err, context.Canceled):
		return errf(499, "ClientClosedRequest", "request cancelled")
	}
	s.Log.Error("internal error", "err", err)
	return errf(500, "InternalError", "we encountered an internal error")
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ks := s.keys.Load()
	a, err := sigv4.Verify(r, ks.creds, s.Now())
	if err != nil {
		switch {
		case errors.Is(err, sigv4.ErrNoAuth):
			s.writeErr(w, r, errf(403, "AccessDenied", "anonymous access is not allowed"))
		case errors.Is(err, sigv4.ErrUnknownKey):
			s.writeErr(w, r, errf(403, "InvalidAccessKeyId", "the access key id you provided does not exist"))
		case errors.Is(err, sigv4.ErrBadSignature):
			s.writeErr(w, r, errf(403, "SignatureDoesNotMatch", "the request signature we calculated does not match"))
		case errors.Is(err, sigv4.ErrExpired):
			s.writeErr(w, r, errf(403, "RequestTimeTooSkewed", "request time too skewed or presigned url expired"))
		default:
			s.writeErr(w, r, errf(400, "AuthorizationHeaderMalformed", err.Error()))
		}
		return
	}

	// unwrap the body once, here, so handlers just read r.Body
	switch a.PayloadHash {
	case sigv4.StreamingSigned, sigv4.StreamingTrailer:
		r.Body = io.NopCloser(sigv4.DecodeChunked(r.Body, a))
	case sigv4.UnsignedPayload, "":
	default:
		r.Body = io.NopCloser(sigv4.HashVerifier(r.Body, a.PayloadHash))
	}

	bucket, key := splitPath(r.URL.Path)
	q := r.URL.Query()
	who := ks.keys[a.AccessKey]
	r.Header.Set(accessKeyHeader, a.AccessKey)
	if bucket != "" && !who.allows(bucket, r.Method != http.MethodGet && r.Method != http.MethodHead) {
		s.writeErr(w, r, errf(403, "AccessDenied", "this access key may not do that on bucket "+bucket))
		return
	}
	var herr error
	switch {
	case bucket == "":
		if r.Method == http.MethodGet {
			herr = s.listBuckets(w, r, who)
		} else {
			herr = errf(405, "MethodNotAllowed", "method not allowed")
		}
	case key == "":
		herr = s.bucketOp(w, r, bucket, q)
	default:
		herr = s.objectOp(w, r, bucket, key, q)
	}
	if herr != nil {
		s.writeErr(w, r, s.mapErr(herr))
	}
}

func splitPath(p string) (bucket, key string) {
	p = strings.TrimPrefix(p, "/")
	bucket, key, _ = strings.Cut(p, "/")
	return
}

func writeXML(w http.ResponseWriter, status int, v any) error {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	io.WriteString(w, xml.Header)
	return xml.NewEncoder(w).Encode(v)
}

func iso(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }

// ---- buckets ----

func (s *Server) listBuckets(w http.ResponseWriter, r *http.Request, who Key) error {
	bs, err := s.Meta.ListBuckets(r.Context())
	if err != nil {
		return err
	}
	type bkt struct {
		Name         string
		CreationDate string
	}
	out := struct {
		XMLName xml.Name `xml:"ListAllMyBucketsResult"`
		Xmlns   string   `xml:"xmlns,attr"`
		Owner   struct{ ID, DisplayName string }
		Buckets struct {
			Bucket []bkt
		}
	}{Xmlns: xmlNS}
	out.Owner.ID, out.Owner.DisplayName = "objstore", "objstore"
	for _, b := range bs {
		if who.allows(b.Name, false) {
			out.Buckets.Bucket = append(out.Buckets.Bucket, bkt{b.Name, iso(b.Created)})
		}
	}
	return writeXML(w, 200, out)
}

func (s *Server) bucketOp(w http.ResponseWriter, r *http.Request, bucket string, q url.Values) error {
	if strings.HasPrefix(bucket, "~") {
		return meta.ErrBucketNotFound
	}
	ctx := r.Context()
	switch r.Method {
	case http.MethodPut:
		if q.Has("versioning") {
			return s.putVersioning(w, r, bucket)
		}
		if q.Has("lifecycle") {
			return s.putLifecycle(w, r, bucket)
		}
		if len(bucket) < 3 || len(bucket) > 63 {
			return errf(400, "InvalidBucketName", "bucket name must be 3-63 characters")
		}
		if err := s.Meta.CreateBucket(ctx, bucket); err != nil {
			return err
		}
		w.Header().Set("Location", "/"+bucket)
		w.WriteHeader(200)
	case http.MethodDelete:
		if q.Has("lifecycle") {
			if err := s.Meta.SetLifecycle(ctx, bucket, nil); err != nil {
				return err
			}
			w.WriteHeader(204)
			return nil
		}
		if err := s.Meta.DeleteBucket(ctx, bucket); err != nil {
			return err
		}
		w.WriteHeader(204)
	case http.MethodHead:
		if _, _, err := s.Meta.ListObjects(ctx, bucket, "", "", 1); err != nil {
			return err
		}
		w.Header().Set("x-amz-bucket-region", s.Region)
		w.WriteHeader(200)
	case http.MethodGet:
		switch {
		case q.Has("versioning"):
			return s.getVersioning(w, r, bucket)
		case q.Has("lifecycle"):
			return s.getLifecycle(w, r, bucket)
		case q.Has("versions"):
			return s.listVersions(w, r, bucket, q)
		case q.Has("uploads"):
			return s.listUploads(w, r, bucket, q)
		case q.Has("location"):
			return writeXML(w, 200, struct {
				XMLName xml.Name `xml:"LocationConstraint"`
				Xmlns   string   `xml:"xmlns,attr"`
				Value   string   `xml:",chardata"`
			}{Xmlns: xmlNS, Value: s.Region})
		}
		return s.listObjects(w, r, bucket, q)
	case http.MethodPost:
		if q.Has("delete") {
			return s.deleteObjects(w, r, bucket)
		}
		return errf(501, "NotImplemented", "unsupported bucket operation")
	default:
		return errf(405, "MethodNotAllowed", "method not allowed")
	}
	return nil
}

type listEntry struct {
	Key          string
	LastModified string
	ETag         string
	Size         int64
	StorageClass string
}

type commonPrefix struct{ Prefix string }

func (s *Server) listObjects(w http.ResponseWriter, r *http.Request, bucket string, q url.Values) error {
	prefix, delim := q.Get("prefix"), q.Get("delimiter")
	limit := maxKeys
	if v, err := strconv.Atoi(q.Get("max-keys")); err == nil && v >= 0 && v < maxKeys {
		limit = v
	}
	v2 := q.Get("list-type") == "2"
	// v2 continuation tokens are our own: the last key (or common prefix) already returned
	after := q.Get("marker")
	if v2 {
		after = q.Get("start-after")
		if t := q.Get("continuation-token"); t != "" {
			after = t
		}
	}

	var contents []listEntry
	prefixes := map[string]bool{}
	var lastKey string
	truncated := false
	count := 0

	// keys come back in order, so rolled-up prefixes are contiguous; remember the last one to fold dupes
	var lastPrefix string
	cursor := after
	for limit > 0 && !truncated {
		items, next, err := s.Meta.ListObjects(r.Context(), bucket, prefix, cursor, 1000)
		if err != nil {
			return err
		}
		for _, it := range items {
			if delim != "" && after != "" && strings.HasSuffix(after, delim) && strings.HasPrefix(it.Key, after) {
				continue // continuation token was a common prefix; its contents were already folded
			}
			if delim != "" {
				rest := it.Key[len(prefix):]
				if j := strings.Index(rest, delim); j >= 0 {
					cp := prefix + rest[:j+len(delim)]
					if cp == lastPrefix {
						continue
					}
					if count == limit {
						truncated = true
						break
					}
					prefixes[cp] = true
					lastPrefix, lastKey = cp, cp
					count++
					continue
				}
			}
			if count == limit {
				truncated = true
				break
			}
			contents = append(contents, listEntry{it.Key, iso(it.Created), `"` + it.ETag + `"`, it.Size, "STANDARD"})
			count++
			lastKey = it.Key
		}
		if next == "" {
			break
		}
		cursor = next
	}

	cps := make([]commonPrefix, 0, len(prefixes))
	for p := range prefixes {
		cps = append(cps, commonPrefix{p})
	}
	sort.Slice(cps, func(i, j int) bool { return cps[i].Prefix < cps[j].Prefix })

	type result struct {
		XMLName               xml.Name `xml:"ListBucketResult"`
		Xmlns                 string   `xml:"xmlns,attr"`
		Name                  string
		Prefix                string
		Delimiter             string `xml:",omitempty"`
		MaxKeys               int
		KeyCount              int    `xml:",omitempty"`
		Marker                string `xml:",omitempty"`
		StartAfter            string `xml:",omitempty"`
		ContinuationToken     string `xml:",omitempty"`
		NextContinuationToken string `xml:",omitempty"`
		NextMarker            string `xml:",omitempty"`
		IsTruncated           bool
		Contents              []listEntry
		CommonPrefixes        []commonPrefix
	}
	res := result{Xmlns: xmlNS, Name: bucket, Prefix: prefix, Delimiter: delim, MaxKeys: limit,
		IsTruncated: truncated, Contents: contents, CommonPrefixes: cps}
	if q.Has("max-keys") {
		res.MaxKeys, _ = strconv.Atoi(q.Get("max-keys"))
		res.MaxKeys = min(res.MaxKeys, maxKeys)
	}
	if v2 {
		res.KeyCount = count
		res.StartAfter = q.Get("start-after")
		res.ContinuationToken = q.Get("continuation-token")
		if truncated {
			res.NextContinuationToken = lastKey
		}
	} else {
		res.Marker = q.Get("marker")
		if truncated && delim != "" {
			res.NextMarker = lastKey
		}
	}
	return writeXML(w, 200, res)
}

func (s *Server) deleteObjects(w http.ResponseWriter, r *http.Request, bucket string) error {
	var req struct {
		Quiet  bool `xml:"Quiet"`
		Object []struct {
			Key       string `xml:"Key"`
			VersionId string `xml:"VersionId"`
		} `xml:"Object"`
	}
	if err := xml.NewDecoder(io.LimitReader(r.Body, maxXMLBody)).Decode(&req); err != nil {
		return errf(400, "MalformedXML", "the xml you provided was not well-formed")
	}
	if len(req.Object) > 1000 {
		return errf(400, "MalformedXML", "at most 1000 keys per request")
	}
	type deleted struct {
		Key                   string
		VersionId             string `xml:",omitempty"`
		DeleteMarker          bool   `xml:",omitempty"`
		DeleteMarkerVersionId string `xml:",omitempty"`
	}
	type failed struct{ Key, Code, Message string }
	out := struct {
		XMLName xml.Name `xml:"DeleteResult"`
		Xmlns   string   `xml:"xmlns,attr"`
		Deleted []deleted
		Error   []failed
	}{Xmlns: xmlNS}
	for _, o := range req.Object {
		d := deleted{Key: o.Key}
		var err error
		if o.VersionId != "" {
			err = s.Eng.DeleteVersion(r.Context(), bucket, o.Key, o.VersionId)
			d.VersionId = o.VersionId
		} else {
			var m *meta.Object
			m, err = s.Eng.Delete(r.Context(), bucket, o.Key)
			if m != nil {
				d.DeleteMarker, d.DeleteMarkerVersionId = true, m.Version
			}
		}
		if err != nil && !errors.Is(err, meta.ErrObjectNotFound) {
			e := s.mapErr(err)
			out.Error = append(out.Error, failed{o.Key, e.Code, e.Msg})
			continue
		}
		if !req.Quiet {
			out.Deleted = append(out.Deleted, d)
		}
	}
	return writeXML(w, 200, out)
}

// ---- objects ----

func userMeta(h http.Header) map[string]string {
	var m map[string]string
	for k, v := range h {
		if strings.HasPrefix(k, userMetaH) && len(v) > 0 {
			if m == nil {
				m = map[string]string{}
			}
			m[strings.ToLower(strings.TrimPrefix(k, userMetaH))] = v[0]
		}
	}
	return m
}

func (s *Server) objectOp(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) error {
	if strings.HasPrefix(bucket, "~") {
		return meta.ErrBucketNotFound
	}
	uploadID := q.Get("uploadId")
	switch r.Method {
	case http.MethodPut:
		switch {
		case uploadID != "":
			return s.uploadPart(w, r, bucket, key, uploadID, q.Get("partNumber"))
		case r.Header.Get("X-Amz-Copy-Source") != "":
			return s.copyObject(w, r, bucket, key)
		}
		return s.putObject(w, r, bucket, key)
	case http.MethodGet, http.MethodHead:
		if uploadID != "" {
			return s.listParts(w, r, bucket, key, uploadID)
		}
		return s.getObject(w, r, bucket, key, q.Get("versionId"))
	case http.MethodDelete:
		if uploadID != "" {
			u, err := s.upload(r, bucket, key, uploadID)
			if err != nil {
				return err
			}
			if err := s.Meta.AbortUpload(r.Context(), u.ID); err != nil {
				return err
			}
			w.WriteHeader(204)
			return nil
		}
		if v := q.Get("versionId"); v != "" {
			if err := s.Eng.DeleteVersion(r.Context(), bucket, key, v); err != nil && !errors.Is(err, meta.ErrObjectNotFound) {
				return err
			}
			w.Header().Set("x-amz-version-id", v)
			w.WriteHeader(204)
			return nil
		}
		m, err := s.Eng.Delete(r.Context(), bucket, key)
		if err != nil && !errors.Is(err, meta.ErrObjectNotFound) {
			return err
		}
		if m != nil {
			w.Header().Set("x-amz-delete-marker", "true")
			w.Header().Set("x-amz-version-id", m.Version)
		}
		w.WriteHeader(204) // S3 is idempotent here too
	case http.MethodPost:
		switch {
		case q.Has("uploads"):
			return s.createUpload(w, r, bucket, key)
		case uploadID != "":
			return s.completeUpload(w, r, bucket, key, uploadID)
		}
		return errf(501, "NotImplemented", "unsupported object operation")
	default:
		return errf(405, "MethodNotAllowed", "method not allowed")
	}
	return nil
}

func okKey(k string) error {
	if len(k) > 1024 {
		return errf(400, "KeyTooLongError", "your key is too long")
	}
	return nil
}

func (s *Server) putObject(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	if err := okKey(key); err != nil {
		return err
	}
	opt := engine.PutOptions{ContentType: r.Header.Get("Content-Type"), UserMeta: userMeta(r.Header)}
	obj, err := s.Eng.Put(r.Context(), bucket, key, r.Body, opt)
	if err != nil {
		return err
	}
	w.Header().Set("ETag", `"`+obj.ETag+`"`)
	w.Header().Set("x-amz-version-id", obj.Version)
	w.WriteHeader(200)
	return nil
}

func (s *Server) copyObject(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	src := r.Header.Get("X-Amz-Copy-Source")
	srcVersion := ""
	if i := strings.Index(src, "?versionId="); i >= 0 {
		srcVersion, src = src[i+len("?versionId="):], src[:i]
	}
	if u, err := url.PathUnescape(src); err == nil {
		src = u
	}
	sb, sk, ok := strings.Cut(strings.TrimPrefix(src, "/"), "/")
	if !ok || sk == "" || strings.HasPrefix(sb, "~") {
		return errf(400, "InvalidArgument", "x-amz-copy-source must be bucket/key")
	}
	if !s.keys.Load().keys[sigv4Access(r)].allows(sb, false) {
		return errf(403, "AccessDenied", "this access key may not read the copy source")
	}
	ctx := r.Context()
	var obj *meta.Object
	var err error
	if srcVersion != "" {
		obj, err = s.Meta.GetObjectVersion(ctx, sb, sk, srcVersion)
	} else {
		obj, err = s.Meta.GetObject(ctx, sb, sk)
	}
	if err != nil {
		return err
	}
	opt := engine.PutOptions{ContentType: obj.ContentType, UserMeta: obj.UserMeta}
	if strings.EqualFold(r.Header.Get("X-Amz-Metadata-Directive"), "REPLACE") {
		opt = engine.PutOptions{ContentType: r.Header.Get("Content-Type"), UserMeta: userMeta(r.Header)}
	}

	var body io.Reader = strings.NewReader("")
	if obj.Size > 0 {
		rd, err := s.Eng.NewReader(ctx, obj, 0, obj.Size-1)
		if err != nil {
			return err
		}
		defer rd.Close()
		pr, pw := io.Pipe()
		go func() {
			_, err := rd.WriteTo(pw)
			pw.CloseWithError(err)
		}()
		defer pr.Close()
		body = pr
	}
	dst, err := s.Eng.Put(ctx, bucket, key, body, opt)
	if err != nil {
		return err
	}
	w.Header().Set("x-amz-version-id", dst.Version)
	return writeXML(w, 200, struct {
		XMLName      xml.Name `xml:"CopyObjectResult"`
		LastModified string
		ETag         string
	}{LastModified: iso(dst.Created), ETag: `"` + dst.ETag + `"`})
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request, bucket, key, version string) error {
	var obj *meta.Object
	var err error
	if version != "" {
		obj, err = s.Meta.GetObjectVersion(r.Context(), bucket, key, version)
	} else {
		obj, err = s.Meta.GetObject(r.Context(), bucket, key)
	}
	if err != nil {
		if errors.Is(err, meta.ErrDeleteMarker) {
			w.Header().Set("x-amz-delete-marker", "true")
			w.Header().Set("x-amz-version-id", obj.Version)
			if version != "" { // S3: asking for a marker by id is a method error, not a miss
				return errf(405, "MethodNotAllowed", "the specified version is a delete marker")
			}
		}
		return err
	}
	h := w.Header()
	h.Set("x-amz-version-id", obj.Version)
	etag := `"` + obj.ETag + `"`
	h.Set("ETag", etag)
	h.Set("Accept-Ranges", "bytes")
	h.Set("Last-Modified", obj.Created.UTC().Format(http.TimeFormat))
	ct := obj.ContentType
	if ct == "" {
		ct = "binary/octet-stream"
	}
	h.Set("Content-Type", ct)
	for k, v := range obj.UserMeta {
		h.Set(userMetaH+k, v)
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" && strings.Contains(inm, etag) {
		w.WriteHeader(http.StatusNotModified)
		return nil
	}
	if im := r.Header.Get("If-Match"); im != "" && im != "*" && !strings.Contains(im, etag) {
		return errf(412, "PreconditionFailed", "at least one of the preconditions you specified did not hold")
	}

	start, end, partial, ok := gateway.ParseRange(r.Header.Get("Range"), obj.Size)
	if !ok {
		return errf(416, "InvalidRange", "the requested range is not satisfiable")
	}
	length := end - start + 1
	if obj.Size == 0 {
		length = 0
	}
	h.Set("Content-Length", strconv.FormatInt(length, 10))
	if partial {
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, obj.Size))
	}
	if r.Method == http.MethodHead || length == 0 {
		if partial {
			w.WriteHeader(206)
		}
		return nil
	}

	rd, err := s.Eng.NewReader(r.Context(), obj, start, end)
	if err != nil {
		return err
	}
	defer rd.Close()
	if partial {
		w.WriteHeader(206)
	}
	if _, err := rd.WriteTo(w); err != nil && !errors.Is(err, context.Canceled) {
		s.Log.Warn("download aborted mid-stream", "bucket", bucket, "key", key, "err", err)
		panic(http.ErrAbortHandler)
	}
	return nil
}

// ---- multipart ----

func (s *Server) upload(r *http.Request, bucket, key, id string) (*meta.Upload, error) {
	u, err := s.Meta.GetUpload(r.Context(), id)
	if err != nil {
		return nil, err
	}
	if u.Bucket != bucket || u.Key != key {
		return nil, meta.ErrUploadNotFound
	}
	return u, nil
}

func (s *Server) createUpload(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	if err := okKey(key); err != nil {
		return err
	}
	u := meta.Upload{
		ID: engine.NewID(), Bucket: bucket, Key: key, ContentType: r.Header.Get("Content-Type"),
		UserMeta: userMeta(r.Header), Created: s.Now().UTC().Truncate(time.Millisecond),
	}
	if err := s.Meta.CreateUpload(r.Context(), u); err != nil {
		return err
	}
	return writeXML(w, 200, struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Xmlns    string   `xml:"xmlns,attr"`
		Bucket   string
		Key      string
		UploadId string
	}{Xmlns: xmlNS, Bucket: bucket, Key: key, UploadId: u.ID})
}

func (s *Server) uploadPart(w http.ResponseWriter, r *http.Request, bucket, key, id, num string) error {
	n, err := strconv.Atoi(num)
	if err != nil || n < 1 || n > 10000 {
		return errf(400, "InvalidArgument", "part number must be an integer between 1 and 10000")
	}
	u, err := s.upload(r, bucket, key, id)
	if err != nil {
		return err
	}
	obj, err := s.Eng.PutPart(r.Context(), u.ID, n, r.Body)
	if err != nil {
		return err
	}
	w.Header().Set("ETag", `"`+obj.ETag+`"`)
	w.WriteHeader(200)
	return nil
}

func (s *Server) listParts(w http.ResponseWriter, r *http.Request, bucket, key, id string) error {
	u, err := s.upload(r, bucket, key, id)
	if err != nil {
		return err
	}
	items, _, err := s.Meta.ListObjects(r.Context(), meta.MPUBucket, u.ID+"/", "", 10000)
	if err != nil {
		return err
	}
	type part struct {
		PartNumber   int
		LastModified string
		ETag         string
		Size         int64
	}
	out := struct {
		XMLName  xml.Name `xml:"ListPartsResult"`
		Xmlns    string   `xml:"xmlns,attr"`
		Bucket   string
		Key      string
		UploadId string
		Part     []part
	}{Xmlns: xmlNS, Bucket: bucket, Key: key, UploadId: id}
	for _, it := range items {
		n, _ := strconv.Atoi(strings.TrimPrefix(it.Key, u.ID+"/"))
		out.Part = append(out.Part, part{n, iso(it.Created), `"` + it.ETag + `"`, it.Size})
	}
	sort.Slice(out.Part, func(i, j int) bool { return out.Part[i].PartNumber < out.Part[j].PartNumber })
	return writeXML(w, 200, out)
}

func (s *Server) listUploads(w http.ResponseWriter, r *http.Request, bucket string, q url.Values) error {
	all, err := s.Meta.ListUploads(r.Context())
	if err != nil {
		return err
	}
	type up struct {
		Key       string
		UploadId  string
		Initiated string
	}
	out := struct {
		XMLName xml.Name `xml:"ListMultipartUploadsResult"`
		Xmlns   string   `xml:"xmlns,attr"`
		Bucket  string
		Upload  []up
	}{Xmlns: xmlNS, Bucket: bucket}
	prefix := q.Get("prefix")
	for _, u := range all {
		if u.Bucket == bucket && strings.HasPrefix(u.Key, prefix) {
			out.Upload = append(out.Upload, up{u.Key, u.ID, iso(u.Created)})
		}
	}
	sort.Slice(out.Upload, func(i, j int) bool { return out.Upload[i].Key < out.Upload[j].Key })
	return writeXML(w, 200, out)
}

func (s *Server) completeUpload(w http.ResponseWriter, r *http.Request, bucket, key, id string) error {
	u, err := s.upload(r, bucket, key, id)
	if err != nil {
		return err
	}
	var req struct {
		Part []struct {
			PartNumber int    `xml:"PartNumber"`
			ETag       string `xml:"ETag"`
		} `xml:"Part"`
	}
	if err := xml.NewDecoder(io.LimitReader(r.Body, maxXMLBody)).Decode(&req); err != nil || len(req.Part) == 0 {
		return errf(400, "MalformedXML", "the xml you provided was not well-formed")
	}
	sel := make([]meta.PartSel, len(req.Part))
	for i, p := range req.Part {
		sel[i] = meta.PartSel{Number: p.PartNumber, ETag: strings.Trim(p.ETag, `"`)}
	}
	obj, err := s.Meta.CompleteUpload(r.Context(), u.ID, sel)
	if err != nil {
		return err
	}
	return writeXML(w, 200, struct {
		XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
		Xmlns    string   `xml:"xmlns,attr"`
		Location string
		Bucket   string
		Key      string
		ETag     string
	}{Xmlns: xmlNS, Location: "/" + bucket + "/" + key, Bucket: bucket, Key: key, ETag: `"` + obj.ETag + `"`})
}
