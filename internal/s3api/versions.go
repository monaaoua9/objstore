package s3api

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/monaaoua9/objstore/internal/meta"
)

// ---- versioning ----

func (s *Server) putVersioning(w http.ResponseWriter, r *http.Request, bucket string) error {
	var req struct {
		Status string `xml:"Status"`
	}
	if err := xml.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil {
		return errf(400, "MalformedXML", "the xml you provided was not well-formed")
	}
	if req.Status != "Enabled" {
		return errf(501, "NotImplemented", "versioning can be enabled but not suspended")
	}
	if err := s.Meta.EnableVersioning(r.Context(), bucket); err != nil {
		return err
	}
	w.WriteHeader(200)
	return nil
}

func (s *Server) getVersioning(w http.ResponseWriter, r *http.Request, bucket string) error {
	bs, err := s.Meta.ListBuckets(r.Context())
	if err != nil {
		return err
	}
	status := ""
	found := false
	for _, b := range bs {
		if b.Name == bucket {
			found = true
			if b.Versioning {
				status = "Enabled"
			}
		}
	}
	if !found {
		return meta.ErrBucketNotFound
	}
	return writeXML(w, 200, struct {
		XMLName xml.Name `xml:"VersioningConfiguration"`
		Xmlns   string   `xml:"xmlns,attr"`
		Status  string   `xml:",omitempty"`
	}{Xmlns: xmlNS, Status: status})
}

func (s *Server) listVersions(w http.ResponseWriter, r *http.Request, bucket string, q url.Values) error {
	limit := maxKeys
	if v, err := strconv.Atoi(q.Get("max-keys")); err == nil && v > 0 && v < maxKeys {
		limit = v
	}
	items, nk, nv, err := s.Meta.ListVersions(r.Context(), bucket, q.Get("prefix"), q.Get("key-marker"), q.Get("version-id-marker"), limit)
	if err != nil {
		return err
	}
	type ver struct {
		Key          string
		VersionId    string
		IsLatest     bool
		LastModified string
		ETag         string
		Size         int64
		StorageClass string
	}
	type marker struct {
		Key          string
		VersionId    string
		IsLatest     bool
		LastModified string
	}
	out := struct {
		XMLName             xml.Name `xml:"ListVersionsResult"`
		Xmlns               string   `xml:"xmlns,attr"`
		Name                string
		Prefix              string
		KeyMarker           string
		VersionIdMarker     string
		NextKeyMarker       string `xml:",omitempty"`
		NextVersionIdMarker string `xml:",omitempty"`
		MaxKeys             int
		IsTruncated         bool
		Version             []ver
		DeleteMarker        []marker
	}{Xmlns: xmlNS, Name: bucket, Prefix: q.Get("prefix"), KeyMarker: q.Get("key-marker"),
		VersionIdMarker: q.Get("version-id-marker"), MaxKeys: limit, IsTruncated: nk != "",
		NextKeyMarker: nk, NextVersionIdMarker: nv}
	for _, it := range items {
		if it.DeleteMarker {
			out.DeleteMarker = append(out.DeleteMarker, marker{it.Key, it.Version, it.IsLatest, iso(it.Created)})
			continue
		}
		out.Version = append(out.Version, ver{it.Key, it.Version, it.IsLatest, iso(it.Created), `"` + it.ETag + `"`, it.Size, "STANDARD"})
	}
	return writeXML(w, 200, out)
}
