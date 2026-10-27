package s3api

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"

	"github.com/monaaoua9/objstore/internal/meta"
)

type lcRule struct {
	ID     string `xml:"ID"`
	Status string `xml:"Status"`
	Prefix string `xml:"Prefix"`
	Filter *struct {
		Prefix string `xml:"Prefix"`
		And    *struct {
			Prefix string `xml:"Prefix"`
		} `xml:"And"`
	} `xml:"Filter"`
	Expiration *struct {
		Days                      int  `xml:"Days"`
		ExpiredObjectDeleteMarker bool `xml:"ExpiredObjectDeleteMarker"`
	} `xml:"Expiration"`
	Noncurrent *struct {
		Days int `xml:"NoncurrentDays"`
	} `xml:"NoncurrentVersionExpiration"`
}

func (s *Server) putLifecycle(w http.ResponseWriter, r *http.Request, bucket string) error {
	var req struct {
		Rule []lcRule `xml:"Rule"`
	}
	if err := xml.NewDecoder(io.LimitReader(r.Body, maxXMLBody)).Decode(&req); err != nil {
		return errf(400, "MalformedXML", "the xml you provided was not well-formed")
	}
	rules := make([]meta.LifecycleRule, 0, len(req.Rule))
	for _, x := range req.Rule {
		lr := meta.LifecycleRule{ID: x.ID, Prefix: x.Prefix, Disabled: x.Status == "Disabled"}
		if x.Filter != nil {
			lr.Prefix = x.Filter.Prefix
			if x.Filter.And != nil {
				lr.Prefix = x.Filter.And.Prefix
			}
		}
		if x.Expiration != nil {
			lr.ExpireDays, lr.CleanMarkers = x.Expiration.Days, x.Expiration.ExpiredObjectDeleteMarker
		}
		if x.Noncurrent != nil {
			lr.NoncurrentDays = x.Noncurrent.Days
		}
		rules = append(rules, lr)
	}
	if err := s.Meta.SetLifecycle(r.Context(), bucket, rules); err != nil {
		if errors.Is(err, meta.ErrInvalidLifecycle) {
			return errf(400, "InvalidArgument", err.Error())
		}
		return err
	}
	w.WriteHeader(200)
	return nil
}

func (s *Server) getLifecycle(w http.ResponseWriter, r *http.Request, bucket string) error {
	bs, err := s.Meta.ListBuckets(r.Context())
	if err != nil {
		return err
	}
	var rules []meta.LifecycleRule
	found := false
	for _, b := range bs {
		if b.Name == bucket {
			rules, found = b.Lifecycle, true
		}
	}
	if !found {
		return meta.ErrBucketNotFound
	}
	if len(rules) == 0 {
		return errf(404, "NoSuchLifecycleConfiguration", "the lifecycle configuration does not exist")
	}
	type exp struct {
		Days                      int  `xml:",omitempty"`
		ExpiredObjectDeleteMarker bool `xml:",omitempty"`
	}
	type nc struct{ NoncurrentDays int }
	type filter struct{ Prefix string }
	type rule struct {
		ID         string
		Status     string
		Filter     filter
		Expiration *exp `xml:",omitempty"`
		Noncurrent *nc  `xml:"NoncurrentVersionExpiration,omitempty"`
	}
	out := struct {
		XMLName xml.Name `xml:"LifecycleConfiguration"`
		Xmlns   string   `xml:"xmlns,attr"`
		Rule    []rule
	}{Xmlns: xmlNS}
	for _, lr := range rules {
		x := rule{ID: lr.ID, Status: "Enabled", Filter: filter{lr.Prefix}}
		if lr.Disabled {
			x.Status = "Disabled"
		}
		if lr.ExpireDays > 0 || lr.CleanMarkers {
			x.Expiration = &exp{lr.ExpireDays, lr.CleanMarkers}
		}
		if lr.NoncurrentDays > 0 {
			x.Noncurrent = &nc{lr.NoncurrentDays}
		}
		out.Rule = append(out.Rule, x)
	}
	return writeXML(w, 200, out)
}
