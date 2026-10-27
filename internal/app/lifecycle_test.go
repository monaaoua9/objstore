package app

import (
	"strings"
	"testing"
	"time"
)

// test clusters run lifecycle with a 150ms "day" (see harness config)

func (c *tcluster) setRules(via, bucket, rulesJSON string) {
	c.t.Helper()
	resp, b := c.do("PUT", c.base(via)+"/api/v1/buckets/"+bucket+"/lifecycle", strings.NewReader(rulesJSON), nil)
	if resp.StatusCode != 200 {
		c.t.Fatalf("set lifecycle: %d %s", resp.StatusCode, b)
	}
}

func TestLifecycleExpiresByPrefix(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2)
	c.mkbucket("n1", "tmpdata")
	c.put("n1", "tmpdata", "keep/forever", randBytes(5000))
	c.setRules("n2", "tmpdata", `{"rules":[{"id":"logs","prefix":"logs/","expire_days":3}]}`)
	c.put("n1", "tmpdata", "logs/today", randBytes(5000))

	eventually(t, 10*time.Second, "logs/ expire", func() bool {
		resp, _ := c.get("n3", "tmpdata", "logs/today", nil)
		return resp.StatusCode == 404
	})
	resp, _ := c.get("n3", "tmpdata", "keep/forever", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("object outside the rule prefix was touched: %d", resp.StatusCode)
	}
	// and the shard files are reclaimed through the usual GC
	eventually(t, 10*time.Second, "gc of expired object", func() bool {
		return c.nodes["n1"].Blob.Count()+c.nodes["n2"].Blob.Count()+c.nodes["n3"].Blob.Count()+
			c.nodes["n4"].Blob.Count()+c.nodes["n5"].Blob.Count()+c.nodes["n6"].Blob.Count() == 5
	})
}

func TestLifecycleVersionedCleanup(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2)
	c.s3("n1", "PUT", "/hist2", nil, nil)
	c.s3("n1", "PUT", "/hist2?versioning", []byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil)

	resp, b := c.s3("n1", "PUT", "/hist2/k", []byte("one"), nil)
	want(t, resp, b, 200)
	v1 := resp.Header.Get("x-amz-version-id")
	resp, b = c.s3("n1", "PUT", "/hist2/k", []byte("two"), nil)
	want(t, resp, b, 200)
	v2 := resp.Header.Get("x-amz-version-id")

	rules := `<LifecycleConfiguration><Rule><ID>tidy</ID><Status>Enabled</Status><Filter><Prefix></Prefix></Filter>` +
		`<Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>` +
		`<NoncurrentVersionExpiration><NoncurrentDays>2</NoncurrentDays></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>`
	resp, b = c.s3("n2", "PUT", "/hist2?lifecycle", []byte(rules), nil)
	want(t, resp, b, 200)
	resp, b = c.s3("n3", "GET", "/hist2?lifecycle", nil, nil)
	want(t, resp, b, 200)
	if !strings.Contains(string(b), "<NoncurrentDays>2</NoncurrentDays>") || !strings.Contains(string(b), "<ID>tidy</ID>") {
		t.Fatalf("rules did not round-trip: %s", b)
	}

	// v1 was replaced by v2, so only v1 ages out; the current version stays
	eventually(t, 10*time.Second, "noncurrent v1 removed", func() bool {
		r, _ := c.s3("n1", "GET", "/hist2/k?versionId="+v1, nil, nil)
		return r.StatusCode == 404
	})
	resp, b = c.s3("n1", "GET", "/hist2/k", nil, nil)
	if resp.StatusCode != 200 || string(b) != "two" {
		t.Fatalf("current version damaged: %d %q", resp.StatusCode, b)
	}

	// delete it: v2 goes noncurrent behind a marker, then it and finally the lone marker disappear
	resp, b = c.s3("n1", "DELETE", "/hist2/k", nil, nil)
	want(t, resp, b, 204)
	eventually(t, 15*time.Second, "everything cleaned up", func() bool {
		r, body := c.s3("n1", "GET", "/hist2?versions", nil, nil)
		return r.StatusCode == 200 && !strings.Contains(string(body), "<Key>k</Key>")
	})
	if r, body := c.s3("n1", "GET", "/hist2/k?versionId="+v2, nil, nil); r.StatusCode != 404 {
		t.Fatalf("v2 should be gone: %d %s", r.StatusCode, body)
	}
	resp, b = c.s3("n1", "DELETE", "/hist2", nil, nil)
	want(t, resp, b, 204) // an empty bucket again
}

func TestLifecycleRejectsNonsense(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2)
	c.mkbucket("n1", "badrules")
	for _, body := range []string{
		`{"rules":[{"id":"x"}]}`,
		`{"rules":[{"id":"x","expire_days":-1}]}`,
		`{"rules":[{"id":"x","expire_days":1},{"id":"x","expire_days":2}]}`,
		`{"rules":[{"expire_days":1}]}`,
	} {
		resp, b := c.do("PUT", c.base("n1")+"/api/v1/buckets/badrules/lifecycle", strings.NewReader(body), nil)
		if resp.StatusCode != 400 || !strings.Contains(string(b), "INVALID_LIFECYCLE") {
			t.Errorf("%s: %d %s", body, resp.StatusCode, b)
		}
	}
	resp, _ := c.do("DELETE", c.base("n1")+"/api/v1/buckets/badrules/lifecycle", nil, nil)
	if resp.StatusCode != 204 {
		t.Fatalf("clear: %d", resp.StatusCode)
	}
}
