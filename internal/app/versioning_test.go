package app

import (
	"bytes"
	"encoding/xml"
	"strings"
	"testing"
	"time"
)

func TestS3Versioning(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2)
	c.s3("n1", "PUT", "/vers", nil, nil)

	// off by default
	resp, b := c.s3("n1", "GET", "/vers?versioning", nil, nil)
	want(t, resp, b, 200)
	if strings.Contains(string(b), "Enabled") {
		t.Fatalf("versioning should start off: %s", b)
	}
	resp, b = c.s3("n2", "PUT", "/vers?versioning", []byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil)
	want(t, resp, b, 200)
	resp, b = c.s3("n3", "GET", "/vers?versioning", nil, nil)
	if !strings.Contains(string(b), "<Status>Enabled</Status>") {
		t.Fatalf("versioning not reported: %s", b)
	}

	d1, d2 := randBytes(90_000), randBytes(70_000)
	resp, b = c.s3("n1", "PUT", "/vers/doc.txt", d1, nil)
	want(t, resp, b, 200)
	v1 := resp.Header.Get("x-amz-version-id")
	resp, b = c.s3("n2", "PUT", "/vers/doc.txt", d2, nil)
	want(t, resp, b, 200)
	v2 := resp.Header.Get("x-amz-version-id")
	if v1 == "" || v2 == "" || v1 == v2 {
		t.Fatalf("version ids: %q %q", v1, v2)
	}

	resp, b = c.s3("n3", "GET", "/vers/doc.txt", nil, nil)
	want(t, resp, b, 200)
	if !bytes.Equal(b, d2) || resp.Header.Get("x-amz-version-id") != v2 {
		t.Fatal("current version should be the second write")
	}
	resp, b = c.s3("n3", "GET", "/vers/doc.txt?versionId="+v1, nil, nil)
	want(t, resp, b, 200)
	if !bytes.Equal(b, d1) {
		t.Fatal("old version has wrong bytes")
	}
	resp, b = c.s3("n3", "GET", "/vers/doc.txt?versionId="+v1, nil, map[string]string{"Range": "bytes=10-19"})
	want(t, resp, b, 206)
	if !bytes.Equal(b, d1[10:20]) {
		t.Fatal("range on old version")
	}

	// plain delete hides the key behind a marker
	resp, b = c.s3("n1", "DELETE", "/vers/doc.txt", nil, nil)
	want(t, resp, b, 204)
	marker := resp.Header.Get("x-amz-version-id")
	if resp.Header.Get("x-amz-delete-marker") != "true" || marker == "" {
		t.Fatalf("delete response headers: %v", resp.Header)
	}
	resp, b = c.s3("n2", "GET", "/vers/doc.txt", nil, nil)
	if resp.StatusCode != 404 || resp.Header.Get("x-amz-delete-marker") != "true" {
		t.Fatalf("read behind marker: %d %v", resp.StatusCode, resp.Header)
	}
	resp, b = c.s3("n2", "GET", "/vers?list-type=2", nil, nil)
	want(t, resp, b, 200)
	var lst s3List
	xml.Unmarshal(b, &lst)
	if len(lst.Contents) != 0 {
		t.Fatalf("deleted key listed: %s", b)
	}
	resp, b = c.s3("n2", "GET", "/vers?versions", nil, nil)
	want(t, resp, b, 200)
	var vl struct {
		Version      []struct{ VersionId string }
		DeleteMarker []struct {
			VersionId string
			IsLatest  bool
		}
	}
	if err := xml.Unmarshal(b, &vl); err != nil || len(vl.Version) != 2 || len(vl.DeleteMarker) != 1 || !vl.DeleteMarker[0].IsLatest || vl.DeleteMarker[0].VersionId != marker {
		t.Fatalf("version listing: %v %s", err, b)
	}

	// deleting the marker undeletes
	resp, b = c.s3("n1", "DELETE", "/vers/doc.txt?versionId="+marker, nil, nil)
	want(t, resp, b, 204)
	resp, b = c.s3("n3", "GET", "/vers/doc.txt", nil, nil)
	want(t, resp, b, 200)
	if !bytes.Equal(b, d2) {
		t.Fatal("undelete did not restore the latest version")
	}

	// permanent delete of a specific version, via the bulk API
	del := `<Delete><Object><Key>doc.txt</Key><VersionId>` + v1 + `</VersionId></Object></Delete>`
	resp, b = c.s3("n1", "POST", "/vers?delete", []byte(del), nil)
	want(t, resp, b, 200)
	resp, b = c.s3("n1", "GET", "/vers/doc.txt?versionId="+v1, nil, nil)
	if resp.StatusCode != 404 {
		t.Fatalf("version survived permanent delete: %d", resp.StatusCode)
	}

	// copy from an explicit old version
	resp, b = c.s3("n1", "PUT", "/vers/doc.txt", d1, nil)
	want(t, resp, b, 200)
	v3 := resp.Header.Get("x-amz-version-id")
	resp, b = c.s3("n1", "PUT", "/vers/copy.txt", nil, map[string]string{"X-Amz-Copy-Source": "/vers/doc.txt?versionId=" + v2})
	want(t, resp, b, 200)
	resp, b = c.s3("n2", "GET", "/vers/copy.txt", nil, nil)
	if !bytes.Equal(b, d2) {
		t.Fatalf("copy of old version %s (current is %s) has wrong content", v2, v3)
	}
}

// the old version's shards must stay protected: lose nodes and read the previous version back
func TestOldVersionsSurviveNodeLoss(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2)
	c.s3("n1", "PUT", "/hist", nil, nil)
	c.s3("n1", "PUT", "/hist?versioning", []byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil)

	d1, d2 := randBytes(200_000), randBytes(200_000)
	resp, b := c.s3("n1", "PUT", "/hist/k", d1, nil)
	want(t, resp, b, 200)
	v1 := resp.Header.Get("x-amz-version-id")
	c.s3("n1", "PUT", "/hist/k", d2, nil)

	c.kill("n5", "n6")
	time.Sleep(500 * time.Millisecond)
	resp, b = c.s3("n1", "GET", "/hist/k?versionId="+v1, nil, nil)
	want(t, resp, b, 200)
	if !bytes.Equal(b, d1) {
		t.Fatal("old version unreadable with m nodes down")
	}
}

func TestS3VersioningSuspend(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2)
	c.s3("n1", "PUT", "/susp", nil, nil)
	susp := []byte(`<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>`)
	on := []byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`)

	resp, b := c.s3("n1", "PUT", "/susp?versioning", on, nil)
	want(t, resp, b, 200)
	d1 := randBytes(40_000)
	resp, b = c.s3("n1", "PUT", "/susp/k", d1, nil)
	want(t, resp, b, 200)
	v1 := resp.Header.Get("x-amz-version-id")

	resp, b = c.s3("n2", "PUT", "/susp?versioning", susp, nil)
	want(t, resp, b, 200)
	resp, b = c.s3("n3", "GET", "/susp?versioning", nil, nil)
	if !strings.Contains(string(b), "<Status>Suspended</Status>") {
		t.Fatalf("suspended not reported: %s", b)
	}

	// two writes while suspended leave exactly one null version on top of v1
	c.s3("n1", "PUT", "/susp/k", randBytes(10_000), nil)
	d3 := randBytes(20_000)
	resp, b = c.s3("n2", "PUT", "/susp/k", d3, nil)
	want(t, resp, b, 200)
	resp, b = c.s3("n3", "GET", "/susp?versions", nil, nil)
	want(t, resp, b, 200)
	if n := strings.Count(string(b), "<Version>"); n != 2 {
		t.Fatalf("want 2 versions (null + v1), got %d: %s", n, b)
	}
	if !strings.Contains(string(b), "<VersionId>null</VersionId>") || !strings.Contains(string(b), v1) {
		t.Fatalf("listing: %s", b)
	}
	resp, b = c.s3("n1", "GET", "/susp/k?versionId="+v1, nil, nil)
	want(t, resp, b, 200)
	if !bytes.Equal(b, d1) {
		t.Fatal("enabled-era version lost")
	}
	resp, b = c.s3("n1", "GET", "/susp/k?versionId=null", nil, nil)
	want(t, resp, b, 200)
	if !bytes.Equal(b, d3) {
		t.Fatal("null version should hold the last write")
	}

	// delete while suspended: null marker replaces the null version, v1 survives
	resp, b = c.s3("n1", "DELETE", "/susp/k", nil, nil)
	want(t, resp, b, 204)
	resp, b = c.s3("n1", "GET", "/susp/k", nil, nil)
	if resp.StatusCode != 404 {
		t.Fatalf("deleted key should 404: %d", resp.StatusCode)
	}
	resp, b = c.s3("n1", "GET", "/susp/k?versionId="+v1, nil, nil)
	want(t, resp, b, 200)
	resp, b = c.s3("n3", "GET", "/susp?versions", nil, nil)
	if n := strings.Count(string(b), "<Version>"); n != 1 || strings.Count(string(b), "<DeleteMarker>") != 1 {
		t.Fatalf("after suspended delete: %s", b)
	}
}
