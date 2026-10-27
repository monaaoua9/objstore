package app

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/monaaoua9/objstore/internal/sigv4"
)

const (
	testAK = "AKIATESTKEY"
	testSK = "test/secret+key"

	roAK     = "AKIAREADONLY"
	scopedAK = "AKIASCOPED"
)

func (c *tcluster) s3addr(via string) string {
	c.nmu.Lock()
	defer c.nmu.Unlock()
	return c.nodes[via].S3Addr()
}

// s3 sends a SigV4-signed request to a node's S3 port.
func (c *tcluster) s3(via, method, path string, body []byte, hdr map[string]string) (*http.Response, []byte) {
	c.t.Helper()
	return c.s3with(via, method, path, body, hdr, testSK, false)
}

func (c *tcluster) s3with(via, method, path string, body []byte, hdr map[string]string, secret string, chunked bool) (*http.Response, []byte) {
	c.t.Helper()
	return c.s3as(testAK, secret, via, method, path, body, hdr, chunked)
}

func (c *tcluster) s3as(ak, secret, via, method, path string, body []byte, hdr map[string]string, chunked bool) (*http.Response, []byte) {
	c.t.Helper()
	req, err := http.NewRequest(method, "http://"+c.s3addr(via)+path, nil)
	if err != nil {
		c.t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	sum := sha256.Sum256(body)
	ph := hex.EncodeToString(sum[:])
	if chunked {
		ph = sigv4.StreamingSigned
		req.Header.Set("X-Amz-Decoded-Content-Length", fmt.Sprint(len(body)))
	}
	a, err := sigv4.Sign(req, ak, secret, "us-east-1", "s3", ph, time.Now())
	if err != nil {
		c.t.Fatal(err)
	}
	if chunked {
		body = sigv4.EncodeChunked(body, 10_000, a)
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func want(t *testing.T, resp *http.Response, b []byte, code int) {
	t.Helper()
	if resp.StatusCode != code {
		t.Fatalf("status %d (want %d): %s", resp.StatusCode, code, b)
	}
}

type s3List struct {
	IsTruncated           bool
	NextContinuationToken string
	KeyCount              int
	Contents              []struct{ Key string }
	CommonPrefixes        []struct{ Prefix string }
}

func TestS3Basics(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2)

	resp, b := c.s3("n1", "PUT", "/photos", nil, nil)
	want(t, resp, b, 200)
	resp, b = c.s3("n2", "PUT", "/photos", nil, nil)
	if resp.StatusCode != 409 || !strings.Contains(string(b), "BucketAlreadyOwnedByYou") {
		t.Fatalf("duplicate bucket: %d %s", resp.StatusCode, b)
	}

	data := randBytes(300_000)
	resp, b = c.s3("n1", "PUT", "/photos/2024/cat.jpg", data, map[string]string{"Content-Type": "image/jpeg", "X-Amz-Meta-Owner": "ali"})
	want(t, resp, b, 200)
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Fatal("no etag")
	}

	// read it from a different gateway
	resp, b = c.s3("n3", "GET", "/photos/2024/cat.jpg", nil, nil)
	want(t, resp, b, 200)
	if !bytes.Equal(b, data) || resp.Header.Get("Content-Type") != "image/jpeg" || resp.Header.Get("X-Amz-Meta-Owner") != "ali" || resp.Header.Get("ETag") != etag {
		t.Fatalf("get mismatch: ct=%q meta=%q", resp.Header.Get("Content-Type"), resp.Header.Get("X-Amz-Meta-Owner"))
	}

	resp, b = c.s3("n2", "GET", "/photos/2024/cat.jpg", nil, map[string]string{"Range": "bytes=1000-1999"})
	want(t, resp, b, 206)
	if !bytes.Equal(b, data[1000:2000]) || resp.Header.Get("Content-Range") != "bytes 1000-1999/300000" {
		t.Fatalf("range wrong: %q", resp.Header.Get("Content-Range"))
	}

	resp, _ = c.s3("n2", "HEAD", "/photos/2024/cat.jpg", nil, nil)
	if resp.StatusCode != 200 || resp.Header.Get("Content-Length") != "300000" {
		t.Fatalf("head: %d len=%s", resp.StatusCode, resp.Header.Get("Content-Length"))
	}

	// keys that need escaping in the canonical request
	odd := "dir with space/ключ+plus&amp.txt"
	oddPath := "/photos/" + (&url.URL{Path: odd}).EscapedPath()
	resp, b = c.s3("n1", "PUT", oddPath, []byte("odd"), nil)
	want(t, resp, b, 200)
	resp, b = c.s3("n2", "GET", oddPath, nil, nil)
	want(t, resp, b, 200)
	if string(b) != "odd" {
		t.Fatalf("odd key body %q", b)
	}

	resp, b = c.s3("n1", "GET", "/photos/missing", nil, nil)
	if resp.StatusCode != 404 || !strings.Contains(string(b), "NoSuchKey") {
		t.Fatalf("missing: %d %s", resp.StatusCode, b)
	}

	resp, b = c.s3("n1", "GET", "/", nil, nil)
	want(t, resp, b, 200)
	if !strings.Contains(string(b), "<Name>photos</Name>") {
		t.Fatalf("bucket list: %s", b)
	}

	resp, b = c.s3("n1", "DELETE", "/photos", nil, nil)
	if resp.StatusCode != 409 || !strings.Contains(string(b), "BucketNotEmpty") {
		t.Fatalf("delete non-empty bucket: %d %s", resp.StatusCode, b)
	}
}

func TestS3Auth(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2)
	c.s3("n1", "PUT", "/secure", nil, nil)

	resp, b := c.s3with("n1", "PUT", "/secure/k", []byte("x"), nil, "wrong-secret", false)
	if resp.StatusCode != 403 || !strings.Contains(string(b), "SignatureDoesNotMatch") {
		t.Fatalf("bad secret: %d %s", resp.StatusCode, b)
	}

	req, _ := http.NewRequest("GET", "http://"+c.s3addr("n1")+"/secure/k", nil)
	r2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r2.Body.Close()
	if r2.StatusCode != 403 {
		t.Fatalf("anonymous: %d", r2.StatusCode)
	}

	c.s3("n1", "PUT", "/secure/shared.txt", []byte("hi there"), nil)
	pre, _ := http.NewRequest("GET", "http://"+c.s3addr("n2")+"/secure/shared.txt", nil)
	u := sigv4.Presign(pre, testAK, testSK, "us-east-1", time.Minute, time.Now())
	r3, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	pb, _ := io.ReadAll(r3.Body)
	r3.Body.Close()
	if r3.StatusCode != 200 || string(pb) != "hi there" {
		t.Fatalf("presigned: %d %q", r3.StatusCode, pb)
	}
}

// a body that doesn't match the signed x-amz-content-sha256 must not end up stored
func TestS3RejectsPayloadMismatch(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2)
	c.s3("n1", "PUT", "/bkt", nil, nil)

	sent := "what I sent instead"
	req, _ := http.NewRequest("PUT", "http://"+c.s3addr("n1")+"/bkt/evil", nil)
	sum := sha256.Sum256([]byte("what I signed"))
	sigv4.Sign(req, testAK, testSK, "us-east-1", "s3", hex.EncodeToString(sum[:]), time.Now())
	req.Body = io.NopCloser(strings.NewReader(sent))
	req.ContentLength = int64(len(sent))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode < 400 {
		t.Fatalf("tampered body accepted: %d", resp.StatusCode)
	}
	resp2, b := c.s3("n1", "GET", "/bkt/evil", nil, nil)
	if resp2.StatusCode != 404 {
		t.Fatalf("object exists after rejected put: %d %s", resp2.StatusCode, b)
	}
}

func TestS3ChunkedUpload(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2)
	c.s3("n1", "PUT", "/bkt", nil, nil)
	data := randBytes(123_456)
	resp, b := c.s3with("n1", "PUT", "/bkt/streamed", data, nil, testSK, true)
	want(t, resp, b, 200)
	resp, b = c.s3("n2", "GET", "/bkt/streamed", nil, nil)
	want(t, resp, b, 200)
	if !bytes.Equal(b, data) {
		t.Fatal("chunked upload stored wrong bytes")
	}
}

func TestS3ListingCopyAndBulkDelete(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2)
	c.s3("n1", "PUT", "/lst", nil, nil)
	keys := []string{"a.txt", "docs/a", "docs/b", "docs/sub/c", "img/1.png", "img/2.png", "z.txt"}
	for _, k := range keys {
		resp, b := c.s3("n1", "PUT", "/lst/"+k, []byte("v-"+k), nil)
		want(t, resp, b, 200)
	}

	list := func(q string) s3List {
		t.Helper()
		resp, b := c.s3("n2", "GET", "/lst?"+q, nil, nil)
		want(t, resp, b, 200)
		var out s3List
		if err := xml.Unmarshal(b, &out); err != nil {
			t.Fatalf("%v: %s", err, b)
		}
		return out
	}

	all := list("list-type=2")
	if len(all.Contents) != len(keys) || all.IsTruncated {
		t.Fatalf("full listing: %+v", all)
	}

	d := list("list-type=2&delimiter=/")
	var cps []string
	for _, p := range d.CommonPrefixes {
		cps = append(cps, p.Prefix)
	}
	if strings.Join(cps, ",") != "docs/,img/" || len(d.Contents) != 2 || d.KeyCount != 4 {
		t.Fatalf("delimiter listing: prefixes=%v contents=%v count=%d", cps, d.Contents, d.KeyCount)
	}

	d = list("list-type=2&delimiter=/&prefix=docs/")
	if len(d.Contents) != 2 || len(d.CommonPrefixes) != 1 || d.CommonPrefixes[0].Prefix != "docs/sub/" {
		t.Fatalf("prefix listing: %+v", d)
	}

	// two per page with a delimiter: nothing may repeat or go missing across pages
	seen := map[string]bool{}
	token := ""
	for pages := 0; pages < 20; pages++ {
		q := "list-type=2&delimiter=/&max-keys=2"
		if token != "" {
			q += "&continuation-token=" + url.QueryEscape(token)
		}
		pg := list(q)
		for _, e := range pg.Contents {
			if seen[e.Key] {
				t.Fatalf("duplicate %s", e.Key)
			}
			seen[e.Key] = true
		}
		for _, e := range pg.CommonPrefixes {
			if seen[e.Prefix] {
				t.Fatalf("duplicate %s", e.Prefix)
			}
			seen[e.Prefix] = true
		}
		if !pg.IsTruncated {
			break
		}
		token = pg.NextContinuationToken
	}
	if len(seen) != 4 {
		t.Fatalf("paged listing saw %v", seen)
	}

	resp, b := c.s3("n3", "PUT", "/lst/copy-of-z", nil, map[string]string{"X-Amz-Copy-Source": "/lst/z.txt"})
	want(t, resp, b, 200)
	resp, b = c.s3("n1", "GET", "/lst/copy-of-z", nil, nil)
	want(t, resp, b, 200)
	if string(b) != "v-z.txt" {
		t.Fatalf("copy body %q", b)
	}

	del := `<Delete><Object><Key>a.txt</Key></Object><Object><Key>z.txt</Key></Object><Object><Key>nope</Key></Object></Delete>`
	resp, b = c.s3("n1", "POST", "/lst?delete", []byte(del), nil)
	want(t, resp, b, 200)
	if strings.Count(string(b), "<Deleted>") != 3 {
		t.Fatalf("delete result: %s", b)
	}
	resp, _ = c.s3("n1", "HEAD", "/lst/a.txt", nil, nil)
	if resp.StatusCode != 404 {
		t.Fatalf("a.txt survived bulk delete: %d", resp.StatusCode)
	}
}

func TestS3Multipart(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2)
	c.s3("n1", "PUT", "/mpu", nil, nil)

	resp, b := c.s3("n1", "POST", "/mpu/big.bin?uploads", nil, map[string]string{"Content-Type": "application/x-big"})
	want(t, resp, b, 200)
	var init struct{ UploadId string }
	if err := xml.Unmarshal(b, &init); err != nil || init.UploadId == "" {
		t.Fatalf("initiate: %v %s", err, b)
	}

	p1, p2, p3 := randBytes(70_000), randBytes(70_000), randBytes(5_000)
	etags := map[int]string{}
	nums := []int{3, 1, 2} // out of order on purpose
	for i, p := range [][]byte{p3, p1, p2} {
		resp, b = c.s3(fmt.Sprintf("n%d", i+1), "PUT", fmt.Sprintf("/mpu/big.bin?partNumber=%d&uploadId=%s", nums[i], init.UploadId), p, nil)
		want(t, resp, b, 200)
		etags[nums[i]] = resp.Header.Get("ETag")
	}

	resp, b = c.s3("n2", "GET", "/mpu/big.bin?uploadId="+init.UploadId, nil, nil)
	want(t, resp, b, 200)
	if strings.Count(string(b), "<Part>") != 3 {
		t.Fatalf("list parts: %s", b)
	}

	var cx strings.Builder
	cx.WriteString("<CompleteMultipartUpload>")
	for n := 1; n <= 3; n++ {
		fmt.Fprintf(&cx, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", n, etags[n])
	}
	cx.WriteString("</CompleteMultipartUpload>")
	resp, b = c.s3("n3", "POST", "/mpu/big.bin?uploadId="+init.UploadId, []byte(cx.String()), nil)
	want(t, resp, b, 200)

	whole := append(append(append([]byte{}, p1...), p2...), p3...)
	resp, b = c.s3("n1", "GET", "/mpu/big.bin", nil, nil)
	want(t, resp, b, 200)
	if !bytes.Equal(b, whole) || resp.Header.Get("Content-Type") != "application/x-big" {
		t.Fatalf("assembled object wrong (%d bytes, ct=%q)", len(b), resp.Header.Get("Content-Type"))
	}
	resp, b = c.s3("n2", "GET", "/mpu/big.bin", nil, map[string]string{"Range": "bytes=69990-70010"})
	want(t, resp, b, 206)
	if !bytes.Equal(b, whole[69990:70011]) {
		t.Fatal("range across a part boundary is wrong")
	}

	resp, b = c.s3("n1", "POST", "/mpu/gone?uploads", nil, nil)
	want(t, resp, b, 200)
	var init2 struct{ UploadId string }
	xml.Unmarshal(b, &init2)
	resp, b = c.s3("n1", "DELETE", "/mpu/gone?uploadId="+init2.UploadId, nil, nil)
	want(t, resp, b, 204)
	resp, b = c.s3("n1", "PUT", "/mpu/gone?partNumber=1&uploadId="+init2.UploadId, []byte("x"), nil)
	if resp.StatusCode != 404 || !strings.Contains(string(b), "NoSuchUpload") {
		t.Fatalf("part after abort: %d %s", resp.StatusCode, b)
	}
}
