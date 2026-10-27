package app

import (
	"strings"
	"testing"

	"github.com/monaaoua9/objstore/internal/s3api"
)

func denied(t *testing.T, status int, body []byte, what string) {
	t.Helper()
	if status != 403 || !strings.Contains(string(body), "AccessDenied") {
		t.Fatalf("%s: got %d %s", what, status, body)
	}
}

func TestS3KeyPolicies(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2)
	for _, b := range []string{"mine", "shared", "other"} {
		resp, body := c.s3("n1", "PUT", "/"+b, nil, nil)
		want(t, resp, body, 200)
	}
	c.s3("n1", "PUT", "/shared/pub.txt", []byte("public"), nil)
	c.s3("n1", "PUT", "/other/secret.txt", []byte("secret"), nil)

	ro := func(method, path string, body []byte) (int, []byte) {
		resp, b := c.s3as(roAK, "readonly-secret", "n2", method, path, body, nil, false)
		return resp.StatusCode, b
	}
	sc := func(method, path string, body []byte, hdr map[string]string) (int, []byte) {
		resp, b := c.s3as(scopedAK, "scoped-secret", "n3", method, path, body, hdr, false)
		return resp.StatusCode, b
	}

	// read-only: reads everywhere, writes nowhere
	if st, b := ro("GET", "/shared/pub.txt", nil); st != 200 || string(b) != "public" {
		t.Fatalf("ro read: %d %s", st, b)
	}
	st, b := ro("PUT", "/shared/x", []byte("nope"))
	denied(t, st, b, "ro put")
	st, b = ro("DELETE", "/shared/pub.txt", nil)
	denied(t, st, b, "ro delete")
	st, b = ro("POST", "/shared?delete", []byte(`<Delete><Object><Key>pub.txt</Key></Object></Delete>`))
	denied(t, st, b, "ro bulk delete")
	st, b = ro("PUT", "/brandnew", nil)
	denied(t, st, b, "ro create bucket")

	// scoped: full rights on its two buckets, nothing elsewhere
	if st, b := sc("PUT", "/mine/a.txt", []byte("mine"), nil); st != 200 {
		t.Fatalf("scoped write to own bucket: %d %s", st, b)
	}
	st, b = sc("GET", "/other/secret.txt", nil, nil)
	denied(t, st, b, "scoped read of foreign bucket")
	st, b = sc("PUT", "/other/x", []byte("x"), nil)
	denied(t, st, b, "scoped write to foreign bucket")
	st, b = sc("GET", "/other?list-type=2", nil, nil)
	denied(t, st, b, "scoped list of foreign bucket")

	st, b = sc("GET", "/", nil, nil)
	if st != 200 || !strings.Contains(string(b), "<Name>mine</Name>") || !strings.Contains(string(b), "<Name>shared</Name>") || strings.Contains(string(b), "<Name>other</Name>") {
		t.Fatalf("bucket list leaks or hides buckets: %d %s", st, b)
	}

	st, b = sc("PUT", "/mine/stolen.txt", nil, map[string]string{"X-Amz-Copy-Source": "/other/secret.txt"})
	denied(t, st, b, "copy out of a foreign bucket")
	if st, b := sc("PUT", "/mine/copied.txt", nil, map[string]string{"X-Amz-Copy-Source": "/shared/pub.txt"}); st != 200 {
		t.Fatalf("copy from an allowed bucket: %d %s", st, b)
	}
	if resp, body := c.s3("n1", "GET", "/other/stolen.txt", nil, nil); resp.StatusCode != 404 {
		t.Fatalf("stolen object exists: %d %s", resp.StatusCode, body)
	}

	// revoking a key takes effect without a restart
	for _, n := range c.nodes {
		n.SetS3Keys(map[string]s3api.Key{testAK: {Secret: testSK}})
	}
	st, b = sc("GET", "/mine/a.txt", nil, nil)
	if st != 403 || !strings.Contains(string(b), "InvalidAccessKeyId") {
		t.Fatalf("revoked key still works: %d %s", st, b)
	}
}
