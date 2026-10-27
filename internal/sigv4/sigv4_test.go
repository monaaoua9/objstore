package sigv4

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// The worked example from AWS's own SigV4 documentation (iam ListUsers); the signature below is the
// one AWS publishes, so this pins our canonicalisation to the real thing, not to our own signer.
func TestAWSPublishedVector(t *testing.T) {
	r, _ := http.NewRequest("GET", "https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08", nil)
	r.Host = "iam.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	r.Header.Set("X-Amz-Date", "20150830T123600Z")
	emptyHash := sha256.Sum256(nil)
	r.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(emptyHash[:]))
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, "+
		"SignedHeaders=content-type;host;x-amz-date, "+
		"Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7")
	// the published example omits x-amz-content-sha256 from SignedHeaders; it still hashes the empty body
	creds := Credentials{"AKIDEXAMPLE": "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"}
	now := time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)
	if _, err := Verify(r, creds, now); err != nil {
		t.Fatalf("published AWS signature rejected: %v", err)
	}
}

func signed(t *testing.T, method, url string, body []byte, hdr map[string]string) (*http.Request, *Auth) {
	t.Helper()
	r, _ := http.NewRequest(method, url, bytes.NewReader(body))
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	sum := sha256.Sum256(body)
	a, err := Sign(r, "AK", "SECRET", "eu-west-1", "s3", hex.EncodeToString(sum[:]), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return r, a
}

var testCreds = Credentials{"AK": "SECRET"}

func TestRoundTripAndTamper(t *testing.T) {
	r, _ := signed(t, "PUT", "http://localhost:9100/bucket/some%20dir/key+plus.txt?uploadId=abc&partNumber=2", []byte("hello"),
		map[string]string{"Content-Type": "text/plain", "X-Amz-Meta-Colour": "blue  green"})
	if _, err := Verify(r, testCreds, time.Now()); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}

	cases := map[string]func(*http.Request){
		"path":   func(r *http.Request) { r.URL.Path = "/bucket/other" },
		"query":  func(r *http.Request) { r.URL.RawQuery = "uploadId=abc&partNumber=3" },
		"method": func(r *http.Request) { r.Method = "DELETE" },
		"header": func(r *http.Request) { r.Header.Set("X-Amz-Meta-Colour", "red") },
		"host":   func(r *http.Request) { r.Host = "evil.example" },
	}
	for name, mutate := range cases {
		r2, _ := signed(t, "PUT", "http://localhost:9100/bucket/some%20dir/key+plus.txt?uploadId=abc&partNumber=2", []byte("hello"),
			map[string]string{"Content-Type": "text/plain", "X-Amz-Meta-Colour": "blue  green"})
		mutate(r2)
		if _, err := Verify(r2, testCreds, time.Now()); !errors.Is(err, ErrBadSignature) {
			t.Errorf("%s tamper: got %v", name, err)
		}
	}

	if _, err := Verify(r, Credentials{"AK": "WRONG"}, time.Now()); !errors.Is(err, ErrBadSignature) {
		t.Errorf("wrong secret: %v", err)
	}
	if _, err := Verify(r, Credentials{"OTHER": "x"}, time.Now()); !errors.Is(err, ErrUnknownKey) {
		t.Errorf("unknown key: %v", err)
	}
	if _, err := Verify(r, testCreds, time.Now().Add(time.Hour)); !errors.Is(err, ErrExpired) {
		t.Errorf("skew: %v", err)
	}
	r3, _ := http.NewRequest("GET", "http://x/", nil)
	if _, err := Verify(r3, testCreds, time.Now()); !errors.Is(err, ErrNoAuth) {
		t.Errorf("anonymous: %v", err)
	}
}

func TestPresigned(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost:9100/bucket/key", nil)
	u := Presign(req, "AK", "SECRET", "us-east-1", time.Minute, time.Now())
	r, _ := http.NewRequest("GET", u, nil)
	a, err := Verify(r, testCreds, time.Now())
	if err != nil || !a.Presigned {
		t.Fatalf("presigned rejected: %v", err)
	}
	if _, err := Verify(r, testCreds, time.Now().Add(2*time.Minute)); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired presign accepted: %v", err)
	}
	r2, _ := http.NewRequest("GET", strings.Replace(u, "/key", "/other", 1), nil)
	if _, err := Verify(r2, testCreds, time.Now()); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("presign reused for another key: %v", err)
	}
}

func TestChunkedBody(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789abcdef"), 5000) // 80 KB
	r, _ := http.NewRequest("PUT", "http://localhost:9100/b/k", nil)
	a, _ := Sign(r, "AK", "SECRET", "us-east-1", "s3", StreamingSigned, time.Now())

	enc := EncodeChunked(data, 16<<10, a)
	got, err := io.ReadAll(DecodeChunked(bytes.NewReader(enc), a))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("chunked round trip: %v (%d bytes)", err, len(got))
	}

	bad := append([]byte(nil), enc...)
	bad[len(bad)/2] ^= 1 // flips a payload byte somewhere in the middle
	if _, err := io.ReadAll(DecodeChunked(bytes.NewReader(bad), a)); err == nil {
		t.Fatal("tampered chunk accepted")
	}
	// dropping the final zero-length chunk (truncation attack) must not look like a clean end
	cut := enc[:bytes.LastIndex(enc, []byte("0;chunk-signature"))]
	if _, err := io.ReadAll(DecodeChunked(bytes.NewReader(cut), a)); err == nil {
		t.Fatal("truncated stream accepted")
	}
}

func TestUnsignedTrailerBody(t *testing.T) {
	body := "5\r\nhello\r\n6\r\n world\r\n0\r\nx-amz-checksum-crc32:abcd\r\n\r\n"
	r, _ := http.NewRequest("PUT", "http://x/b/k", nil)
	a, _ := Sign(r, "AK", "SECRET", "us-east-1", "s3", StreamingTrailer, time.Now())
	got, err := io.ReadAll(DecodeChunked(strings.NewReader(body), a))
	if err != nil || string(got) != "hello world" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestHashVerifier(t *testing.T) {
	sum := sha256.Sum256([]byte("abc"))
	if _, err := io.ReadAll(HashVerifier(strings.NewReader("abc"), hex.EncodeToString(sum[:]))); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(HashVerifier(strings.NewReader("abd"), hex.EncodeToString(sum[:]))); !errors.Is(err, ErrPayloadDigest) {
		t.Fatalf("got %v", err)
	}
}
