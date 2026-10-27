// Package sigv4 verifies (and produces) AWS Signature Version 4 requests: Authorization-header
// signing, presigned URLs, and the aws-chunked streaming payload formats.
package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	Algorithm        = "AWS4-HMAC-SHA256"
	UnsignedPayload  = "UNSIGNED-PAYLOAD"
	StreamingSigned  = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	StreamingTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	maxSkew          = 15 * time.Minute
	timeFmt          = "20060102T150405Z"
)

var (
	ErrNoAuth        = errors.New("missing authentication")
	ErrBadAuth       = errors.New("malformed authorization")
	ErrUnknownKey    = errors.New("unknown access key")
	ErrBadSignature  = errors.New("signature does not match")
	ErrExpired       = errors.New("request expired or clock skew too large")
	ErrPayloadDigest = errors.New("payload sha256 does not match x-amz-content-sha256")
)

// Credentials maps access key -> secret.
type Credentials map[string]string

// Auth is the outcome of a successful verification; the streaming decoders need it.
type Auth struct {
	AccessKey   string
	Region      string
	Service     string
	AmzDate     string // 20060102T150405Z
	Scope       string // date/region/service/aws_request
	PayloadHash string // value of x-amz-content-sha256 (or UNSIGNED-PAYLOAD)
	Signature   string
	Presigned   bool
	key         []byte
}

func hmacSHA(key []byte, msg string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(msg))
	return h.Sum(nil)
}

func signingKey(secret, date, region, service string) []byte {
	k := hmacSHA([]byte("AWS4"+secret), date)
	k = hmacSHA(k, region)
	k = hmacSHA(k, service)
	return hmacSHA(k, "aws4_request")
}

func hexSHA(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// uriEncode is AWS's flavour of percent-encoding: everything but unreserved, optionally keeping '/'.
func uriEncode(s string, keepSlash bool) string {
	const hexd = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && keepSlash:
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexd[c>>4])
			b.WriteByte(hexd[c&15])
		}
	}
	return b.String()
}

func canonicalURI(r *http.Request) string {
	p := r.URL.EscapedPath()
	if p == "" {
		return "/"
	}
	dec, err := url.PathUnescape(p)
	if err != nil {
		dec = p
	}
	return uriEncode(dec, true)
}

func canonicalQuery(q url.Values, skipSig bool) string {
	type kv struct{ k, v string }
	var all []kv
	for k, vs := range q {
		if skipSig && k == "X-Amz-Signature" {
			continue
		}
		for _, v := range vs {
			all = append(all, kv{uriEncode(k, false), uriEncode(v, false)})
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].k != all[j].k {
			return all[i].k < all[j].k
		}
		return all[i].v < all[j].v
	})
	parts := make([]string, len(all))
	for i, p := range all {
		parts[i] = p.k + "=" + p.v
	}
	return strings.Join(parts, "&")
}

func canonicalHeaders(r *http.Request, signed []string) string {
	var b strings.Builder
	for _, name := range signed {
		var val string
		if name == "host" {
			val = r.Host
		} else {
			vs := r.Header.Values(http.CanonicalHeaderKey(name))
			for i := range vs {
				vs[i] = strings.Join(strings.Fields(vs[i]), " ")
			}
			val = strings.Join(vs, ",")
		}
		b.WriteString(name + ":" + val + "\n")
	}
	return b.String()
}

func canonicalRequest(r *http.Request, signed []string, payloadHash string, presigned bool) string {
	return strings.Join([]string{
		r.Method,
		canonicalURI(r),
		canonicalQuery(r.URL.Query(), presigned),
		canonicalHeaders(r, signed),
		strings.Join(signed, ";"),
		payloadHash,
	}, "\n")
}

func stringToSign(amzDate, scope, canonical string) string {
	return Algorithm + "\n" + amzDate + "\n" + scope + "\n" + hexSHA(canonical)
}

// Verify authenticates r. creds lookup failures and signature mismatches are distinguishable for error codes.
func Verify(r *http.Request, creds Credentials, now time.Time) (*Auth, error) {
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") != "" {
		return verifyPresigned(r, q, creds, now)
	}
	h := r.Header.Get("Authorization")
	if h == "" {
		return nil, ErrNoAuth
	}
	rest, ok := strings.CutPrefix(h, Algorithm+" ")
	if !ok {
		return nil, ErrBadAuth
	}
	fields := map[string]string{}
	for _, p := range strings.Split(rest, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(p), "=")
		if !ok {
			return nil, ErrBadAuth
		}
		fields[k] = v
	}
	cred, signedStr, sig := fields["Credential"], fields["SignedHeaders"], fields["Signature"]
	if cred == "" || signedStr == "" || sig == "" {
		return nil, ErrBadAuth
	}
	a, err := parseCredential(cred, creds)
	if err != nil {
		return nil, err
	}
	a.AmzDate = r.Header.Get("X-Amz-Date")
	if a.AmzDate == "" {
		if d := r.Header.Get("Date"); d != "" {
			if t, err := http.ParseTime(d); err == nil {
				a.AmzDate = t.UTC().Format(timeFmt)
			}
		}
	}
	if err := checkTime(a.AmzDate, now); err != nil {
		return nil, err
	}
	if !strings.HasPrefix(a.AmzDate, a.Scope[:8]) {
		return nil, ErrBadAuth
	}
	a.PayloadHash = r.Header.Get("X-Amz-Content-Sha256")
	if a.PayloadHash == "" {
		a.PayloadHash = UnsignedPayload
	}
	signed := strings.Split(signedStr, ";")
	canon := canonicalRequest(r, signed, a.PayloadHash, false)
	want := hex.EncodeToString(hmacSHA(a.key, stringToSign(a.AmzDate, a.Scope, canon)))
	if !hmac.Equal([]byte(want), []byte(sig)) {
		return nil, ErrBadSignature
	}
	a.Signature = sig
	return a, nil
}

func verifyPresigned(r *http.Request, q url.Values, creds Credentials, now time.Time) (*Auth, error) {
	if q.Get("X-Amz-Algorithm") != Algorithm {
		return nil, ErrBadAuth
	}
	a, err := parseCredential(q.Get("X-Amz-Credential"), creds)
	if err != nil {
		return nil, err
	}
	a.AmzDate = q.Get("X-Amz-Date")
	exp, err := strconv.Atoi(q.Get("X-Amz-Expires"))
	if err != nil || exp < 1 || exp > 7*24*3600 {
		return nil, ErrBadAuth
	}
	t, err := time.Parse(timeFmt, a.AmzDate)
	if err != nil || now.Before(t.Add(-maxSkew)) || now.After(t.Add(time.Duration(exp)*time.Second)) {
		return nil, ErrExpired
	}
	signed := strings.Split(q.Get("X-Amz-SignedHeaders"), ";")
	a.PayloadHash = UnsignedPayload
	a.Presigned = true
	canon := canonicalRequest(r, signed, UnsignedPayload, true)
	want := hex.EncodeToString(hmacSHA(a.key, stringToSign(a.AmzDate, a.Scope, canon)))
	if !hmac.Equal([]byte(want), []byte(q.Get("X-Amz-Signature"))) {
		return nil, ErrBadSignature
	}
	return a, nil
}

func parseCredential(cred string, creds Credentials) (*Auth, error) {
	// AK/20240101/region/service/aws4_request
	parts := strings.Split(cred, "/")
	if len(parts) != 5 || parts[4] != "aws4_request" || len(parts[1]) != 8 {
		return nil, ErrBadAuth
	}
	secret, ok := creds[parts[0]]
	if !ok {
		return nil, ErrUnknownKey
	}
	return &Auth{
		AccessKey: parts[0], Region: parts[2], Service: parts[3],
		Scope: strings.Join(parts[1:], "/"),
		key:   signingKey(secret, parts[1], parts[2], parts[3]),
	}, nil
}

func checkTime(amzDate string, now time.Time) error {
	t, err := time.Parse(timeFmt, amzDate)
	if err != nil {
		return ErrBadAuth
	}
	if d := now.Sub(t); d > maxSkew || d < -maxSkew {
		return ErrExpired
	}
	return nil
}

// Sign adds an Authorization header to req (header-style SigV4). payloadHash is the hex SHA-256 of the
// body, or UnsignedPayload / StreamingSigned etc. It sets Host, X-Amz-Date and X-Amz-Content-Sha256.
func Sign(req *http.Request, accessKey, secret, region, service, payloadHash string, now time.Time) (*Auth, error) {
	amz := now.UTC().Format(timeFmt)
	date := amz[:8]
	req.Header.Set("X-Amz-Date", amz)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if req.Host == "" {
		req.Host = req.URL.Host
	}

	names := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	for k := range req.Header {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-amz-") && lk != "x-amz-content-sha256" && lk != "x-amz-date" {
			names = append(names, lk)
		}
	}
	if req.Header.Get("Content-Type") != "" {
		names = append(names, "content-type")
	}
	sort.Strings(names)

	scope := fmt.Sprintf("%s/%s/%s/aws4_request", date, region, service)
	canon := canonicalRequest(req, names, payloadHash, false)
	key := signingKey(secret, date, region, service)
	sig := hex.EncodeToString(hmacSHA(key, stringToSign(amz, scope, canon)))
	req.Header.Set("Authorization", fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		Algorithm, accessKey, scope, strings.Join(names, ";"), sig))
	return &Auth{AccessKey: accessKey, Region: region, Service: service, AmzDate: amz, Scope: scope, PayloadHash: payloadHash, Signature: sig, key: key}, nil
}

// Presign returns a URL valid for ttl that grants whatever method req carries.
func Presign(req *http.Request, accessKey, secret, region string, ttl time.Duration, now time.Time) string {
	amz := now.UTC().Format(timeFmt)
	scope := fmt.Sprintf("%s/%s/s3/aws4_request", amz[:8], region)
	q := req.URL.Query()
	q.Set("X-Amz-Algorithm", Algorithm)
	q.Set("X-Amz-Credential", accessKey+"/"+scope)
	q.Set("X-Amz-Date", amz)
	q.Set("X-Amz-Expires", strconv.Itoa(int(ttl.Seconds())))
	q.Set("X-Amz-SignedHeaders", "host")
	req.URL.RawQuery = q.Encode()
	if req.Host == "" {
		req.Host = req.URL.Host
	}
	canon := canonicalRequest(req, []string{"host"}, UnsignedPayload, true)
	key := signingKey(secret, amz[:8], region, "s3")
	q.Set("X-Amz-Signature", hex.EncodeToString(hmacSHA(key, stringToSign(amz, scope, canon))))
	req.URL.RawQuery = q.Encode()
	return req.URL.String()
}
