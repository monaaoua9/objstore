package sigv4

import (
	"bytes"
	"io"
	"testing"
)

func FuzzDecodeChunked(f *testing.F) {
	f.Add([]byte("5\r\nhello\r\n0\r\n\r\n"), false)
	f.Add([]byte("fffffffffffffff\r\n"), false)
	f.Add(EncodeChunked([]byte("some body"), 4, &Auth{PayloadHash: StreamingSigned, key: []byte("k"), Scope: "20240101/us-east-1/s3/aws4_request", AmzDate: "20240101T000000Z", Signature: "00"}), true)
	f.Fuzz(func(t *testing.T, data []byte, signed bool) {
		a := &Auth{PayloadHash: StreamingTrailer, Signature: "00"}
		if signed {
			a = &Auth{PayloadHash: StreamingSigned, key: []byte("k"), Scope: "20240101/us-east-1/s3/aws4_request", AmzDate: "20240101T000000Z", Signature: "00"}
		}
		// must terminate, must not panic, must not allocate more than a chunk per read
		n, _ := io.Copy(io.Discard, io.LimitReader(DecodeChunked(bytes.NewReader(data), a), 1<<30))
		if n > int64(len(data)) {
			t.Fatalf("decoded %d bytes out of %d", n, len(data))
		}
	})
}
