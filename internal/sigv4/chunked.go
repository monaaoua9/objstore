package sigv4

import (
	"bufio"
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var ErrChunk = errors.New("sigv4: malformed or unauthentic aws-chunked body")

const maxChunk = 16 << 20

// DecodeChunked unwraps an aws-chunked body. For StreamingSigned every chunk signature is checked
// against the chain that starts at the request signature; for StreamingTrailer there are no
// signatures (the trailer is consumed and ignored).
func DecodeChunked(body io.Reader, a *Auth) io.Reader {
	return &chunkReader{r: bufio.NewReaderSize(body, 64<<10), a: a, signed: a.PayloadHash == StreamingSigned, prev: a.Signature}
}

type chunkReader struct {
	r      *bufio.Reader
	a      *Auth
	signed bool
	prev   string
	buf    []byte
	done   bool
	err    error
}

func (c *chunkReader) Read(p []byte) (int, error) {
	for len(c.buf) == 0 {
		if c.err != nil {
			return 0, c.err
		}
		if c.done {
			return 0, io.EOF
		}
		c.err = c.next()
	}
	n := copy(p, c.buf)
	c.buf = c.buf[n:]
	return n, nil
}

func (c *chunkReader) next() error {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("%w: header: %v", ErrChunk, err)
	}
	line = strings.TrimRight(line, "\r\n")
	sizeStr, ext, _ := strings.Cut(line, ";")
	size, err := strconv.ParseInt(sizeStr, 16, 64)
	if err != nil || size < 0 || size > maxChunk {
		return ErrChunk
	}
	var gotSig string
	if c.signed {
		s, ok := strings.CutPrefix(ext, "chunk-signature=")
		if !ok {
			return ErrChunk
		}
		gotSig = s
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(c.r, data); err != nil {
		return fmt.Errorf("%w: body: %v", ErrChunk, err)
	}
	if c.signed {
		empty := sha256.Sum256(nil)
		sum := sha256.Sum256(data)
		sts := strings.Join([]string{Algorithm + "-PAYLOAD", c.a.AmzDate, c.a.Scope, c.prev, hex.EncodeToString(empty[:]), hex.EncodeToString(sum[:])}, "\n")
		want := hex.EncodeToString(hmacSHA(c.a.key, sts))
		if !hmac.Equal([]byte(want), []byte(gotSig)) {
			return ErrBadSignature
		}
		c.prev = want
	}
	if size == 0 {
		c.done = true
		io.Copy(io.Discard, c.r) // trailing checksums / final CRLF
		return nil
	}
	if crlf, err := c.r.Peek(2); err == nil && bytes.Equal(crlf, []byte("\r\n")) {
		c.r.Discard(2)
	} else {
		return ErrChunk
	}
	c.buf = data
	return nil
}

// HashVerifier checks the body against a declared hex SHA-256 as it streams; the mismatch surfaces at EOF.
func HashVerifier(r io.Reader, hexSum string) io.Reader {
	return &verifyReader{r: r, h: sha256.New(), want: strings.ToLower(hexSum)}
}

type verifyReader struct {
	r io.Reader
	h interface {
		Write([]byte) (int, error)
		Sum([]byte) []byte
	}
	want string
}

func (v *verifyReader) Read(p []byte) (int, error) {
	n, err := v.r.Read(p)
	v.h.Write(p[:n])
	if err == io.EOF && hex.EncodeToString(v.h.Sum(nil)) != v.want {
		return n, ErrPayloadDigest
	}
	return n, err
}

// EncodeChunked produces an aws-chunked signed body (used by tests and any Go client of this server).
func EncodeChunked(data []byte, chunk int, a *Auth) []byte {
	var out bytes.Buffer
	prev := a.Signature
	empty := sha256.Sum256(nil)
	emit := func(d []byte) {
		sum := sha256.Sum256(d)
		sts := strings.Join([]string{Algorithm + "-PAYLOAD", a.AmzDate, a.Scope, prev, hex.EncodeToString(empty[:]), hex.EncodeToString(sum[:])}, "\n")
		sig := hex.EncodeToString(hmacSHA(a.key, sts))
		prev = sig
		fmt.Fprintf(&out, "%x;chunk-signature=%s\r\n", len(d), sig)
		out.Write(d)
		out.WriteString("\r\n")
	}
	for len(data) > 0 {
		n := min(chunk, len(data))
		emit(data[:n])
		data = data[n:]
	}
	emit(nil)
	return out.Bytes()
}
