// Package frame defines the on-disk and on-wire shard framing: one frame per stripe,
// each carrying its own SHA-256 so corruption is caught at the first bad stripe rather
// than after a whole object has been streamed.
package frame

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	HeaderSize = 4 + sha256.Size
	MaxPayload = 64 << 20
)

var ErrCorrupt = errors.New("frame: checksum mismatch")

// Stride is the byte distance between consecutive full frames.
func Stride(shardSize int) int64 { return int64(HeaderSize + shardSize) }

func Write(w io.Writer, payload []byte) error {
	var h [HeaderSize]byte
	binary.LittleEndian.PutUint32(h[:4], uint32(len(payload)))
	sum := sha256.Sum256(payload)
	copy(h[4:], sum[:])
	if _, err := w.Write(h[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// Reader pulls verified frames off a stream.
type Reader struct {
	r   io.Reader
	hdr [HeaderSize]byte
	n   int // frames read so far, for error messages
}

func NewReader(r io.Reader) *Reader { return &Reader{r: r} }

// Next returns the next payload, reusing buf's backing array when it fits.
// io.EOF means the stream ended cleanly on a frame boundary.
func (fr *Reader) Next(buf []byte) ([]byte, error) {
	if _, err := io.ReadFull(fr.r, fr.hdr[:]); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("frame %d header: %w", fr.n, noEOF(err))
	}
	n := int(binary.LittleEndian.Uint32(fr.hdr[:4]))
	if n > MaxPayload {
		return nil, fmt.Errorf("frame %d: length %d: %w", fr.n, n, ErrCorrupt)
	}
	if cap(buf) < n {
		buf = make([]byte, n)
	}
	buf = buf[:n]
	if _, err := io.ReadFull(fr.r, buf); err != nil {
		return nil, fmt.Errorf("frame %d body: %w", fr.n, noEOF(err))
	}
	if sha256.Sum256(buf) != [sha256.Size]byte(fr.hdr[4:]) {
		return nil, fmt.Errorf("frame %d: %w", fr.n, ErrCorrupt)
	}
	fr.n++
	return buf, nil
}

func (fr *Reader) Count() int { return fr.n }

func noEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
