package blobstore

import (
	"io"
	"sync"
	"time"
)

// Limiter is a byte-rate throttle so scrubbing doesn't starve foreground traffic.
// Zero rate means unlimited.
type Limiter struct {
	mu    sync.Mutex
	rate  float64 // bytes/sec
	next  time.Time
	burst time.Duration
}

func NewLimiter(bytesPerSec int64) *Limiter {
	if bytesPerSec <= 0 {
		return nil
	}
	return &Limiter{rate: float64(bytesPerSec), burst: 50 * time.Millisecond}
}

func (l *Limiter) wait(n int) {
	l.mu.Lock()
	now := time.Now()
	if l.next.Before(now.Add(-l.burst)) {
		l.next = now.Add(-l.burst)
	}
	l.next = l.next.Add(time.Duration(float64(n) / l.rate * float64(time.Second)))
	d := l.next.Sub(now)
	l.mu.Unlock()
	if d > 0 {
		time.Sleep(d)
	}
}

type limited struct {
	r io.Reader
	l *Limiter
}

func (l *Limiter) Reader(r io.Reader) io.Reader { return &limited{r, l} }

func (lr *limited) Read(p []byte) (int, error) {
	if len(p) > 256<<10 {
		p = p[:256<<10]
	}
	n, err := lr.r.Read(p)
	if n > 0 {
		lr.l.wait(n)
	}
	return n, err
}
