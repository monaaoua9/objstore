// Package httpx builds the HTTP/2 client and server every component shares.
// Without TLS we run h2c with prior knowledge, which is what makes streaming bodies cheap
// (one multiplexed connection per peer, no head-of-line blocking between shard streams).
package httpx

import (
	"crypto/subtle"
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"time"
)

func NewClient(tlsCfg *tls.Config) *http.Client {
	p := new(http.Protocols)
	t := &http.Transport{
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
		DialContext:         (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 15 * time.Second}).DialContext,
		TLSHandshakeTimeout: 5 * time.Second,
		TLSClientConfig:     tlsCfg,
		// pings let us notice a blackholed peer instead of waiting on a stream forever
		HTTP2: &http.HTTP2Config{MaxReadFrameSize: 1 << 20, SendPingTimeout: 15 * time.Second, PingTimeout: 10 * time.Second},
	}
	if tlsCfg == nil {
		p.SetUnencryptedHTTP2(true)
	} else {
		p.SetHTTP2(true)
	}
	t.Protocols = p
	return &http.Client{Transport: t}
}

func NewServer(addr string, h http.Handler, tlsCfg *tls.Config) *http.Server {
	p := new(http.Protocols)
	p.SetHTTP1(true)
	if tlsCfg == nil {
		p.SetUnencryptedHTTP2(true)
	} else {
		p.SetHTTP2(true)
	}
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		TLSConfig:         tlsCfg,
		Protocols:         p,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		HTTP2:             &http.HTTP2Config{MaxReadFrameSize: 1 << 20, MaxConcurrentStreams: 1024},
	}
}

// RequireSecret guards intra-cluster endpoints with a shared bearer token. Empty secret = open.
func RequireSecret(secret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if secret == "" {
			return next
		}
		want := []byte("Bearer " + secret)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got := []byte(strings.TrimSpace(r.Header.Get("Authorization")))
			if subtle.ConstantTimeCompare(got, want) != 1 {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
