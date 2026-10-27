package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/monaaoua9/objstore/internal/raft"
)

// Backend is the transport-agnostic way to talk to the metadata group: voters use the raft-backed
// one directly, storage-only nodes go through the HTTP one.
type Backend interface {
	Apply(ctx context.Context, cmd []byte) ([]byte, error)
	Query(ctx context.Context, q []byte) ([]byte, error)
}

type staleKey struct{}

// WithStale lets a read skip the leader round-trip. Only for things that tolerate lag (dashboards, repair planning).
func WithStale(ctx context.Context) context.Context { return context.WithValue(ctx, staleKey{}, true) }

type RaftBackend struct {
	Node *raft.Node
	FSM  *FSM
}

func (b *RaftBackend) Apply(ctx context.Context, c []byte) ([]byte, error) {
	return b.Node.Propose(ctx, c)
}

func (b *RaftBackend) Query(ctx context.Context, q []byte) ([]byte, error) {
	if ctx.Value(staleKey{}) == nil {
		if err := b.retryBarrier(ctx); err != nil {
			return nil, err
		}
	}
	return b.FSM.Query(q), nil
}

func (b *RaftBackend) retryBarrier(ctx context.Context) error {
	for {
		err := b.Node.ReadBarrier(ctx)
		if err == nil || !(errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrNoLeader)) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// HTTPBackend forwards to any voter. Voters forward to the leader internally, so we just
// rotate through the list until one answers.
type HTTPBackend struct {
	Client *http.Client
	Scheme string
	Addrs  []string
	Secret string
	// Seeds is the id -> address list the node was configured with; Refresh uses it for voters
	// whose address the metadata group itself doesn't carry (bootstrap voters).
	Seeds map[string]string

	mu   sync.RWMutex
	pref atomic.Uint32
}

func (b *HTTPBackend) addrs() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.Addrs
}

// Refresh asks any reachable voter for the current voter set and adopts it, so storage nodes keep
// working after voters have been added or replaced.
func (b *HTTPBackend) Refresh(ctx context.Context) error {
	scheme := b.Scheme
	if scheme == "" {
		scheme = "http"
	}
	var last error
	for _, a := range b.addrs() {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, scheme+"://"+a+"/internal/meta/voters", nil)
		if err != nil {
			return err
		}
		if b.Secret != "" {
			req.Header.Set("Authorization", "Bearer "+b.Secret)
		}
		resp, err := b.Client.Do(req)
		if err != nil {
			last = err
			continue
		}
		var voters map[string]string
		err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&voters)
		resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK {
			last = fmt.Errorf("voters from %s: status %d: %v", a, resp.StatusCode, err)
			continue
		}
		var fresh []string
		for id, addr := range voters {
			if addr == "" {
				addr = b.Seeds[id]
			}
			if addr != "" {
				fresh = append(fresh, addr)
			}
		}
		if len(fresh) == 0 {
			return nil
		}
		sort.Strings(fresh)
		b.mu.Lock()
		b.Addrs = fresh
		b.mu.Unlock()
		return nil
	}
	return last
}

func (b *HTTPBackend) Apply(ctx context.Context, c []byte) ([]byte, error) {
	return b.post(ctx, "/internal/meta/apply", c)
}

func (b *HTTPBackend) Query(ctx context.Context, q []byte) ([]byte, error) {
	path := "/internal/meta/query"
	if ctx.Value(staleKey{}) != nil {
		path += "?stale=1"
	}
	return b.post(ctx, path, q)
}

func (b *HTTPBackend) post(ctx context.Context, path string, body []byte) ([]byte, error) {
	scheme := b.Scheme
	if scheme == "" {
		scheme = "http"
	}
	addrs := b.addrs()
	n := len(addrs)
	start := int(b.pref.Load())
	var last error
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, scheme+"://"+addrs[idx]+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		if b.Secret != "" {
			req.Header.Set("Authorization", "Bearer "+b.Secret)
		}
		resp, err := b.Client.Do(req)
		if err != nil {
			last = err
			continue
		}
		out, rerr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK && rerr == nil {
			b.pref.Store(uint32(idx))
			return out, nil
		}
		last = fmt.Errorf("meta %s: status %d: %s", addrs[idx], resp.StatusCode, bytes.TrimSpace(out))
		if resp.StatusCode == http.StatusConflict {
			return nil, raft.ErrLostLeader // outcome unknown, must not blindly retry
		}
	}
	return nil, fmt.Errorf("no metadata voter reachable: %w", last)
}

// Mount exposes a RaftBackend to storage-only nodes.
func Mount(mux *http.ServeMux, b *RaftBackend, wrap func(http.Handler) http.Handler) {
	if wrap == nil {
		wrap = func(h http.Handler) http.Handler { return h }
	}
	h := func(apply bool) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
			if err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			var out []byte
			ctx := r.Context()
			if apply {
				out, err = b.Apply(ctx, body)
			} else {
				if r.URL.Query().Get("stale") != "" {
					ctx = WithStale(ctx)
				}
				out, err = b.Query(ctx, body)
			}
			if err != nil {
				code := http.StatusInternalServerError
				switch {
				case errors.Is(err, raft.ErrLostLeader):
					code = http.StatusConflict
				case errors.Is(err, raft.ErrNoLeader), errors.Is(err, raft.ErrNotLeader), errors.Is(err, context.DeadlineExceeded):
					code = http.StatusServiceUnavailable
				}
				http.Error(w, err.Error(), code)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(out)
		})
	}
	mux.Handle("GET /internal/meta/voters", wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(b.Node.Members())
	})))
	mux.Handle("POST /internal/meta/apply", wrap(h(true)))
	mux.Handle("POST /internal/meta/query", wrap(h(false)))
}
