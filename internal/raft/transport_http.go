package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

// HTTPTransport speaks JSON over the shared HTTP/2 client. addrs maps peer id -> "host:port".
type HTTPTransport struct {
	Client *http.Client
	Scheme string
	Addrs  map[string]string
	Secret string

	mu sync.RWMutex
}

// SetPeers merges in addresses learned from membership changes. The caller's map is never mutated.
func (t *HTTPTransport) SetPeers(addrs map[string]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	m := make(map[string]string, len(t.Addrs)+len(addrs))
	for k, v := range t.Addrs {
		m[k] = v
	}
	for k, v := range addrs {
		if v != "" {
			m[k] = v
		}
	}
	t.Addrs = m
}

func (t *HTTPTransport) url(to, path string) (string, error) {
	t.mu.RLock()
	a, ok := t.Addrs[to]
	t.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("raft: unknown peer %q", to)
	}
	s := t.Scheme
	if s == "" {
		s = "http"
	}
	return s + "://" + a + path, nil
}

func (t *HTTPTransport) do(ctx context.Context, to, path string, body []byte, ctype string) ([]byte, error) {
	u, err := t.url(to, path)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", ctype)
	if t.Secret != "" {
		req.Header.Set("Authorization", "Bearer "+t.Secret)
	}
	resp, err := t.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, 1<<30))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, statusToErr(resp.StatusCode, string(out))
	}
	return out, nil
}

func statusToErr(code int, msg string) error {
	switch code {
	case http.StatusMisdirectedRequest:
		return ErrNotLeader
	case http.StatusConflict:
		return ErrLostLeader
	case http.StatusServiceUnavailable:
		return ErrNoLeader
	case http.StatusBadRequest:
		return fmt.Errorf("%w: %s", ErrMembership, strings.TrimSpace(msg))
	case http.StatusTooManyRequests:
		return ErrConfigBusy
	}
	return fmt.Errorf("raft rpc: status %d: %s", code, strings.TrimSpace(msg))
}

func (t *HTTPTransport) call(ctx context.Context, to, path string, in, out any) error {
	b, err := json.Marshal(in)
	if err != nil {
		return err
	}
	res, err := t.do(ctx, to, path, b, "application/json")
	if err != nil {
		return err
	}
	return json.Unmarshal(res, out)
}

func (t *HTTPTransport) AppendEntries(ctx context.Context, to string, r *AppendEntriesReq) (*AppendEntriesResp, error) {
	var out AppendEntriesResp
	return &out, t.call(ctx, to, "/raft/append", r, &out)
}

func (t *HTTPTransport) RequestVote(ctx context.Context, to string, r *VoteReq) (*VoteResp, error) {
	var out VoteResp
	return &out, t.call(ctx, to, "/raft/vote", r, &out)
}

func (t *HTTPTransport) InstallSnapshot(ctx context.Context, to string, r *InstallSnapshotReq) (*InstallSnapshotResp, error) {
	var out InstallSnapshotResp
	return &out, t.call(ctx, to, "/raft/snapshot", r, &out)
}

func (t *HTTPTransport) ReadIndex(ctx context.Context, to string) (uint64, error) {
	var out struct{ Index uint64 }
	err := t.call(ctx, to, "/raft/readindex", struct{}{}, &out)
	return out.Index, err
}

func (t *HTTPTransport) Forward(ctx context.Context, to string, data []byte) ([]byte, error) {
	return t.do(ctx, to, "/raft/forward", data, "application/octet-stream")
}

// Mount registers the RPC endpoints for n on mux. wrap is applied to each handler (auth).
func Mount(mux *http.ServeMux, n *Node, wrap func(http.Handler) http.Handler) {
	if wrap == nil {
		wrap = func(h http.Handler) http.Handler { return h }
	}
	rpc := func(path string, fn func(w http.ResponseWriter, r *http.Request)) {
		mux.Handle("POST "+path, wrap(http.HandlerFunc(fn)))
	}
	decode := func(r *http.Request, v any) error {
		return json.NewDecoder(io.LimitReader(r.Body, 1<<30)).Decode(v)
	}
	reply := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(v)
	}

	rpc("/raft/append", func(w http.ResponseWriter, r *http.Request) {
		var req AppendEntriesReq
		if decode(r, &req) != nil {
			http.Error(w, "bad request", 400)
			return
		}
		reply(w, n.HandleAppendEntries(&req))
	})
	rpc("/raft/vote", func(w http.ResponseWriter, r *http.Request) {
		var req VoteReq
		if decode(r, &req) != nil {
			http.Error(w, "bad request", 400)
			return
		}
		reply(w, n.HandleVote(&req))
	})
	rpc("/raft/snapshot", func(w http.ResponseWriter, r *http.Request) {
		var req InstallSnapshotReq
		if decode(r, &req) != nil {
			http.Error(w, "bad request", 400)
			return
		}
		reply(w, n.HandleInstallSnapshot(&req))
	})
	rpc("/raft/readindex", func(w http.ResponseWriter, r *http.Request) {
		idx, err := n.HandleReadIndex(r.Context())
		if err != nil {
			writeErr(w, err)
			return
		}
		reply(w, struct{ Index uint64 }{idx})
	})
	rpc("/raft/forward", func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(io.LimitReader(r.Body, 1<<30))
		if err != nil {
			http.Error(w, "bad request", 400)
			return
		}
		out, err := n.HandleForward(r.Context(), data)
		if err != nil {
			writeErr(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(out)
	})
}

func writeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotLeader):
		http.Error(w, err.Error(), http.StatusMisdirectedRequest)
	case errors.Is(err, ErrMembership):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, ErrConfigBusy):
		http.Error(w, err.Error(), http.StatusTooManyRequests)
	case errors.Is(err, ErrLostLeader):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, ErrNoLeader), errors.Is(err, ErrStopped):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
