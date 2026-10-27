// Package app wires one node together: blob store, optional Raft voter, gateway, repair, scrubber.
package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/monaaoua9/objstore/internal/blobstore"
	"github.com/monaaoua9/objstore/internal/cluster"
	"github.com/monaaoua9/objstore/internal/engine"
	"github.com/monaaoua9/objstore/internal/gateway"
	"github.com/monaaoua9/objstore/internal/httpx"
	"github.com/monaaoua9/objstore/internal/meta"
	"github.com/monaaoua9/objstore/internal/metrics"
	"github.com/monaaoua9/objstore/internal/raft"
	"github.com/monaaoua9/objstore/internal/repair"
	"github.com/monaaoua9/objstore/internal/s3api"
	"github.com/monaaoua9/objstore/internal/scrub"
	"github.com/monaaoua9/objstore/internal/shardrpc"
)

type Config struct {
	ID        string
	Listen    string // bind address; ignored if Listener is set
	Listener  net.Listener
	Advertise string // address other nodes use to reach us (defaults to the listener address)
	DataDir   string
	Zone      string
	Weight    int

	// Voters maps raft voter id -> address. If our ID is in it we run a Raft replica,
	// otherwise we are a storage/gateway-only node that talks to the voters.
	Voters map[string]string
	// Join runs a Raft replica that starts empty and waits to be added with `objctl addvoter`;
	// Voters then only lists existing voters to contact.
	Join bool
	// VoterRefresh is how often storage-only nodes re-read the voter list (default 10s).
	VoterRefresh time.Duration

	K, M          int
	ShardSize     int
	WriteQuorum   int
	MaxObjectSize int64

	ClusterSecret string
	APIToken      string
	TLSCert       string
	TLSKey        string
	TLSCA         string

	RaftHeartbeat time.Duration
	RaftElection  time.Duration
	RaftNoSync    bool // tests only: skips fsync on the WAL
	RaftSnapshot  uint64

	// S3 is optional: set S3Listen (or S3Listener) and at least one key pair to expose the S3 API.
	S3Listen   string
	S3Listener net.Listener
	S3Keys     map[string]s3api.Key // access key -> secret and permissions
	S3Region   string

	Repair repair.Config
	Scrub  scrub.Config

	Logger *slog.Logger
}

type Node struct {
	Cfg  Config
	Log  *slog.Logger
	Meta *meta.Client
	View *cluster.View
	Eng  *engine.Engine
	Raft *raft.Node // nil on storage-only nodes
	Blob *blobstore.Store
	Reg  *metrics.Registry
	Scrb *scrub.Scrubber
	Sup  *repair.Supervisor

	fsm *meta.FSM

	addr   string
	srv    *http.Server
	s3srv  *http.Server
	s3     *s3api.Server
	s3addr string
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (n *Node) Addr() string { return n.addr }

// S3Addr is empty when the S3 listener is off.
func (n *Node) S3Addr() string { return n.s3addr }

// SetS3Keys swaps the S3 credential set at runtime; a no-op when the S3 listener is off.
func (n *Node) SetS3Keys(keys map[string]s3api.Key) {
	if n.s3 != nil {
		n.s3.SetKeys(keys)
	}
}

func Start(cfg Config) (*Node, error) {
	lg := cfg.Logger
	if lg == nil {
		lg = slog.Default()
	}
	lg = lg.With("node", cfg.ID)
	if cfg.K == 0 {
		cfg.K, cfg.M = 4, 2
	}

	var srvTLS, cliTLS *tls.Config
	if cfg.TLSCert != "" {
		var err error
		if srvTLS, cliTLS, err = loadTLS(cfg.TLSCert, cfg.TLSKey, cfg.TLSCA); err != nil {
			return nil, err
		}
	}
	scheme := "http"
	if srvTLS != nil {
		scheme = "https"
	}

	ln := cfg.Listener
	if ln == nil {
		var err error
		if ln, err = net.Listen("tcp", cfg.Listen); err != nil {
			return nil, err
		}
	}
	advertise := cfg.Advertise
	if advertise == "" {
		advertise = ln.Addr().String()
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	blob, err := blobstore.Open(filepath.Join(cfg.DataDir, "blobs"))
	if err != nil {
		return nil, fmt.Errorf("open blob store: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{Cfg: cfg, Log: lg, Blob: blob, Reg: metrics.NewRegistry(), addr: advertise, ctx: ctx, cancel: cancel}
	httpc := httpx.NewClient(cliTLS)
	guard := httpx.RequireSecret(cfg.ClusterSecret)
	mux := http.NewServeMux()

	voterIDs := make([]string, 0, len(cfg.Voters))
	for id := range cfg.Voters {
		voterIDs = append(voterIDs, id)
	}
	sort.Strings(voterIDs)

	var backend meta.Backend
	if _, isVoter := cfg.Voters[cfg.ID]; isVoter || cfg.Join {
		fsm, err := meta.NewFSMAt(filepath.Join(cfg.DataDir, "meta-objects.spill"))
		if err != nil {
			cancel()
			return nil, err
		}
		st, err := raft.NewFileStorage(filepath.Join(cfg.DataDir, "raft"), cfg.RaftNoSync)
		if err != nil {
			cancel()
			return nil, err
		}
		book := map[string]string{cfg.ID: advertise}
		for id, a := range cfg.Voters {
			if id != cfg.ID {
				book[id] = a
			}
		}
		tr := &raft.HTTPTransport{Client: httpc, Scheme: scheme, Addrs: book, Secret: cfg.ClusterSecret}
		peers := voterIDs
		if cfg.Join {
			peers = nil
		}
		rn, err := raft.NewNode(raft.Config{
			ID: cfg.ID, Peers: peers, Addrs: book, Join: cfg.Join,
			HeartbeatInterval: cfg.RaftHeartbeat, ElectionTimeout: cfg.RaftElection,
			SnapshotThreshold: cfg.RaftSnapshot,
		}, fsm, st, tr, lg)
		if err != nil {
			cancel()
			return nil, err
		}
		if err := rn.Start(); err != nil {
			cancel()
			return nil, fmt.Errorf("start raft: %w", err)
		}
		n.Raft = rn
		n.fsm = fsm
		rb := &meta.RaftBackend{Node: rn, FSM: fsm}
		raft.Mount(mux, rn, guard)
		meta.Mount(mux, rb, guard)
		backend = rb
	} else {
		var addrs []string
		for _, id := range voterIDs {
			addrs = append(addrs, cfg.Voters[id])
		}
		if len(addrs) == 0 {
			cancel()
			return nil, errors.New("no metadata voters configured")
		}
		hb := &meta.HTTPBackend{Client: httpc, Scheme: scheme, Addrs: addrs, Secret: cfg.ClusterSecret, Seeds: cfg.Voters}
		backend = hb
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			every := cfg.VoterRefresh
			if every == 0 {
				every = 10 * time.Second
			}
			t := time.NewTicker(every)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					c, cancel := context.WithTimeout(ctx, 3*time.Second)
					if err := hb.Refresh(c); err != nil {
						lg.Debug("voter list refresh failed", "err", err)
					}
					cancel()
				}
			}
		}()
	}
	n.Meta = &meta.Client{B: backend}
	n.View = cluster.NewView(n.Meta, lg)

	rpc := &shardrpc.Client{HTTP: httpc, Scheme: scheme, Secret: cfg.ClusterSecret}
	(&shardrpc.Server{
		NodeID: cfg.ID, Store: blob,
		BytesIn:  n.Reg.Counter("objstore_node_bytes_in_total", "Shard bytes received."),
		BytesOut: n.Reg.Counter("objstore_node_bytes_out_total", "Shard bytes served."),
		Rejected: n.Reg.Counter("objstore_node_rejected_frames_total", "Uploads refused because a frame failed its checksum."),
	}).Mount(mux, guard)

	eng, err := engine.New(n.Meta, n.View, rpc, engine.Config{
		K: cfg.K, M: cfg.M, ShardSize: cfg.ShardSize, WriteQuorum: cfg.WriteQuorum, MaxObjectSize: cfg.MaxObjectSize,
	}, engine.NewMetrics(n.Reg), lg)
	if err != nil {
		cancel()
		return nil, err
	}
	n.Eng = eng

	gw := gateway.New(eng, n.Meta, n.View, cfg.APIToken, n.Reg, lg)
	if n.Raft != nil {
		gw.RaftStatus = n.Raft.Status
		gw.Raft = n.Raft
	}
	gw.Mount(mux)

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		c, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if _, err := n.Meta.Stats(meta.WithStale(c)); err != nil {
			http.Error(w, "metadata unreachable", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ready\n"))
	})
	mux.Handle("GET /metrics", n.Reg.Handler())
	n.registerGauges()

	isLeader := func() bool { return n.Raft != nil && n.Raft.IsLeader() }
	n.Sup = repair.New(n.Meta, n.View, rpc, eng, isLeader, cfg.Repair, n.Reg, lg)
	n.Scrb = scrub.New(cfg.ID, blob, n.Meta, cfg.Scrub, n.Reg, lg)

	n.srv = httpx.NewServer(advertise, mux, srvTLS)
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		var err error
		if srvTLS != nil {
			err = n.srv.ServeTLS(ln, "", "")
		} else {
			err = n.srv.Serve(ln)
		}
		if !errors.Is(err, http.ErrServerClosed) {
			lg.Error("http server stopped", "err", err)
		}
	}()

	if len(cfg.S3Keys) > 0 {
		s3ln := cfg.S3Listener
		if s3ln == nil && cfg.S3Listen != "" {
			if s3ln, err = net.Listen("tcp", cfg.S3Listen); err != nil {
				cancel()
				return nil, err
			}
		}
		if s3ln != nil {
			n.s3addr = s3ln.Addr().String()
			n.s3 = s3api.New(eng, n.Meta, cfg.S3Keys, cfg.S3Region, lg)
			n.s3srv = httpx.NewServer(n.s3addr, n.s3, srvTLS)
			n.wg.Add(1)
			go func() {
				defer n.wg.Done()
				var err error
				if srvTLS != nil {
					err = n.s3srv.ServeTLS(s3ln, "", "")
				} else {
					err = n.s3srv.Serve(s3ln)
				}
				if !errors.Is(err, http.ErrServerClosed) {
					lg.Error("s3 server stopped", "err", err)
				}
			}()
			lg.Info("s3 api listening", "addr", n.s3addr)
		}
	}

	n.wg.Add(3)
	go func() { defer n.wg.Done(); n.register(ctx) }()
	go func() { defer n.wg.Done(); n.Sup.Run(ctx) }()
	go func() { defer n.wg.Done(); n.Scrb.Run(ctx) }()

	lg.Info("node started", "addr", advertise, "voter", n.Raft != nil, "k", cfg.K, "m", cfg.M)
	return n, nil
}

// register announces this node to the metadata group, retrying until a leader exists.
func (n *Node) register(ctx context.Context) {
	info := meta.Node{ID: n.Cfg.ID, Addr: n.addr, Zone: n.Cfg.Zone, Weight: n.Cfg.Weight}
	backoff := 200 * time.Millisecond
	for ctx.Err() == nil {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := n.Meta.SetNode(c, info)
		cancel()
		if err == nil {
			break
		}
		n.Log.Debug("registration pending", "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 3*time.Second)
	}
	n.View.Run(ctx, time.Second)
}

func (n *Node) registerGauges() {
	n.Reg.GaugeFunc("objstore_node_shards", "Shard files stored on this node.", func() float64 { return float64(n.Blob.Count()) })
	n.Reg.GaugeFunc("objstore_node_bytes", "Bytes of shard data stored on this node.", func() float64 { return float64(n.Blob.Bytes()) })
	n.Reg.GaugeFunc("objstore_raft_is_leader", "1 if this node is the metadata leader.", func() float64 {
		if n.Raft != nil && n.Raft.IsLeader() {
			return 1
		}
		return 0
	})
	n.Reg.GaugeFunc("objstore_raft_term", "Current Raft term.", func() float64 {
		if n.Raft == nil {
			return 0
		}
		return float64(n.Raft.Status().Term)
	})
	n.Reg.GaugeFunc("objstore_raft_applied_index", "Last applied Raft log index.", func() float64 {
		if n.Raft == nil {
			return 0
		}
		return float64(n.Raft.Status().Applied)
	})
}

// Stop drains in-flight requests, then shuts background work and Raft down.
func (n *Node) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	n.srv.Shutdown(ctx)
	if n.s3srv != nil {
		n.s3srv.Shutdown(ctx)
	}
	n.cancel()
	n.wg.Wait()
	n.Eng.Wait()
	if n.Raft != nil {
		n.Raft.Stop()
		n.fsm.Close()
	}
}

// Kill drops the node without any grace, for failure-injection tests.
func (n *Node) Kill() {
	n.srv.Close()
	if n.s3srv != nil {
		n.s3srv.Close()
	}
	n.cancel()
	n.wg.Wait()
	if n.Raft != nil {
		n.Raft.Stop()
		n.fsm.Close()
	}
}

func loadTLS(certFile, keyFile, caFile string) (srv, cli *tls.Config, err error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, nil, err
	}
	srv = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	cli = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, nil, errors.New("no certificates in CA file")
		}
		cli.RootCAs = pool
		srv.ClientCAs = pool
		srv.ClientAuth = tls.VerifyClientCertIfGiven
	}
	return srv, cli, nil
}
