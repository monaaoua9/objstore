// Package raft is a compact Raft implementation: leader election with pre-vote and
// check-quorum, log replication, snapshots, linearizable reads via ReadIndex, a crash-safe WAL,
// and single-server membership changes (add/remove one voter at a time).
package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type result struct {
	data []byte
	err  error
}

type proposal struct {
	data      []byte
	w         waiter
	index     uint64 // set once appended
	cancelled bool   // caller gave up before it was appended
}

type waiter struct {
	term uint64
	ch   chan result
}

type Node struct {
	cfg Config
	fsm FSM
	st  Storage
	tr  Transport
	lg  *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	fsmMu sync.Mutex // serialises Apply / Snapshot / Restore

	mu       sync.Mutex
	state    State
	term     uint64
	votedFor string
	leader   string

	base     uint64 // index covered by the snapshot
	baseTerm uint64
	snap     Snapshot
	entries  []Entry // entries[i].Index == base+1+i

	commit  uint64
	applied uint64

	lastHeard time.Time
	deadline  time.Time

	// leader-only
	next        map[string]uint64
	match       map[string]uint64
	sendSeq     uint64               // bumped for every outgoing append/snapshot request
	ackSeq      map[string]uint64    // highest sendSeq each peer has answered
	ackTime     map[string]time.Time // when that answer arrived, for check-quorum
	contactCh   chan struct{}        // closed + replaced whenever an ack lands
	noopIndex   uint64
	leaderSince time.Time

	waiters   map[uint64]waiter
	applyKick chan struct{}
	appliedCh chan struct{} // closed + replaced after every apply
	kicks     map[string]chan struct{}

	// membership: peers is the voter set in force right now (latest config entry in the log, even if
	// uncommitted), addrs the address book, learners are replicated to but not counted
	boot        map[string]string
	peers       []string
	addrs       map[string]string
	configIndex uint64
	learners    map[string]string

	// group commit: proposals queue here and one goroutine appends the whole queue with a single fsync
	propQ    []*proposal
	propKick chan struct{}

	// follower side of a chunked InstallSnapshot in progress
	inbound struct {
		index, term uint64
		buf         []byte
	}
}

func NewNode(cfg Config, fsm FSM, st Storage, tr Transport, lg *slog.Logger) (*Node, error) {
	cfg.setDefaults()
	found := false
	for _, p := range cfg.Peers {
		if p == cfg.ID {
			found = true
		}
	}
	if !found && !cfg.Join {
		return nil, fmt.Errorf("raft: own id %q not in peer list", cfg.ID)
	}
	if lg == nil {
		lg = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		cfg: cfg, fsm: fsm, st: st, tr: tr, lg: lg.With("raft", cfg.ID),
		ctx: ctx, cancel: cancel,
		waiters:   map[uint64]waiter{},
		applyKick: make(chan struct{}, 1),
		propKick:  make(chan struct{}, 1),
		appliedCh: make(chan struct{}),
		contactCh: make(chan struct{}),
		kicks:     map[string]chan struct{}{},
		next:      map[string]uint64{},
		match:     map[string]uint64{},
		ackSeq:    map[string]uint64{},
		ackTime:   map[string]time.Time{},
		addrs:     map[string]string{},
		learners:  map[string]string{},
	}
	if !cfg.Join {
		n.boot = map[string]string{}
		for _, p := range cfg.Peers {
			n.boot[p] = cfg.Addrs[p]
		}
	}
	return n, nil
}

func (n *Node) Start() error {
	term, vote, snap, entries, err := n.st.Load()
	if err != nil {
		return err
	}
	n.term, n.votedFor = term, vote
	n.snap = snap
	n.base, n.baseTerm = snap.Index, snap.Term
	n.entries = entries
	n.commit, n.applied = snap.Index, snap.Index
	if snap.Index > 0 {
		if err := n.fsm.Restore(snap.Data); err != nil {
			return fmt.Errorf("raft: restore snapshot: %w", err)
		}
	}
	for i, e := range n.entries {
		if e.Index != n.base+1+uint64(i) {
			return fmt.Errorf("raft: wal has a gap at %d", e.Index)
		}
	}
	n.lastHeard = time.Now()
	n.resetDeadline()

	n.mu.Lock()
	n.refreshConfigLocked()
	n.mu.Unlock()

	n.wg.Add(3)
	go n.tickLoop()
	go n.applyLoop()
	go n.appendLoop()
	n.lg.Info("raft started", "term", term, "last_index", n.lastIndex(), "snapshot", snap.Index)
	return nil
}

func (n *Node) Stop() {
	n.cancel()
	n.wg.Wait()
	n.mu.Lock()
	n.failWaiters(ErrStopped)
	n.mu.Unlock()
	n.st.Close()
}

// ---- log helpers (callers hold mu) ----

func (n *Node) lastIndex() uint64 { return n.base + uint64(len(n.entries)) }

func (n *Node) lastTerm() uint64 {
	if len(n.entries) == 0 {
		return n.baseTerm
	}
	return n.entries[len(n.entries)-1].Term
}

func (n *Node) termAt(i uint64) (uint64, bool) {
	switch {
	case i == n.base:
		return n.baseTerm, true
	case i < n.base || i > n.lastIndex():
		return 0, false
	}
	return n.entries[i-n.base-1].Term, true
}

func (n *Node) quorum() int { return len(n.peers)/2 + 1 }

func (n *Node) resetDeadline() {
	et := n.cfg.ElectionTimeout
	n.deadline = time.Now().Add(et + time.Duration(rand.Int63n(int64(et))))
}

func (n *Node) mustPersistState() {
	if err := n.st.SaveState(n.term, n.votedFor); err != nil {
		// carrying on without a durable vote could let us vote twice in one term
		panic(fmt.Sprintf("raft: cannot persist term/vote: %v", err))
	}
}

// ---- public status ----

type Status struct {
	Peers   []string
	ID      string
	State   State
	Term    uint64
	Leader  string
	Commit  uint64
	Applied uint64
	Last    uint64
	Base    uint64
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{append([]string(nil), n.peers...), n.cfg.ID, n.state, n.term, n.leader, n.commit, n.applied, n.lastIndex(), n.base}
}

func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader
}

func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leader
}

// ---- timers / elections ----

func (n *Node) tickLoop() {
	defer n.wg.Done()
	iv := n.cfg.HeartbeatInterval / 3
	if iv > 10*time.Millisecond {
		iv = 10 * time.Millisecond
	}
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
			n.tick()
		}
	}
}

func (n *Node) tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	now := time.Now()
	if n.state == Leader {
		if now.Sub(n.leaderSince) < n.cfg.ElectionTimeout {
			return
		}
		alive := 0
		for _, p := range n.peers {
			if p == n.cfg.ID || now.Sub(n.ackTime[p]) < n.cfg.ElectionTimeout {
				alive++
			}
		}
		if alive < n.quorum() {
			n.lg.Warn("lost contact with quorum, stepping down", "term", n.term)
			n.stepDownLocked(n.term)
		}
		return
	}
	if now.After(n.deadline) && n.inPeersLocked(n.cfg.ID) {
		n.startPreVoteLocked()
	}
}

func (n *Node) startPreVoteLocked() {
	n.resetDeadline()
	if n.quorum() == 1 {
		n.startElectionLocked()
		return
	}
	n.state = PreCandidate
	term := n.term
	req := &VoteReq{Term: term + 1, CandidateID: n.cfg.ID, LastLogIndex: n.lastIndex(), LastLogTerm: n.lastTerm(), PreVote: true}
	votes := 1
	for _, p := range n.peers {
		if p == n.cfg.ID {
			continue
		}
		go func() {
			ctx, cancel := context.WithTimeout(n.ctx, n.cfg.ElectionTimeout)
			defer cancel()
			resp, err := n.tr.RequestVote(ctx, p, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.state != PreCandidate || n.term != term {
				return
			}
			if resp.Term > n.term {
				n.stepDownLocked(resp.Term)
				return
			}
			if resp.Granted {
				votes++
				if votes >= n.quorum() {
					n.startElectionLocked()
				}
			}
		}()
	}
}

func (n *Node) startElectionLocked() {
	n.state = Candidate
	n.term++
	n.votedFor = n.cfg.ID
	n.leader = ""
	n.mustPersistState()
	n.resetDeadline()
	term := n.term
	n.lg.Info("starting election", "term", term)
	if n.quorum() == 1 {
		n.becomeLeaderLocked()
		return
	}
	req := &VoteReq{Term: term, CandidateID: n.cfg.ID, LastLogIndex: n.lastIndex(), LastLogTerm: n.lastTerm()}
	votes := 1
	for _, p := range n.peers {
		if p == n.cfg.ID {
			continue
		}
		go func() {
			ctx, cancel := context.WithTimeout(n.ctx, n.cfg.ElectionTimeout)
			defer cancel()
			resp, err := n.tr.RequestVote(ctx, p, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.state != Candidate || n.term != term {
				return
			}
			if resp.Term > n.term {
				n.stepDownLocked(resp.Term)
				return
			}
			if resp.Granted {
				votes++
				if votes >= n.quorum() {
					n.becomeLeaderLocked()
				}
			}
		}()
	}
}

func (n *Node) becomeLeaderLocked() {
	n.state = Leader
	n.leader = n.cfg.ID
	n.leaderSince = time.Now()
	n.lastHeard = n.leaderSince
	for _, p := range n.peers {
		n.initPeerLocked(p)
	}
	for p := range n.learners {
		n.initPeerLocked(p)
	}
	// a leader can't commit entries from older terms until it commits one of its own
	noop := Entry{Index: n.lastIndex() + 1, Term: n.term, Type: EntryNoop}
	n.appendLocked([]Entry{noop})
	n.noopIndex = noop.Index
	n.lg.Info("became leader", "term", n.term, "last_index", n.lastIndex())
	n.advanceCommitLocked()
	n.kickAllLocked()
}

// stepDownLocked drops to follower; term may equal the current one (check-quorum case).
func (n *Node) stepDownLocked(term uint64) {
	wasLeader := n.state == Leader
	if term > n.term {
		n.term = term
		n.votedFor = ""
		n.mustPersistState()
		n.leader = ""
	}
	n.state = Follower
	n.resetDeadline()
	if wasLeader {
		n.leader = ""
		n.failWaiters(ErrLostLeader)
	}
}

func (n *Node) failWaiters(err error) {
	for i, w := range n.waiters {
		w.ch <- result{err: err}
		delete(n.waiters, i)
	}
}

func (n *Node) appendLocked(es []Entry) {
	if err := n.st.Append(es); err != nil {
		panic(fmt.Sprintf("raft: wal append failed: %v", err))
	}
	n.entries = append(n.entries, es...)
	for _, e := range es {
		if e.Type == EntryConfig {
			n.refreshConfigLocked()
			break
		}
	}
}

// ---- RPC handlers ----

func (n *Node) HandleVote(req *VoteReq) *VoteResp {
	n.mu.Lock()
	defer n.mu.Unlock()

	upToDate := req.LastLogTerm > n.lastTerm() ||
		(req.LastLogTerm == n.lastTerm() && req.LastLogIndex >= n.lastIndex())

	// a node that was removed (or never added) must not be able to disturb the group
	if len(n.peers) > 0 && !n.inPeersLocked(req.CandidateID) {
		return &VoteResp{Term: n.term}
	}

	if req.PreVote {
		// refuse while we still have a live leader, otherwise a flaky node keeps forcing elections
		heardLeader := n.state == Leader || time.Since(n.lastHeard) < n.cfg.ElectionTimeout
		ok := req.Term >= n.term && upToDate && !heardLeader
		return &VoteResp{Term: n.term, Granted: ok}
	}

	if req.Term < n.term {
		return &VoteResp{Term: n.term}
	}
	if req.Term > n.term {
		n.stepDownLocked(req.Term)
	}
	if (n.votedFor == "" || n.votedFor == req.CandidateID) && upToDate {
		n.votedFor = req.CandidateID
		n.mustPersistState()
		n.resetDeadline()
		return &VoteResp{Term: n.term, Granted: true}
	}
	return &VoteResp{Term: n.term}
}

func (n *Node) HandleAppendEntries(req *AppendEntriesReq) *AppendEntriesResp {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.term {
		return &AppendEntriesResp{Term: n.term}
	}
	if req.Term > n.term || n.state != Follower {
		n.stepDownLocked(req.Term)
	}
	n.leader = req.LeaderID
	n.lastHeard = time.Now()
	n.resetDeadline()

	last := n.lastIndex()
	if req.PrevLogIndex > last {
		return &AppendEntriesResp{Term: n.term, ConflictIndex: last + 1, LastIndex: last}
	}
	if req.PrevLogIndex >= n.base {
		pt, _ := n.termAt(req.PrevLogIndex)
		if pt != req.PrevLogTerm {
			ci := req.PrevLogIndex
			for ci > n.base+1 {
				t, _ := n.termAt(ci - 1)
				if t != pt {
					break
				}
				ci--
			}
			return &AppendEntriesResp{Term: n.term, ConflictTerm: pt, ConflictIndex: ci, LastIndex: last}
		}
	}

	var fresh []Entry
	for i, e := range req.Entries {
		if e.Index <= n.base {
			continue // already folded into our snapshot
		}
		if e.Index <= n.lastIndex() {
			if t, _ := n.termAt(e.Index); t == e.Term {
				continue
			}
			if e.Index <= n.commit {
				panic(fmt.Sprintf("raft: leader asked us to overwrite committed index %d", e.Index))
			}
			if err := n.st.TruncateFrom(e.Index); err != nil {
				panic(fmt.Sprintf("raft: wal truncate failed: %v", err))
			}
			n.entries = n.entries[:e.Index-n.base-1]
			n.refreshConfigLocked()
		}
		fresh = req.Entries[i:]
		break
	}
	if len(fresh) > 0 {
		n.appendLocked(append([]Entry(nil), fresh...))
	}

	lastNew := req.PrevLogIndex + uint64(len(req.Entries))
	if req.LeaderCommit > n.commit {
		c := min(req.LeaderCommit, lastNew)
		if c > n.commit {
			n.commit = c
			n.kickApply()
		}
	}
	return &AppendEntriesResp{Term: n.term, Success: true, LastIndex: n.lastIndex()}
}

func (n *Node) HandleInstallSnapshot(req *InstallSnapshotReq) *InstallSnapshotResp {
	n.fsmMu.Lock()
	defer n.fsmMu.Unlock()
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.term {
		return &InstallSnapshotResp{Term: n.term}
	}
	if req.Term > n.term || n.state != Follower {
		n.stepDownLocked(req.Term)
	}
	n.leader = req.LeaderID
	n.lastHeard = time.Now()
	n.resetDeadline()

	s := req.Snapshot
	if s.Index <= n.commit {
		return &InstallSnapshotResp{Term: n.term, Next: req.Offset + uint64(len(s.Data))}
	}

	in := &n.inbound
	if req.Offset == 0 || in.index != s.Index || in.term != s.Term {
		in.index, in.term, in.buf = s.Index, s.Term, nil
	}
	if req.Offset != uint64(len(in.buf)) {
		return &InstallSnapshotResp{Term: n.term, Next: uint64(len(in.buf))} // gap or replay: tell the leader where we are
	}
	in.buf = append(in.buf, s.Data...)
	if !req.Done {
		return &InstallSnapshotResp{Term: n.term, Next: uint64(len(in.buf))}
	}
	s.Data = in.buf
	in.buf = nil
	next := req.Offset + uint64(len(req.Snapshot.Data))

	var keep []Entry
	if t, ok := n.termAt(s.Index); ok && t == s.Term && s.Index > n.base {
		keep = append(keep, n.entries[s.Index-n.base:]...)
	}
	if err := n.st.SaveSnapshot(s, keep); err != nil {
		panic(fmt.Sprintf("raft: save snapshot failed: %v", err))
	}
	if err := n.fsm.Restore(s.Data); err != nil {
		panic(fmt.Sprintf("raft: fsm restore failed: %v", err))
	}
	n.snap = s
	n.base, n.baseTerm = s.Index, s.Term
	n.entries = keep
	n.refreshConfigLocked()
	n.commit = max(n.commit, s.Index)
	n.applied = s.Index
	n.lg.Info("installed snapshot", "index", s.Index)
	n.broadcastApplied()
	n.kickApply()
	return &InstallSnapshotResp{Term: n.term, Next: next}
}

// sendSnapshot streams snap to peer in SnapshotChunk pieces, resuming wherever the follower says it is.
func (n *Node) sendSnapshot(peer string, snap Snapshot, term uint64) (*InstallSnapshotResp, error) {
	var off uint64
	for {
		end := min(off+uint64(n.cfg.SnapshotChunk), uint64(len(snap.Data)))
		part := snap
		part.Data = snap.Data[off:end]
		req := &InstallSnapshotReq{Term: term, LeaderID: n.cfg.ID, Snapshot: part, Offset: off, Done: end == uint64(len(snap.Data))}
		ctx, cancel := context.WithTimeout(n.ctx, 2*n.cfg.ElectionTimeout)
		resp, err := n.tr.InstallSnapshot(ctx, peer, req)
		cancel()
		if err != nil || resp.Term > term || req.Done {
			return resp, err
		}
		n.mu.Lock()
		alive := n.state == Leader && n.term == term
		n.mu.Unlock()
		if !alive {
			return resp, nil
		}
		off = min(resp.Next, uint64(len(snap.Data)))
	}
}

// HandleReadIndex runs on the leader for a follower that wants a linearizable read point.
func (n *Node) HandleReadIndex(ctx context.Context) (uint64, error) {
	return n.leaderReadIndex(ctx)
}

// forwarded payloads are tagged so membership requests can ride the same endpoint as commands
const (
	fwdCommand byte = 0
	fwdMember  byte = 1
)

func (n *Node) HandleForward(ctx context.Context, data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("raft: empty forward")
	}
	if data[0] == fwdMember {
		var req memberReq
		if err := json.Unmarshal(data[1:], &req); err != nil {
			return nil, err
		}
		return nil, n.changeMembership(ctx, req)
	}
	return n.proposeLeader(ctx, data[1:])
}

// ---- replication ----

func (n *Node) kickAllLocked() {
	for _, k := range n.kicks {
		select {
		case k <- struct{}{}:
		default:
		}
	}
}

func (n *Node) kickAll() {
	n.mu.Lock()
	n.kickAllLocked()
	n.mu.Unlock()
}

func (n *Node) replicator(peer string, kick chan struct{}) {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-kick:
		case <-t.C:
		}
		n.mu.Lock()
		if !n.wantsLocked(peer) {
			delete(n.kicks, peer)
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
		n.replicateTo(peer)
	}
}

func (n *Node) replicateTo(peer string) {
	for {
		n.mu.Lock()
		if n.state != Leader || !n.wantsLocked(peer) {
			n.mu.Unlock()
			return
		}
		term := n.term
		if _, ok := n.next[peer]; !ok {
			n.initPeerLocked(peer)
		}
		n.sendSeq++
		seq := n.sendSeq
		ni := n.next[peer]
		var areq *AppendEntriesReq
		var sreq *InstallSnapshotReq
		var snap Snapshot
		if ni <= n.base {
			snap = n.snap
			sreq = &InstallSnapshotReq{Term: term, LeaderID: n.cfg.ID}
		} else {
			prev := ni - 1
			pt, _ := n.termAt(prev)
			hi := min(n.lastIndex(), prev+uint64(n.cfg.MaxBatch))
			var es []Entry
			if hi > prev {
				es = append(es, n.entries[prev-n.base:hi-n.base]...)
			}
			areq = &AppendEntriesReq{Term: term, LeaderID: n.cfg.ID, PrevLogIndex: prev, PrevLogTerm: pt, Entries: es, LeaderCommit: n.commit}
		}
		n.mu.Unlock()

		ctx, cancel := context.WithTimeout(n.ctx, 2*n.cfg.ElectionTimeout)
		var aresp *AppendEntriesResp
		var sresp *InstallSnapshotResp
		var err error
		if sreq != nil {
			sresp, err = n.sendSnapshot(peer, snap, term)
		} else {
			aresp, err = n.tr.AppendEntries(ctx, peer, areq)
		}
		cancel()
		if err != nil {
			return
		}

		n.mu.Lock()
		if n.state != Leader || n.term != term {
			n.mu.Unlock()
			return
		}
		respTerm := uint64(0)
		if sresp != nil {
			respTerm = sresp.Term
		} else {
			respTerm = aresp.Term
		}
		if respTerm > n.term {
			n.stepDownLocked(respTerm)
			n.mu.Unlock()
			return
		}
		n.noteAckLocked(peer, seq)

		more := false
		switch {
		case sresp != nil:
			if snap.Index > n.match[peer] {
				n.match[peer] = snap.Index
			}
			n.next[peer] = n.match[peer] + 1
			more = n.next[peer] <= n.lastIndex()
		case aresp.Success:
			if m := areq.PrevLogIndex + uint64(len(areq.Entries)); m > n.match[peer] {
				n.match[peer] = m
				n.advanceCommitLocked()
			}
			n.next[peer] = n.match[peer] + 1
			more = n.next[peer] <= n.lastIndex()
		default:
			nx := aresp.ConflictIndex
			if aresp.ConflictTerm != 0 {
				for i := n.lastIndex(); i > n.base; i-- {
					if t, _ := n.termAt(i); t == aresp.ConflictTerm {
						nx = i + 1
						break
					}
				}
			}
			nx = max(nx, 1)
			nx = min(nx, n.lastIndex()+1)
			if nx >= n.next[peer] {
				nx = n.next[peer] - 1 // always make progress
			}
			n.next[peer] = max(nx, 1)
			more = true
		}
		n.mu.Unlock()
		if !more {
			return
		}
	}
}

func (n *Node) noteAckLocked(peer string, seq uint64) {
	n.ackTime[peer] = time.Now()
	if seq > n.ackSeq[peer] {
		n.ackSeq[peer] = seq
	}
	close(n.contactCh)
	n.contactCh = make(chan struct{})
}

func (n *Node) advanceCommitLocked() {
	ms := make([]uint64, 0, len(n.peers))
	for _, p := range n.peers {
		if p == n.cfg.ID {
			ms = append(ms, n.lastIndex())
		} else {
			ms = append(ms, n.match[p])
		}
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i] > ms[j] })
	if len(ms) == 0 {
		return
	}
	idx := ms[n.quorum()-1]
	if idx <= n.commit {
		return
	}
	if t, ok := n.termAt(idx); ok && t == n.term {
		n.commit = idx
		n.kickApply()
	}
}

// ---- apply loop / snapshots ----

func (n *Node) kickApply() {
	select {
	case n.applyKick <- struct{}{}:
	default:
	}
}

func (n *Node) broadcastApplied() {
	close(n.appliedCh)
	n.appliedCh = make(chan struct{})
}

func (n *Node) applyLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.applyKick:
		}
		n.applyReady()
	}
}

func (n *Node) applyReady() {
	for {
		n.mu.Lock()
		if n.applied >= n.commit {
			due := n.applied-n.base >= n.cfg.SnapshotThreshold
			n.mu.Unlock()
			if due {
				n.takeSnapshot()
			}
			return
		}
		idx := n.applied + 1
		if idx <= n.base {
			n.mu.Unlock()
			return
		}
		e := n.entries[idx-n.base-1]
		n.mu.Unlock()

		var out []byte
		n.fsmMu.Lock()
		// a snapshot install may have jumped past us while we waited for fsmMu
		n.mu.Lock()
		stale := n.applied+1 != idx
		n.mu.Unlock()
		if stale {
			n.fsmMu.Unlock()
			continue
		}
		if e.Type == EntryCommand {
			out = n.fsm.Apply(e.Index, e.Data)
		}
		n.fsmMu.Unlock()

		n.mu.Lock()
		n.applied = idx
		if w, ok := n.waiters[idx]; ok {
			delete(n.waiters, idx)
			if w.term == e.Term {
				w.ch <- result{data: out}
			} else {
				w.ch <- result{err: ErrLostLeader}
			}
		}
		// a leader that voted itself out hands over once the change is committed
		if e.Type == EntryConfig && n.state == Leader && !n.inPeersLocked(n.cfg.ID) && e.Index >= n.configIndex {
			n.lg.Info("removed from the voter set, stepping down")
			n.stepDownLocked(n.term)
		}
		n.broadcastApplied()
		n.mu.Unlock()
	}
}

func (n *Node) takeSnapshot() {
	n.mu.Lock()
	at := n.applied
	n.mu.Unlock()

	n.fsmMu.Lock()
	n.mu.Lock()
	if n.applied != at { // install raced us, give up this round
		n.mu.Unlock()
		n.fsmMu.Unlock()
		return
	}
	n.mu.Unlock()
	data, err := n.fsm.Snapshot()
	n.fsmMu.Unlock()
	if err != nil {
		n.lg.Error("fsm snapshot failed", "err", err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if at <= n.base {
		return
	}
	term, _ := n.termAt(at)
	s := Snapshot{Index: at, Term: term, Peers: n.configAtLocked(at), Data: data}
	keep := append([]Entry(nil), n.entries[at-n.base:]...)
	if err := n.st.SaveSnapshot(s, keep); err != nil {
		n.lg.Error("save snapshot failed", "err", err)
		return
	}
	n.snap = s
	n.base, n.baseTerm = at, term
	n.entries = keep
	n.lg.Info("log compacted", "index", at, "snapshot_bytes", len(data))
}

// ---- client API ----

// Propose replicates data and returns whatever FSM.Apply returned for it. It follows the leader
// across nodes. ErrLostLeader means the outcome is unknown; everything else means "not applied".
func (n *Node) Propose(ctx context.Context, data []byte) ([]byte, error) {
	for {
		out, err := n.proposeOnce(ctx, data)
		if !errors.Is(err, ErrNotLeader) && !errors.Is(err, ErrNoLeader) {
			return out, err
		}
		select {
		case <-ctx.Done():
			return nil, err
		case <-n.ctx.Done():
			return nil, ErrStopped
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func (n *Node) proposeOnce(ctx context.Context, data []byte) ([]byte, error) {
	n.mu.Lock()
	isLeader, leader := n.state == Leader, n.leader
	n.mu.Unlock()
	if isLeader {
		return n.proposeLeader(ctx, data)
	}
	if leader == "" || leader == n.cfg.ID {
		return nil, ErrNoLeader
	}
	return n.tr.Forward(ctx, leader, append([]byte{fwdCommand}, data...))
}

func (n *Node) proposeLeader(ctx context.Context, data []byte) ([]byte, error) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return nil, ErrNotLeader
	}
	p := &proposal{data: data, w: waiter{term: n.term, ch: make(chan result, 1)}}
	n.propQ = append(n.propQ, p)
	n.mu.Unlock()
	select {
	case n.propKick <- struct{}{}:
	default:
	}

	select {
	case r := <-p.w.ch:
		return r.data, r.err
	case <-ctx.Done():
		n.mu.Lock()
		if p.index != 0 {
			delete(n.waiters, p.index)
		} else {
			p.cancelled = true
		}
		n.mu.Unlock()
		return nil, ctx.Err()
	case <-n.ctx.Done():
		return nil, ErrStopped
	}
}

// appendLoop turns queued proposals into log entries. Everything that piled up while the previous
// fsync was running goes out in the next one, so throughput under load is not bounded by fsync latency.
func (n *Node) appendLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.propKick:
		}
		n.mu.Lock()
		q := n.propQ
		n.propQ = nil
		if n.state != Leader {
			for _, p := range q {
				p.w.ch <- result{err: ErrNotLeader}
			}
			n.mu.Unlock()
			continue
		}
		es := make([]Entry, 0, len(q))
		live := q[:0]
		for _, p := range q {
			if p.cancelled {
				continue
			}
			if p.w.term != n.term { // leadership changed between enqueue and now
				p.w.ch <- result{err: ErrLostLeader}
				continue
			}
			p.index = n.lastIndex() + 1 + uint64(len(es))
			es = append(es, Entry{Index: p.index, Term: n.term, Type: EntryCommand, Data: p.data})
			live = append(live, p)
		}
		if len(es) > 0 {
			n.appendLocked(es)
			for _, p := range live {
				n.waiters[p.index] = p.w
			}
			n.advanceCommitLocked()
			n.kickAllLocked()
		}
		n.mu.Unlock()
	}
}

// ReadBarrier returns once the local FSM reflects every write that completed before the call.
func (n *Node) ReadBarrier(ctx context.Context) error {
	n.mu.Lock()
	isLeader, leader := n.state == Leader, n.leader
	n.mu.Unlock()

	var idx uint64
	var err error
	switch {
	case isLeader:
		idx, err = n.leaderReadIndex(ctx)
	case leader == "" || leader == n.cfg.ID:
		return ErrNoLeader
	default:
		idx, err = n.tr.ReadIndex(ctx, leader)
	}
	if err != nil {
		return err
	}
	return n.waitApplied(ctx, idx)
}

func (n *Node) leaderReadIndex(ctx context.Context) (uint64, error) {
	// phase 1: wait for our own-term noop to commit so commit index is trustworthy
	n.mu.Lock()
	for {
		if n.state != Leader {
			n.mu.Unlock()
			return 0, ErrNotLeader
		}
		if n.commit >= n.noopIndex {
			break
		}
		ch := n.appliedCh
		n.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
		n.mu.Lock()
	}
	idx := n.commit
	term := n.term
	n.mu.Unlock()

	// phase 2: prove we're still leader by getting acks for requests sent after this point.
	// Sequence numbers, not timestamps: windows clocks tick coarsely enough to alias.
	n.mu.Lock()
	startSeq := n.sendSeq
	n.mu.Unlock()
	for {
		n.mu.Lock()
		if n.state != Leader || n.term != term {
			n.mu.Unlock()
			return 0, ErrNotLeader
		}
		got := 0
		for _, p := range n.peers {
			if p == n.cfg.ID || n.ackSeq[p] > startSeq {
				got++
			}
		}
		if got >= n.quorum() {
			n.mu.Unlock()
			return idx, nil
		}
		ch := n.contactCh
		n.kickAllLocked()
		n.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-n.ctx.Done():
			return 0, ErrStopped
		case <-time.After(n.cfg.HeartbeatInterval):
		}
	}
}

func (n *Node) waitApplied(ctx context.Context, idx uint64) error {
	for {
		n.mu.Lock()
		if n.applied >= idx {
			n.mu.Unlock()
			return nil
		}
		ch := n.appliedCh
		n.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-n.ctx.Done():
			return ErrStopped
		}
	}
}
