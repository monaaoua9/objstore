package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

// PeerSetter is implemented by transports that need to learn addresses as the voter set changes.
type PeerSetter interface {
	SetPeers(addrs map[string]string)
}

type memberReq struct {
	Op   string // "add" | "remove"
	ID   string
	Addr string
}

// catchUpGap is how far behind a new server may be when we let it vote.
const catchUpGap = 32

// AddVoter brings id into the voter set. The server is first replicated to as a non-voting learner
// until it is nearly caught up (so a slow disk can't stall commits), then a config entry makes it a
// voter. It can be called on any voter and is forwarded to the leader.
func (n *Node) AddVoter(ctx context.Context, id, addr string) error {
	if id == "" || addr == "" {
		return fmt.Errorf("%w: id and address are required", ErrMembership)
	}
	return n.membership(ctx, memberReq{Op: "add", ID: id, Addr: addr})
}

func (n *Node) RemoveVoter(ctx context.Context, id string) error {
	return n.membership(ctx, memberReq{Op: "remove", ID: id})
}

// Members returns the voter set currently in force, id -> address (address may be empty for
// bootstrap voters whose address lives only in the transport).
func (n *Node) Members() map[string]string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make(map[string]string, len(n.peers))
	for _, p := range n.peers {
		out[p] = n.addrs[p]
	}
	return out
}

func (n *Node) membership(ctx context.Context, req memberReq) error {
	for {
		n.mu.Lock()
		isLeader, leader := n.state == Leader, n.leader
		n.mu.Unlock()

		var err error
		switch {
		case isLeader:
			err = n.changeMembership(ctx, req)
		case leader == "" || leader == n.cfg.ID:
			err = ErrNoLeader
		default:
			b, _ := json.Marshal(req)
			_, err = n.tr.Forward(ctx, leader, append([]byte{fwdMember}, b...))
		}
		if !errors.Is(err, ErrNotLeader) && !errors.Is(err, ErrNoLeader) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-n.ctx.Done():
			return ErrStopped
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// changeMembership runs on the leader.
func (n *Node) changeMembership(ctx context.Context, req memberReq) error {
	if req.Op == "add" {
		if err := n.catchUpLearner(ctx, req.ID, req.Addr); err != nil {
			return err
		}
	}

	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	// the usual single-server safety rules: our own term must have committed something, and the
	// previous config must be committed before the next one is proposed
	if n.commit < n.noopIndex || n.configIndex > n.commit {
		n.mu.Unlock()
		return ErrConfigBusy
	}
	members := make(map[string]string, len(n.peers)+1)
	for _, p := range n.peers {
		members[p] = n.addrs[p]
	}
	switch req.Op {
	case "add":
		if _, ok := members[req.ID]; ok {
			n.mu.Unlock()
			return fmt.Errorf("%w: %s is already a voter", ErrMembership, req.ID)
		}
		members[req.ID] = req.Addr
	case "remove":
		if _, ok := members[req.ID]; !ok {
			n.mu.Unlock()
			return fmt.Errorf("%w: %s is not a voter", ErrMembership, req.ID)
		}
		if len(members) == 1 {
			n.mu.Unlock()
			return fmt.Errorf("%w: cannot remove the last voter", ErrMembership)
		}
		delete(members, req.ID)
	default:
		n.mu.Unlock()
		return fmt.Errorf("%w: unknown op %q", ErrMembership, req.Op)
	}
	data, _ := json.Marshal(members)
	e := Entry{Index: n.lastIndex() + 1, Term: n.term, Type: EntryConfig, Data: data}
	n.appendLocked([]Entry{e})
	w := waiter{term: e.Term, ch: make(chan result, 1)}
	n.waiters[e.Index] = w
	n.lg.Info("membership change proposed", "op", req.Op, "id", req.ID, "index", e.Index, "voters", len(members))
	n.advanceCommitLocked()
	n.kickAllLocked()
	n.mu.Unlock()

	select {
	case r := <-w.ch:
		return r.err
	case <-ctx.Done():
		n.mu.Lock()
		delete(n.waiters, e.Index)
		n.mu.Unlock()
		return ctx.Err()
	case <-n.ctx.Done():
		return ErrStopped
	}
}

func (n *Node) catchUpLearner(ctx context.Context, id, addr string) error {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	if n.inPeersLocked(id) {
		n.mu.Unlock()
		return fmt.Errorf("%w: %s is already a voter", ErrMembership, id)
	}
	n.learners[id] = addr
	n.addrs[id] = addr
	n.pushPeersLocked()
	n.initPeerLocked(id)
	n.ensureReplicatorsLocked()
	n.kickAllLocked()
	n.mu.Unlock()

	giveUp := func() {
		n.mu.Lock()
		delete(n.learners, id)
		n.mu.Unlock()
	}
	for {
		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			giveUp()
			return ErrNotLeader
		}
		caught := n.ackSeq[id] > 0 && n.match[id]+catchUpGap >= n.lastIndex() // must have answered at least once
		n.mu.Unlock()
		if caught {
			return nil
		}
		select {
		case <-ctx.Done():
			giveUp()
			return fmt.Errorf("%w: %s did not catch up: %v", ErrMembership, id, ctx.Err())
		case <-n.ctx.Done():
			return ErrStopped
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// ---- config bookkeeping (callers hold mu) ----

func (n *Node) inPeersLocked(id string) bool {
	for _, p := range n.peers {
		if p == id {
			return true
		}
	}
	return false
}

func (n *Node) wantsLocked(id string) bool {
	if id == n.cfg.ID {
		return false
	}
	if _, ok := n.learners[id]; ok {
		return true
	}
	return n.inPeersLocked(id)
}

func (n *Node) initPeerLocked(p string) {
	n.next[p] = n.lastIndex() + 1
	n.match[p] = 0
	n.ackSeq[p] = 0
	n.ackTime[p] = time.Time{}
}

func (n *Node) ensureReplicatorsLocked() {
	if n.ctx.Err() != nil {
		return
	}
	start := func(p string) {
		if p == n.cfg.ID {
			return
		}
		if _, ok := n.kicks[p]; ok {
			return
		}
		k := make(chan struct{}, 1)
		n.kicks[p] = k
		n.wg.Add(1)
		go n.replicator(p, k)
	}
	for _, p := range n.peers {
		start(p)
	}
	for p := range n.learners {
		start(p)
	}
}

func (n *Node) pushPeersLocked() {
	if ps, ok := n.tr.(PeerSetter); ok {
		cp := make(map[string]string, len(n.addrs))
		for k, v := range n.addrs {
			cp[k] = v
		}
		ps.SetPeers(cp)
	}
}

func decodeMembers(b []byte) map[string]string {
	var m map[string]string
	if err := json.Unmarshal(b, &m); err != nil {
		panic(fmt.Sprintf("raft: corrupt config entry: %v", err))
	}
	return m
}

// refreshConfigLocked recomputes the active voter set from log -> snapshot -> bootstrap.
func (n *Node) refreshConfigLocked() {
	var members map[string]string
	var idx uint64
	for i := len(n.entries) - 1; i >= 0; i-- {
		if n.entries[i].Type == EntryConfig {
			members, idx = decodeMembers(n.entries[i].Data), n.entries[i].Index
			break
		}
	}
	if members == nil {
		members = n.snap.Peers
	}
	if members == nil {
		members = n.boot
	}

	ids := make([]string, 0, len(members))
	for id, a := range members {
		ids = append(ids, id)
		if a != "" {
			n.addrs[id] = a
		}
	}
	sort.Strings(ids)
	n.peers, n.configIndex = ids, idx
	for _, id := range ids {
		delete(n.learners, id)
	}
	n.pushPeersLocked()
	if n.state == Leader {
		for _, id := range ids {
			if _, ok := n.next[id]; !ok {
				n.initPeerLocked(id)
			}
		}
	}
	n.ensureReplicatorsLocked()
}

// configAtLocked is the voter set as of log index at, for embedding in a snapshot. nil means
// "never changed from bootstrap".
func (n *Node) configAtLocked(at uint64) map[string]string {
	for i := len(n.entries) - 1; i >= 0; i-- {
		e := n.entries[i]
		if e.Index <= at && e.Type == EntryConfig {
			return decodeMembers(e.Data)
		}
	}
	if n.snap.Peers != nil {
		out := make(map[string]string, len(n.snap.Peers))
		for k, v := range n.snap.Peers {
			out[k] = v
		}
		return out
	}
	return nil
}
