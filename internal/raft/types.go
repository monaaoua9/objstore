package raft

import (
	"context"
	"errors"
	"time"
)

type State int

const (
	Follower State = iota
	PreCandidate
	Candidate
	Leader
)

func (s State) String() string {
	return [...]string{"follower", "pre-candidate", "candidate", "leader"}[s]
}

var (
	ErrMembership = errors.New("raft: invalid membership change")
	ErrConfigBusy = errors.New("raft: another membership change is still in flight")
	ErrNotLeader  = errors.New("raft: not leader")
	ErrNoLeader   = errors.New("raft: no known leader")
	ErrStopped    = errors.New("raft: node stopped")
	ErrLostLeader = errors.New("raft: leadership lost before commit")
)

type EntryType uint8

const (
	EntryCommand EntryType = iota
	EntryNoop
	// EntryConfig carries the complete voter set (id -> address) that takes effect the moment the
	// entry is appended, not when it commits. Only one may be in flight at a time.
	EntryConfig
)

type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

// FSM is the replicated state machine. Apply is called serially in log order.
type FSM interface {
	Apply(index uint64, data []byte) []byte
	Snapshot() ([]byte, error)
	Restore(snapshot []byte) error
}

type Snapshot struct {
	Index uint64
	Term  uint64
	// Peers is the voter set in force at Index; nil if the log never changed it from the bootstrap set.
	Peers map[string]string
	Data  []byte
}

type AppendEntriesReq struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendEntriesResp struct {
	Term    uint64
	Success bool
	// on mismatch the follower tells us where to retry instead of us stepping back one at a time
	ConflictIndex uint64
	ConflictTerm  uint64
	LastIndex     uint64
}

type VoteReq struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
	PreVote      bool
}

type VoteResp struct {
	Term    uint64
	Granted bool
}

// InstallSnapshotReq carries one chunk. Snapshot.Data holds only this chunk's bytes; Index, Term and
// Peers are repeated on every chunk so a follower can tell when the leader moved to a newer snapshot.
type InstallSnapshotReq struct {
	Term     uint64
	LeaderID string
	Snapshot Snapshot
	Offset   uint64
	Done     bool
}

type InstallSnapshotResp struct {
	Term uint64
	// Next is the offset the follower wants next; anything other than what the leader just sent
	// means "start (or resume) from here".
	Next uint64
}

// Transport moves RPCs between nodes. Implementations must honour ctx cancellation.
type Transport interface {
	AppendEntries(ctx context.Context, to string, r *AppendEntriesReq) (*AppendEntriesResp, error)
	RequestVote(ctx context.Context, to string, r *VoteReq) (*VoteResp, error)
	InstallSnapshot(ctx context.Context, to string, r *InstallSnapshotReq) (*InstallSnapshotResp, error)
	ReadIndex(ctx context.Context, to string) (uint64, error)
	Forward(ctx context.Context, to string, data []byte) ([]byte, error)
}

type Config struct {
	ID    string
	Peers []string // bootstrap voter ids, including ourselves; ignored once the log carries a config
	// Addrs is the initial id -> address book, copied into config entries so joiners learn it.
	Addrs map[string]string
	// Join starts the node with no voters: it never campaigns until a leader replicates a config
	// that includes it (see AddVoter).
	Join bool

	HeartbeatInterval time.Duration
	ElectionTimeout   time.Duration // randomized in [1x, 2x)

	// snapshot once this many entries have been applied since the last one
	SnapshotThreshold uint64
	// entries kept behind the snapshot so slightly-lagging followers don't need a full copy
	SnapshotTrailing uint64

	MaxBatch int // entries per AppendEntries

	SnapshotChunk int // bytes of snapshot data per InstallSnapshot request
}

func (c *Config) setDefaults() {
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 75 * time.Millisecond
	}
	if c.ElectionTimeout == 0 {
		c.ElectionTimeout = 500 * time.Millisecond
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = 8192
	}
	if c.SnapshotTrailing == 0 {
		c.SnapshotTrailing = 512
	}
	if c.SnapshotChunk == 0 {
		c.SnapshotChunk = 1 << 20
	}
	if c.MaxBatch == 0 {
		c.MaxBatch = 256
	}
}
