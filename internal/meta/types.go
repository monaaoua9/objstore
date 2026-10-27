// Package meta is the replicated metadata plane: buckets, object->shard placement, cluster
// membership, plus the repair and garbage queues. Everything goes through the Raft log.
package meta

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrBucketNotFound = errors.New("bucket not found")
	ErrBucketExists   = errors.New("bucket already exists")
	ErrBucketNotEmpty = errors.New("bucket not empty")
	ErrObjectNotFound = errors.New("object not found")
	ErrStalePlacement = errors.New("placement changed concurrently")
	ErrUploadNotFound = errors.New("multipart upload not found")
	ErrInvalidPart    = errors.New("invalid multipart part list")
	// ErrDeleteMarker is what a read of a deleted-in-a-versioned-bucket key reports; it is also an
	// ErrObjectNotFound, but callers that care can tell (S3 answers with x-amz-delete-marker).
	ErrInvalidLifecycle = errors.New("invalid lifecycle rules")
	ErrDeleteMarker     = fmt.Errorf("%w (delete marker)", ErrObjectNotFound)
)

// MPUBucket is the hidden bucket that holds in-flight multipart parts. User bucket names can't start with '~'.
const MPUBucket = "~mpu"

func PartKey(uploadID string, n int) string { return fmt.Sprintf("%s/%05d", uploadID, n) }

type PartRef struct {
	Number  int    `json:"number"`
	Version string `json:"version"` // each part is a normal erasure-coded object version
	Size    int64  `json:"size"`
	ETag    string `json:"etag"`
}

type PartSel struct {
	Number int    `json:"part"`
	ETag   string `json:"etag"`
}

type Upload struct {
	ID          string            `json:"id"`
	Bucket      string            `json:"bucket"`
	Key         string            `json:"key"`
	ContentType string            `json:"content_type,omitempty"`
	UserMeta    map[string]string `json:"user_meta,omitempty"`
	Created     time.Time         `json:"created"`
}

type ShardLoc struct {
	Index int    `json:"index"`
	Node  string `json:"node"`
}

type Object struct {
	Bucket      string            `json:"bucket"`
	Key         string            `json:"key"`
	Version     string            `json:"version"`
	Size        int64             `json:"size"`
	ETag        string            `json:"etag"` // hex sha-256 of the plaintext
	ContentType string            `json:"content_type,omitempty"`
	Created     time.Time         `json:"created"`
	K           int               `json:"k"`
	M           int               `json:"m"`
	ShardSize   int               `json:"shard_size"` // payload bytes per shard in a full stripe
	Stripes     int               `json:"stripes"`
	Shards      []ShardLoc        `json:"shards"` // position == shard index
	UserMeta    map[string]string `json:"user_meta,omitempty"`
	// Parts is set for completed multipart objects; such an object owns no shards itself.
	Parts []PartRef `json:"parts,omitempty"`
	// DeleteMarker entries exist only in versioned buckets: no data, they just hide older versions.
	DeleteMarker bool `json:"delete_marker,omitempty"`
	// Null marks the version s3 calls "null": written while versioning was off or suspended.
	// A bucket holds at most one per key and a suspended write replaces it.
	Null bool `json:"null,omitempty"`
}

// VersionItem is one row of a version listing (current version first, then older ones).
type VersionItem struct {
	Key          string    `json:"key"`
	Version      string    `json:"version"`
	IsLatest     bool      `json:"is_latest"`
	DeleteMarker bool      `json:"delete_marker,omitempty"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag,omitempty"`
	Created      time.Time `json:"created"`
}

// ObjectItem is the slimmed-down listing form.
type ObjectItem struct {
	Key         string    `json:"key"`
	Size        int64     `json:"size"`
	ETag        string    `json:"etag"`
	ContentType string    `json:"content_type,omitempty"`
	Created     time.Time `json:"created"`
}

type Bucket struct {
	Name       string          `json:"name"`
	Created    time.Time       `json:"created"`
	Versioning bool            `json:"versioning,omitempty"`
	Suspended  bool            `json:"suspended,omitempty"`
	Lifecycle  []LifecycleRule `json:"lifecycle,omitempty"`
}

// LifecycleRule is evaluated by the leader's supervisor, never by the FSM (the FSM has no clock).
type LifecycleRule struct {
	ID     string `json:"id"`
	Prefix string `json:"prefix,omitempty"`
	// ExpireDays deletes current versions older than this (a delete marker in a versioned bucket).
	ExpireDays int `json:"expire_days,omitempty"`
	// NoncurrentDays permanently deletes a version this long after a newer one replaced it.
	NoncurrentDays int `json:"noncurrent_days,omitempty"`
	// CleanMarkers removes a delete marker once nothing is left behind it.
	CleanMarkers bool `json:"clean_markers,omitempty"`
	Disabled     bool `json:"disabled,omitempty"`
}

type NodeState string

const (
	NodeUp       NodeState = "up"
	NodeDown     NodeState = "down"
	NodeDraining NodeState = "draining" // still readable, excluded from new placement, shards get moved off
)

type Node struct {
	ID       string    `json:"id"`
	Addr     string    `json:"addr"`
	Zone     string    `json:"zone,omitempty"`
	Weight   int       `json:"weight,omitempty"`
	State    NodeState `json:"state"`
	Since    time.Time `json:"since"` // when State last changed
	Capacity int64     `json:"capacity,omitempty"`
}

type Damage struct {
	Version string    `json:"version"`
	Index   int       `json:"index"`
	Reason  string    `json:"reason"`
	Since   time.Time `json:"since"`
}

type GCItem struct {
	Version string     `json:"version"`
	Shards  []ShardLoc `json:"shards"`
}

type Stats struct {
	Buckets  int   `json:"buckets"`
	Objects  int   `json:"objects"`
	Bytes    int64 `json:"bytes"`
	Damaged  int   `json:"damaged"`
	GCQueued int   `json:"gc_queued"`
	Nodes    int   `json:"nodes"`
}
