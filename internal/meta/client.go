package meta

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// Client is the typed face of the metadata group.
type Client struct {
	B Backend
}

func now() time.Time { return time.Now().UTC().Truncate(time.Millisecond) }

func (c *Client) apply(ctx context.Context, cm cmd) (result, error) {
	cm.At = now()
	b, _ := json.Marshal(cm)
	out, err := c.B.Apply(ctx, b)
	if err != nil {
		return result{}, err
	}
	return decode(out)
}

func (c *Client) ask(ctx context.Context, q query) (result, error) {
	b, _ := json.Marshal(q)
	out, err := c.B.Query(ctx, b)
	if err != nil {
		return result{}, err
	}
	return decode(out)
}

func decode(b []byte) (result, error) {
	var r result
	if err := json.Unmarshal(b, &r); err != nil {
		return r, err
	}
	if r.Code == "" {
		return r, nil
	}
	if e, ok := codeToErr[r.Code]; ok {
		if r.Msg != "" {
			return r, fmt.Errorf("%w: %s", e, r.Msg)
		}
		return r, e
	}
	if r.Code == "GONE" {
		return r, nil // target vanished underneath us; callers treat as success
	}
	return r, fmt.Errorf("meta: %s %s", r.Code, r.Msg)
}

func (c *Client) CreateBucket(ctx context.Context, name string) error {
	_, err := c.apply(ctx, cmd{Op: opCreateBucket, Bucket: name})
	return err
}

func (c *Client) DeleteBucket(ctx context.Context, name string) error {
	_, err := c.apply(ctx, cmd{Op: opDeleteBucket, Bucket: name})
	return err
}

func (c *Client) ListBuckets(ctx context.Context) ([]Bucket, error) {
	r, err := c.ask(ctx, query{Op: qBuckets})
	return r.Buckets, err
}

func (c *Client) CommitObject(ctx context.Context, o *Object, degraded []int) error {
	_, err := c.apply(ctx, cmd{Op: opCommit, Obj: o, Degraded: degraded})
	return err
}

// DeleteObject removes the key, or in a versioned bucket hides it behind a delete marker, which is
// returned (nil otherwise).
func (c *Client) DeleteObject(ctx context.Context, bucket, key string) (*Object, error) {
	r, err := c.apply(ctx, cmd{Op: opDeleteObject, Bucket: bucket, Key: key, Version: newVersionID()})
	return r.Object, err
}

// DeleteVersion permanently removes one version or delete marker.
func (c *Client) DeleteVersion(ctx context.Context, bucket, key, version string) error {
	_, err := c.apply(ctx, cmd{Op: opDeleteVer, Bucket: bucket, Key: key, Version: version})
	return err
}

// SetLifecycle replaces the bucket's rules (nil clears them).
func (c *Client) SetLifecycle(ctx context.Context, bucket string, rules []LifecycleRule) error {
	_, err := c.apply(ctx, cmd{Op: opLifecycle, Bucket: bucket, Rules: rules})
	return err
}

func (c *Client) EnableVersioning(ctx context.Context, bucket string) error {
	_, err := c.apply(ctx, cmd{Op: opVersioning, Bucket: bucket})
	return err
}

func (c *Client) SuspendVersioning(ctx context.Context, bucket string) error {
	_, err := c.apply(ctx, cmd{Op: opVersioning, Bucket: bucket, Suspend: true})
	return err
}

func (c *Client) GetObject(ctx context.Context, bucket, key string) (*Object, error) {
	r, err := c.ask(ctx, query{Op: qGetObject, Bucket: bucket, Key: key})
	if err == nil && r.Object != nil && r.Object.DeleteMarker {
		return r.Object, ErrDeleteMarker
	}
	return r.Object, err
}

// GetObjectVersion fetches a specific version; a delete marker comes back with ErrDeleteMarker.
func (c *Client) GetObjectVersion(ctx context.Context, bucket, key, version string) (*Object, error) {
	r, err := c.ask(ctx, query{Op: qGetKeyVer, Bucket: bucket, Key: key, Version: version})
	if err == nil && r.Object != nil && r.Object.DeleteMarker {
		return r.Object, ErrDeleteMarker
	}
	return r.Object, err
}

// ListVersions pages through every version and delete marker. Pass the previous page's
// nextKey/nextVersion as afterKey/afterVersion.
func (c *Client) ListVersions(ctx context.Context, bucket, prefix, afterKey, afterVersion string, limit int) (items []VersionItem, nextKey, nextVersion string, err error) {
	r, err := c.ask(ctx, query{Op: qVersions, Bucket: bucket, Prefix: prefix, After: afterKey, Version: afterVersion, Limit: limit})
	return r.VItems, r.Next, r.NextVer, err
}

func (c *Client) GetVersion(ctx context.Context, version string) (*Object, error) {
	r, err := c.ask(ctx, query{Op: qGetVersion, Version: version})
	return r.Object, err
}

// ListObjects returns up to limit items with key > after. next is non-empty if more remain.
func (c *Client) ListObjects(ctx context.Context, bucket, prefix, after string, limit int) (items []ObjectItem, next string, err error) {
	r, err := c.ask(ctx, query{Op: qList, Bucket: bucket, Prefix: prefix, After: after, Limit: limit})
	return r.Items, r.Next, err
}

// Scan pages through every object (full placement info) in bucket/key order.
func (c *Client) Scan(ctx context.Context, cursor string, limit int) ([]*Object, string, error) {
	r, err := c.ask(ctx, query{Op: qScan, After: cursor, Limit: limit})
	return r.Objects, r.Next, err
}

func (c *Client) SetNode(ctx context.Context, n Node) error {
	_, err := c.apply(ctx, cmd{Op: opSetNode, Node: &n})
	return err
}

func (c *Client) SetNodeState(ctx context.Context, id string, st NodeState) error {
	_, err := c.apply(ctx, cmd{Op: opNodeState, NodeID: id, State: st})
	return err
}

func (c *Client) RemoveNode(ctx context.Context, id string) error {
	_, err := c.apply(ctx, cmd{Op: opRemoveNode, NodeID: id})
	return err
}

func (c *Client) Nodes(ctx context.Context) ([]Node, error) {
	r, err := c.ask(ctx, query{Op: qNodes})
	return r.Nodes, err
}

func (c *Client) MarkDamaged(ctx context.Context, version string, idx int, reason string) error {
	_, err := c.apply(ctx, cmd{Op: opDamaged, Version: version, Index: idx, Reason: reason})
	return err
}

// MarkDamagedOn is MarkDamaged from the holder's point of view: ignored if node no longer owns that shard.
func (c *Client) MarkDamagedOn(ctx context.Context, version string, idx int, node, reason string) error {
	_, err := c.apply(ctx, cmd{Op: opDamaged, Version: version, Index: idx, From: node, Reason: reason})
	return err
}

// Relocate repoints shard idx from one node to another; clear also drops its damage record.
func (c *Client) Relocate(ctx context.Context, version string, idx int, from, to string, clear bool) error {
	_, err := c.apply(ctx, cmd{Op: opRelocate, Version: version, Index: idx, From: from, To: to, Clear: clear})
	return err
}

func (c *Client) Damaged(ctx context.Context, limit int) ([]Damage, error) {
	r, err := c.ask(ctx, query{Op: qDamaged, Limit: limit})
	return r.Damaged, err
}

func (c *Client) GCQueue(ctx context.Context, limit int) ([]GCItem, error) {
	r, err := c.ask(ctx, query{Op: qGC, Limit: limit})
	return r.GC, err
}

func (c *Client) GCDone(ctx context.Context, version string) error {
	_, err := c.apply(ctx, cmd{Op: opGCDone, Version: version})
	return err
}

// Placements returns the live objects among the given version IDs, keyed by version.
func (c *Client) Placements(ctx context.Context, versions []string) (map[string]*Object, error) {
	r, err := c.ask(ctx, query{Op: qPlacement, Versions: versions})
	if err != nil {
		return nil, err
	}
	m := make(map[string]*Object, len(r.Objects))
	for _, o := range r.Objects {
		m[o.Version] = o
	}
	return m, nil
}

func (c *Client) Stats(ctx context.Context) (Stats, error) {
	r, err := c.ask(ctx, query{Op: qStats})
	if r.Stats == nil {
		return Stats{}, err
	}
	return *r.Stats, err
}

func (c *Client) CreateUpload(ctx context.Context, u Upload) error {
	_, err := c.apply(ctx, cmd{Op: opMPUCreate, Upload: &u})
	return err
}

func (c *Client) GetUpload(ctx context.Context, id string) (*Upload, error) {
	r, err := c.ask(ctx, query{Op: qUpload, Version: id})
	return r.Upload, err
}

func (c *Client) ListUploads(ctx context.Context) ([]Upload, error) {
	r, err := c.ask(ctx, query{Op: qUploads})
	return r.Uploads, err
}

func (c *Client) AbortUpload(ctx context.Context, id string) error {
	_, err := c.apply(ctx, cmd{Op: opMPUAbort, Version: id})
	return err
}

// CompleteUpload stitches the chosen parts into one object atomically and returns the manifest.
func (c *Client) CompleteUpload(ctx context.Context, id string, parts []PartSel) (*Object, error) {
	r, err := c.apply(ctx, cmd{Op: opMPUComplete, Version: id, Sel: parts})
	return r.Object, err
}

func newVersionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
