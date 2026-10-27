package meta

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// command and query ops. kept as strings so the log stays greppable.
const (
	opCreateBucket = "create_bucket"
	opDeleteBucket = "delete_bucket"
	opCommit       = "commit_object"
	opDeleteObject = "delete_object"
	opSetNode      = "set_node"
	opNodeState    = "node_state"
	opRemoveNode   = "remove_node"
	opDamaged      = "mark_damaged"
	opRelocate     = "relocate"
	opGCDone       = "gc_done"
	opMPUCreate    = "mpu_create"
	opMPUAbort     = "mpu_abort"
	opMPUComplete  = "mpu_complete"
	opDeleteVer    = "delete_version"
	opVersioning   = "set_versioning"
	opLifecycle    = "set_lifecycle"

	qGetObject  = "get_object"
	qGetVersion = "get_version"
	qList       = "list_objects"
	qBuckets    = "list_buckets"
	qNodes      = "nodes"
	qDamaged    = "damaged"
	qGC         = "gc"
	qPlacement  = "placement"
	qScan       = "scan"
	qStats      = "stats"
	qUpload     = "upload"
	qUploads    = "uploads"
	qGetKeyVer  = "get_key_version"
	qVersions   = "list_versions"
)

type cmd struct {
	Op       string          `json:"op"`
	Bucket   string          `json:"bucket,omitempty"`
	Key      string          `json:"key,omitempty"`
	Obj      *Object         `json:"obj,omitempty"`
	Degraded []int           `json:"degraded,omitempty"`
	Node     *Node           `json:"node,omitempty"`
	NodeID   string          `json:"node_id,omitempty"`
	State    NodeState       `json:"state,omitempty"`
	Version  string          `json:"version,omitempty"`
	Index    int             `json:"index,omitempty"`
	From     string          `json:"from,omitempty"`
	To       string          `json:"to,omitempty"`
	Clear    bool            `json:"clear,omitempty"`
	Reason   string          `json:"reason,omitempty"`
	At       time.Time       `json:"at"`
	Upload   *Upload         `json:"upload,omitempty"`
	Sel      []PartSel       `json:"sel,omitempty"`
	Rules    []LifecycleRule `json:"rules,omitempty"`
}

type query struct {
	Op       string   `json:"op"`
	Bucket   string   `json:"bucket,omitempty"`
	Key      string   `json:"key,omitempty"`
	Version  string   `json:"version,omitempty"`
	Prefix   string   `json:"prefix,omitempty"`
	After    string   `json:"after,omitempty"`
	Limit    int      `json:"limit,omitempty"`
	Versions []string `json:"versions,omitempty"`
}

type result struct {
	Code string `json:"code,omitempty"`
	Msg  string `json:"msg,omitempty"`

	Object   *Object       `json:"object,omitempty"`
	Objects  []*Object     `json:"objects,omitempty"`
	Items    []ObjectItem  `json:"items,omitempty"`
	Next     string        `json:"next,omitempty"`
	NextVer  string        `json:"next_version,omitempty"`
	VItems   []VersionItem `json:"version_items,omitempty"`
	Buckets  []Bucket      `json:"buckets,omitempty"`
	Nodes    []Node        `json:"nodes,omitempty"`
	Damaged  []Damage      `json:"damaged,omitempty"`
	GC       []GCItem      `json:"gc,omitempty"`
	Versions []string      `json:"versions,omitempty"`
	Stats    *Stats        `json:"stats,omitempty"`
	Upload   *Upload       `json:"upload,omitempty"`
	Uploads  []Upload      `json:"uploads,omitempty"`
}

var codeToErr = map[string]error{
	"BUCKET_NOT_FOUND":  ErrBucketNotFound,
	"BUCKET_EXISTS":     ErrBucketExists,
	"BUCKET_NOT_EMPTY":  ErrBucketNotEmpty,
	"OBJECT_NOT_FOUND":  ErrObjectNotFound,
	"STALE_PLACEMENT":   ErrStalePlacement,
	"UPLOAD_NOT_FOUND":  ErrUploadNotFound,
	"INVALID_PART":      ErrInvalidPart,
	"INVALID_LIFECYCLE": ErrInvalidLifecycle,
}

func errResult(code string) result { return result{Code: code} }

type bucketState struct {
	Bucket
	objs map[string]*Object
	keys []string // sorted
	// hist holds the older versions of a key, newest first; objs[key] is always the current one
	hist map[string][]*Object
}

func (b *bucketState) pushHist(o *Object) {
	if b.hist == nil {
		b.hist = map[string][]*Object{}
	}
	b.hist[o.Key] = append([]*Object{o}, b.hist[o.Key]...)
}

// supersede is what a new write does to the object it replaces
func (f *FSM) supersede(b *bucketState, old *Object) {
	if b.Versioning {
		b.pushHist(old)
	} else {
		f.retire(old)
	}
}

func (b *bucketState) put(o *Object) {
	if _, ok := b.objs[o.Key]; !ok {
		i := sort.SearchStrings(b.keys, o.Key)
		b.keys = append(b.keys, "")
		copy(b.keys[i+1:], b.keys[i:])
		b.keys[i] = o.Key
	}
	b.objs[o.Key] = o
}

func (b *bucketState) del(key string) {
	delete(b.objs, key)
	i := sort.SearchStrings(b.keys, key)
	if i < len(b.keys) && b.keys[i] == key {
		b.keys = append(b.keys[:i], b.keys[i+1:]...)
	}
}

type FSM struct {
	mu       sync.RWMutex
	buckets  map[string]*bucketState
	versions map[string]*Object
	nodes    map[string]*Node
	damaged  map[string]map[int]Damage
	gc       map[string][]ShardLoc
	uploads  map[string]*Upload
	strs     map[string]string // intern pool: node ids, content types and bucket names repeat across millions of objects
}

const maxInterned = 8192

func (f *FSM) intern(s string) string {
	if s == "" {
		return s
	}
	if v, ok := f.strs[s]; ok {
		return v
	}
	if len(f.strs) < maxInterned {
		f.strs[s] = s
	}
	return s
}

func (f *FSM) internObj(o *Object) {
	o.Bucket = f.intern(o.Bucket)
	o.ContentType = f.intern(o.ContentType)
	for i := range o.Shards {
		o.Shards[i].Node = f.intern(o.Shards[i].Node)
	}
}

func NewFSM() *FSM {
	f := &FSM{}
	f.reset()
	return f
}

func (f *FSM) reset() {
	f.buckets = map[string]*bucketState{}
	f.versions = map[string]*Object{}
	f.nodes = map[string]*Node{}
	f.damaged = map[string]map[int]Damage{}
	f.gc = map[string][]ShardLoc{}
	f.uploads = map[string]*Upload{}
	f.strs = map[string]string{}
}

// Apply implements raft.FSM. Must be deterministic: no clocks, no randomness.
func (f *FSM) Apply(_ uint64, data []byte) []byte {
	var c cmd
	if err := json.Unmarshal(data, &c); err != nil {
		return mustJSON(result{Code: "BAD_COMMAND", Msg: err.Error()})
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return mustJSON(f.apply(&c))
}

func (f *FSM) apply(c *cmd) result {
	switch c.Op {
	case opCreateBucket:
		if _, ok := f.buckets[c.Bucket]; ok {
			return errResult("BUCKET_EXISTS")
		}
		f.buckets[c.Bucket] = &bucketState{Bucket: Bucket{Name: c.Bucket, Created: c.At}, objs: map[string]*Object{}}

	case opDeleteBucket:
		b, ok := f.buckets[c.Bucket]
		if !ok {
			return errResult("BUCKET_NOT_FOUND")
		}
		if len(b.objs) > 0 {
			return errResult("BUCKET_NOT_EMPTY")
		}
		delete(f.buckets, c.Bucket)

	case opCommit:
		o := c.Obj
		b, ok := f.buckets[o.Bucket]
		if !ok && o.Bucket == MPUBucket {
			b = &bucketState{Bucket: Bucket{Name: MPUBucket, Created: c.At}, objs: map[string]*Object{}}
			f.buckets[MPUBucket] = b
			ok = true
		}
		if !ok {
			return errResult("BUCKET_NOT_FOUND")
		}
		if old, ok := b.objs[o.Key]; ok {
			f.supersede(b, old)
		}
		f.internObj(o)
		b.put(o)
		f.versions[o.Version] = o
		for _, i := range c.Degraded {
			f.markDamaged(o.Version, i, "write-degraded", c.At)
		}

	case opDeleteObject:
		b, ok := f.buckets[c.Bucket]
		if !ok {
			return errResult("BUCKET_NOT_FOUND")
		}
		old, ok := b.objs[c.Key]
		if b.Versioning {
			// deleting in a versioned bucket only hides the key behind a marker
			m := &Object{Bucket: c.Bucket, Key: c.Key, Version: c.Version, Created: c.At, DeleteMarker: true}
			if ok {
				b.pushHist(old)
			}
			b.put(m)
			return result{Object: m}
		}
		if !ok {
			return errResult("OBJECT_NOT_FOUND")
		}
		b.del(c.Key)
		f.retire(old)

	case opDeleteVer:
		b, ok := f.buckets[c.Bucket]
		if !ok {
			return errResult("BUCKET_NOT_FOUND")
		}
		if !f.deleteVersion(b, c.Key, c.Version) {
			return errResult("OBJECT_NOT_FOUND")
		}

	case opLifecycle:
		b, ok := f.buckets[c.Bucket]
		if !ok {
			return errResult("BUCKET_NOT_FOUND")
		}
		if msg := checkRules(c.Rules); msg != "" {
			return result{Code: "INVALID_LIFECYCLE", Msg: msg}
		}
		b.Lifecycle = c.Rules

	case opVersioning:
		b, ok := f.buckets[c.Bucket]
		if !ok {
			return errResult("BUCKET_NOT_FOUND")
		}
		b.Versioning = true

	case opSetNode:
		n := *c.Node
		if prev, ok := f.nodes[n.ID]; ok {
			// re-registration after restart keeps the operator-set state (e.g. draining)
			if n.State == "" {
				n.State = prev.State
				n.Since = prev.Since
			}
		}
		if n.State == "" {
			n.State = NodeUp
			n.Since = c.At
		}
		f.nodes[n.ID] = &n

	case opNodeState:
		n, ok := f.nodes[c.NodeID]
		if !ok {
			return result{Code: "NODE_NOT_FOUND"}
		}
		if n.State != c.State {
			n.State, n.Since = c.State, c.At
		}

	case opRemoveNode:
		delete(f.nodes, c.NodeID)

	case opDamaged:
		o, ok := f.versions[c.Version]
		if !ok || c.Index < 0 || c.Index >= len(o.Shards) {
			return result{Code: "GONE"}
		}
		// a node reporting its own shard must still be the recorded holder, else it's a stale copy
		if c.From != "" && o.Shards[c.Index].Node != c.From {
			return result{Code: "GONE"}
		}
		f.markDamaged(c.Version, c.Index, c.Reason, c.At)

	case opRelocate:
		o, ok := f.versions[c.Version]
		if !ok || c.Index < 0 || c.Index >= len(o.Shards) {
			return result{Code: "GONE"}
		}
		if o.Shards[c.Index].Node != c.From {
			return errResult("STALE_PLACEMENT")
		}
		// copy-on-write: readers holding the old slice (via snapshot encode) stay consistent
		sh := append([]ShardLoc(nil), o.Shards...)
		sh[c.Index].Node = f.intern(c.To)
		o.Shards = sh
		if c.Clear {
			f.clearDamage(c.Version, c.Index)
		}

	case opGCDone:
		delete(f.gc, c.Version)

	case opMPUCreate:
		if _, ok := f.buckets[c.Upload.Bucket]; !ok {
			return errResult("BUCKET_NOT_FOUND")
		}
		u := *c.Upload
		f.uploads[u.ID] = &u

	case opMPUAbort:
		if _, ok := f.uploads[c.Version]; !ok {
			return errResult("UPLOAD_NOT_FOUND")
		}
		f.dropParts(c.Version, nil)
		delete(f.uploads, c.Version)

	case opMPUComplete:
		return f.completeUpload(c)

	default:
		return result{Code: "BAD_COMMAND", Msg: "unknown op " + c.Op}
	}
	return result{}
}

// deleteVersion permanently removes one version (or marker) of key. If it was the current one the
// next-newest takes over.
func (f *FSM) deleteVersion(b *bucketState, key, version string) bool {
	cur, ok := b.objs[key]
	if !ok {
		return false
	}
	if cur.Version == version {
		if h := b.hist[key]; len(h) > 0 {
			b.objs[key] = h[0]
			if len(h) == 1 {
				delete(b.hist, key)
			} else {
				b.hist[key] = h[1:]
			}
		} else {
			b.del(key)
		}
		f.retire(cur)
		return true
	}
	h := b.hist[key]
	for i, o := range h {
		if o.Version == version {
			rest := append(append([]*Object(nil), h[:i]...), h[i+1:]...)
			if len(rest) == 0 {
				delete(b.hist, key)
			} else {
				b.hist[key] = rest
			}
			f.retire(o)
			return true
		}
	}
	return false
}

func (f *FSM) retire(o *Object) {
	delete(f.versions, o.Version)
	delete(f.damaged, o.Version)
	if len(o.Shards) > 0 {
		f.gc[o.Version] = append([]ShardLoc(nil), o.Shards...)
	}
	for _, p := range o.Parts {
		if po := f.versions[p.Version]; po != nil {
			f.retire(po)
		}
	}
}

// dropParts removes every staged part of an upload from the hidden bucket, retiring those not in keep.
func (f *FSM) dropParts(id string, keep map[string]bool) {
	hb := f.buckets[MPUBucket]
	if hb == nil {
		return
	}
	prefix := id + "/"
	var doomed []string
	for i := sort.SearchStrings(hb.keys, prefix); i < len(hb.keys) && strings.HasPrefix(hb.keys[i], prefix); i++ {
		doomed = append(doomed, hb.keys[i])
	}
	for _, k := range doomed {
		o := hb.objs[k]
		hb.del(k)
		if !keep[k] {
			f.retire(o)
		}
	}
}

func (f *FSM) completeUpload(c *cmd) result {
	up := f.uploads[c.Version]
	if up == nil {
		return errResult("UPLOAD_NOT_FOUND")
	}
	b := f.buckets[up.Bucket]
	if b == nil {
		return errResult("BUCKET_NOT_FOUND")
	}
	hb := f.buckets[MPUBucket]
	if hb == nil || len(c.Sel) == 0 {
		return result{Code: "INVALID_PART", Msg: "no parts"}
	}
	keep := map[string]bool{}
	var refs []PartRef
	var total int64
	h := sha256.New()
	last := 0
	for _, sel := range c.Sel {
		if sel.Number <= last {
			return result{Code: "INVALID_PART", Msg: "parts must be strictly ascending"}
		}
		last = sel.Number
		key := PartKey(up.ID, sel.Number)
		po := hb.objs[key]
		if po == nil || po.ETag != sel.ETag {
			return result{Code: "INVALID_PART", Msg: fmt.Sprintf("part %d missing or etag mismatch", sel.Number)}
		}
		keep[key] = true
		refs = append(refs, PartRef{Number: sel.Number, Version: po.Version, Size: po.Size, ETag: po.ETag})
		total += po.Size
		h.Write([]byte(po.ETag))
	}
	first := hb.objs[PartKey(up.ID, c.Sel[0].Number)]
	m := &Object{
		Bucket: up.Bucket, Key: up.Key, Version: up.ID, Size: total,
		ETag:        fmt.Sprintf("mp-%x-%d", h.Sum(nil), len(refs)),
		ContentType: up.ContentType, UserMeta: up.UserMeta, Created: c.At,
		K: first.K, M: first.M, ShardSize: first.ShardSize, Parts: refs,
	}
	f.dropParts(up.ID, keep)
	if old, ok := b.objs[up.Key]; ok {
		f.supersede(b, old)
	}
	b.put(m)
	f.versions[m.Version] = m
	delete(f.uploads, up.ID)
	return result{Object: m}
}

func (f *FSM) markDamaged(v string, idx int, reason string, at time.Time) {
	m := f.damaged[v]
	if m == nil {
		m = map[int]Damage{}
		f.damaged[v] = m
	}
	if _, ok := m[idx]; !ok {
		m[idx] = Damage{Version: v, Index: idx, Reason: reason, Since: at}
	}
}

func (f *FSM) clearDamage(v string, idx int) {
	if m := f.damaged[v]; m != nil {
		delete(m, idx)
		if len(m) == 0 {
			delete(f.damaged, v)
		}
	}
}

// Query runs a read against the local state. Callers are responsible for the read barrier.
func (f *FSM) Query(data []byte) []byte {
	var q query
	if err := json.Unmarshal(data, &q); err != nil {
		return mustJSON(result{Code: "BAD_QUERY", Msg: err.Error()})
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return mustJSON(f.query(&q))
}

func (f *FSM) query(q *query) result {
	switch q.Op {
	case qGetObject:
		b, ok := f.buckets[q.Bucket]
		if !ok {
			return errResult("BUCKET_NOT_FOUND")
		}
		o, ok := b.objs[q.Key]
		if !ok {
			return errResult("OBJECT_NOT_FOUND")
		}
		return result{Object: o}

	case qGetVersion:
		o, ok := f.versions[q.Version]
		if !ok {
			return errResult("OBJECT_NOT_FOUND")
		}
		return result{Object: o}

	case qList:
		b, ok := f.buckets[q.Bucket]
		if !ok {
			return errResult("BUCKET_NOT_FOUND")
		}
		limit := clampLimit(q.Limit, 1000)
		start := sort.SearchStrings(b.keys, q.Prefix)
		if q.After != "" {
			// strictly after the cursor
			if j := sort.SearchStrings(b.keys, q.After); j < len(b.keys) && b.keys[j] == q.After {
				j++
				start = max(start, j)
			} else {
				start = max(start, j)
			}
		}
		var res result
		for i := start; i < len(b.keys); i++ {
			k := b.keys[i]
			if !strings.HasPrefix(k, q.Prefix) {
				break
			}
			o := b.objs[k]
			if o.DeleteMarker {
				continue
			}
			if len(res.Items) == limit {
				res.Next = res.Items[len(res.Items)-1].Key
				break
			}
			res.Items = append(res.Items, ObjectItem{k, o.Size, o.ETag, o.ContentType, o.Created})
		}
		return res

	case qGetKeyVer:
		b, ok := f.buckets[q.Bucket]
		if !ok {
			return errResult("BUCKET_NOT_FOUND")
		}
		if o := b.objs[q.Key]; o != nil && o.Version == q.Version {
			return result{Object: o}
		}
		for _, o := range b.hist[q.Key] {
			if o.Version == q.Version {
				return result{Object: o}
			}
		}
		return errResult("OBJECT_NOT_FOUND")

	case qVersions:
		b, ok := f.buckets[q.Bucket]
		if !ok {
			return errResult("BUCKET_NOT_FOUND")
		}
		limit := clampLimit(q.Limit, 1000)
		var res result
		// After is the key of the last row of the previous page and Version its version id
		for i := sort.SearchStrings(b.keys, max(q.Prefix, q.After)); i < len(b.keys); i++ {
			k := b.keys[i]
			if !strings.HasPrefix(k, q.Prefix) {
				break
			}
			seq := append([]*Object{b.objs[k]}, b.hist[k]...)
			from := 0
			if k == q.After && q.Version != "" {
				for j, o := range seq {
					if o.Version == q.Version {
						from = j + 1
						break
					}
				}
			} else if k == q.After {
				continue
			}
			for j := from; j < len(seq); j++ {
				if len(res.VItems) == limit {
					last := res.VItems[len(res.VItems)-1]
					res.Next, res.NextVer = last.Key, last.Version
					return res
				}
				o := seq[j]
				res.VItems = append(res.VItems, VersionItem{k, o.Version, j == 0, o.DeleteMarker, o.Size, o.ETag, o.Created})
			}
		}
		return res

	case qBuckets:
		out := make([]Bucket, 0, len(f.buckets))
		for _, b := range f.buckets {
			if b.Name != MPUBucket {
				out = append(out, b.Bucket)
			}
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
		return result{Buckets: out}

	case qNodes:
		out := make([]Node, 0, len(f.nodes))
		for _, n := range f.nodes {
			out = append(out, *n)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
		return result{Nodes: out}

	case qDamaged:
		limit := clampLimit(q.Limit, 10000)
		var out []Damage
		for _, m := range f.damaged {
			for _, d := range m {
				out = append(out, d)
			}
		}
		sort.Slice(out, func(i, j int) bool {
			if !out[i].Since.Equal(out[j].Since) {
				return out[i].Since.Before(out[j].Since)
			}
			if out[i].Version != out[j].Version {
				return out[i].Version < out[j].Version
			}
			return out[i].Index < out[j].Index
		})
		if len(out) > limit {
			out = out[:limit]
		}
		return result{Damaged: out}

	case qGC:
		limit := clampLimit(q.Limit, 1000)
		vs := make([]string, 0, len(f.gc))
		for v := range f.gc {
			vs = append(vs, v)
		}
		sort.Strings(vs)
		if len(vs) > limit {
			vs = vs[:limit]
		}
		out := make([]GCItem, len(vs))
		for i, v := range vs {
			out[i] = GCItem{v, f.gc[v]}
		}
		return result{GC: out}

	case qPlacement:
		// live versions only; anything else (retired, never committed) is simply absent
		var res result
		for _, v := range q.Versions {
			if o, ok := f.versions[v]; ok {
				res.Objects = append(res.Objects, o)
			}
		}
		return res

	case qScan:
		// cursor is "bucket\x00key"; walk buckets in name order
		limit := clampLimit(q.Limit, 500)
		names := make([]string, 0, len(f.buckets))
		for n := range f.buckets {
			names = append(names, n)
		}
		sort.Strings(names)
		cb, ck, _ := strings.Cut(q.After, "\x00")
		var res result
		for _, name := range names {
			if q.After != "" && name < cb {
				continue
			}
			b := f.buckets[name]
			i := 0
			if q.After != "" && name == cb {
				i = sort.SearchStrings(b.keys, ck)
				if i < len(b.keys) && b.keys[i] == ck {
					i++
				}
			}
			for ; i < len(b.keys); i++ {
				if len(res.Objects) >= limit {
					res.Next = b.Name + "\x00" + b.keys[i-1]
					return res
				}
				k := b.keys[i]
				group := append([]*Object{b.objs[k]}, b.hist[k]...)
				for _, o := range group {
					if o.DeleteMarker {
						continue
					}
					res.Objects = append(res.Objects, o)
					// a manifest owns no shards; the audit needs its parts as first-class objects
					for _, p := range o.Parts {
						if po := f.versions[p.Version]; po != nil {
							res.Objects = append(res.Objects, po)
						}
					}
				}
			}
		}
		return res

	case qUpload:
		u := f.uploads[q.Version]
		if u == nil {
			return errResult("UPLOAD_NOT_FOUND")
		}
		return result{Upload: u}

	case qUploads:
		var out []Upload
		for _, u := range f.uploads {
			out = append(out, *u)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
		return result{Uploads: out}

	case qStats:
		s := &Stats{Buckets: len(f.buckets), Objects: len(f.versions), GCQueued: len(f.gc), Nodes: len(f.nodes)}
		for _, o := range f.versions {
			s.Bytes += o.Size
		}
		for _, m := range f.damaged {
			s.Damaged += len(m)
		}
		return result{Stats: s}
	}
	return result{Code: "BAD_QUERY", Msg: "unknown op " + q.Op}
}

func clampLimit(n, def int) int {
	if n <= 0 || n > def {
		return def
	}
	return n
}

type snapshot struct {
	Buckets []Bucket              `json:"buckets"`
	Objects []*Object             `json:"objects"`
	Nodes   []*Node               `json:"nodes"`
	Damaged []Damage              `json:"damaged"`
	GC      map[string][]ShardLoc `json:"gc"`
	Parts   []*Object             `json:"parts,omitempty"`
	Uploads []*Upload             `json:"uploads,omitempty"`
	Hist    []*Object             `json:"hist,omitempty"` // older versions, per key newest first
}

func (f *FSM) Snapshot() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s := snapshot{GC: f.gc}
	for _, b := range f.buckets {
		s.Buckets = append(s.Buckets, b.Bucket)
		for _, k := range b.keys {
			o := b.objs[k]
			s.Objects = append(s.Objects, o)
			for _, p := range o.Parts {
				if po := f.versions[p.Version]; po != nil {
					s.Parts = append(s.Parts, po)
				}
			}
			for _, h := range b.hist[k] {
				s.Hist = append(s.Hist, h)
				for _, p := range h.Parts {
					if po := f.versions[p.Version]; po != nil {
						s.Parts = append(s.Parts, po)
					}
				}
			}
		}
	}
	for _, n := range f.nodes {
		s.Nodes = append(s.Nodes, n)
	}
	for _, u := range f.uploads {
		s.Uploads = append(s.Uploads, u)
	}
	for _, m := range f.damaged {
		for _, d := range m {
			s.Damaged = append(s.Damaged, d)
		}
	}
	// gzip: the JSON is extremely repetitive (node ids, field names), ~10x smaller on disk and on the wire
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if err := json.NewEncoder(zw).Encode(s); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (f *FSM) Restore(data []byte) error {
	if len(data) > 2 && data[0] == 0x1f && data[1] == 0x8b {
		zr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("meta: bad snapshot: %w", err)
		}
		if data, err = io.ReadAll(zr); err != nil {
			return fmt.Errorf("meta: bad snapshot: %w", err)
		}
	}
	var s snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("meta: bad snapshot: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reset()
	for _, b := range s.Buckets {
		f.buckets[b.Name] = &bucketState{Bucket: b, objs: map[string]*Object{}}
	}
	for _, o := range s.Objects {
		if f.buckets[o.Bucket] == nil { // hidden mpu bucket carries staged parts
			f.buckets[o.Bucket] = &bucketState{Bucket: Bucket{Name: o.Bucket}, objs: map[string]*Object{}}
		}
		f.internObj(o)
		f.buckets[o.Bucket].put(o)
		if !o.DeleteMarker {
			f.versions[o.Version] = o
		}
	}
	for _, h := range s.Hist {
		f.internObj(h)
		b := f.buckets[h.Bucket]
		if b == nil {
			continue
		}
		if b.hist == nil {
			b.hist = map[string][]*Object{}
		}
		b.hist[h.Key] = append(b.hist[h.Key], h) // snapshot order is newest first, so appending keeps it
		if !h.DeleteMarker {
			f.versions[h.Version] = h
		}
	}
	for _, po := range s.Parts {
		f.internObj(po)
		f.versions[po.Version] = po
	}
	for _, u := range s.Uploads {
		f.uploads[u.ID] = u
	}
	for _, n := range s.Nodes {
		f.nodes[n.ID] = n
	}
	for _, d := range s.Damaged {
		f.markDamaged(d.Version, d.Index, d.Reason, d.Since)
	}
	if s.GC != nil {
		f.gc = s.GC
	}
	return nil
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func checkRules(rs []LifecycleRule) string {
	if len(rs) > 100 {
		return "at most 100 rules"
	}
	ids := map[string]bool{}
	for _, r := range rs {
		switch {
		case r.ID == "" || len(r.ID) > 255:
			return "every rule needs an id of up to 255 characters"
		case ids[r.ID]:
			return "duplicate rule id " + r.ID
		case r.ExpireDays < 0 || r.NoncurrentDays < 0:
			return "days cannot be negative"
		case r.ExpireDays == 0 && r.NoncurrentDays == 0 && !r.CleanMarkers:
			return "rule " + r.ID + " does nothing"
		case len(r.Prefix) > 1024:
			return "prefix too long"
		}
		ids[r.ID] = true
	}
	return ""
}
