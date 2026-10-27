package s3api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/monaaoua9/objstore/internal/sigv4"
)

// Key is one S3 credential. Buckets empty (or containing "*") means every bucket.
type Key struct {
	Secret   string
	Buckets  []string
	ReadOnly bool
}

func (k Key) allows(bucket string, write bool) bool {
	if write && k.ReadOnly {
		return false
	}
	if len(k.Buckets) == 0 {
		return true
	}
	for _, b := range k.Buckets {
		if b == "*" || b == bucket {
			return true
		}
	}
	return false
}

type keySet struct {
	creds sigv4.Credentials
	keys  map[string]Key
}

// SetKeys swaps the credential set atomically; requests already past authentication finish with the old one.
func (s *Server) SetKeys(keys map[string]Key) {
	ks := &keySet{creds: sigv4.Credentials{}, keys: make(map[string]Key, len(keys))}
	for ak, k := range keys {
		ks.creds[ak] = k.Secret
		ks.keys[ak] = k
	}
	s.keys.Store(ks)
}

// sigv4Access digs the access key back out of an already verified request (copy-source checks).
func sigv4Access(r *http.Request) string {
	return r.Header.Get(accessKeyHeader)
}

// accessKeyHeader is set by ServeHTTP after verification and stripped from client input first.
const accessKeyHeader = "X-Objstore-Access-Key"

type keyFileEntry struct {
	AccessKey string   `json:"access_key"`
	SecretKey string   `json:"secret_key"`
	Buckets   []string `json:"buckets,omitempty"`
	ReadOnly  bool     `json:"read_only,omitempty"`
}

// LoadKeys reads a JSON array of {access_key, secret_key, buckets?, read_only?}.
func LoadKeys(path string) (map[string]Key, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var entries []keyFileEntry
	if err := json.Unmarshal(b, &entries); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	out := make(map[string]Key, len(entries))
	for i, e := range entries {
		if e.AccessKey == "" || len(e.SecretKey) < 8 {
			return nil, fmt.Errorf("%s: entry %d needs an access_key and a secret_key of at least 8 characters", path, i)
		}
		if _, dup := out[e.AccessKey]; dup {
			return nil, fmt.Errorf("%s: access key %q listed twice", path, e.AccessKey)
		}
		out[e.AccessKey] = Key{Secret: e.SecretKey, Buckets: e.Buckets, ReadOnly: e.ReadOnly}
	}
	return out, nil
}
