package s3api

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadKeys(t *testing.T) {
	dir := t.TempDir()
	write := func(s string) string {
		p := filepath.Join(dir, "k.json")
		os.WriteFile(p, []byte(s), 0o600)
		return p
	}
	keys, err := LoadKeys(write(`[{"access_key":"A1","secret_key":"longenough1","buckets":["b1"]},{"access_key":"A2","secret_key":"longenough2","read_only":true}]`))
	if err != nil || len(keys) != 2 || !keys["A2"].ReadOnly || keys["A1"].Buckets[0] != "b1" {
		t.Fatalf("%v %+v", err, keys)
	}
	for _, bad := range []string{
		`[{"access_key":"A1","secret_key":"short"}]`,
		`[{"secret_key":"longenough1"}]`,
		`[{"access_key":"A","secret_key":"longenough1"},{"access_key":"A","secret_key":"longenough2"}]`,
		`not json`,
	} {
		if _, err := LoadKeys(write(bad)); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}

	k := Key{Secret: "x", Buckets: []string{"a", "b"}}
	if !k.allows("a", true) || k.allows("c", false) {
		t.Fatal("bucket scoping")
	}
	if (Key{ReadOnly: true}).allows("a", true) || !(Key{ReadOnly: true}).allows("a", false) {
		t.Fatal("read-only")
	}
	if !(Key{Buckets: []string{"*"}}).allows("zzz", true) || !(Key{}).allows("zzz", true) {
		t.Fatal("wildcards")
	}
}
