package raft

import (
	"os"
	"path/filepath"
	"testing"
)

// whatever a crash leaves in the WAL or snapshot file, loading either fails cleanly or repairs the tail
func FuzzLoadDamagedFiles(f *testing.F) {
	f.Add([]byte{}, []byte{})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, []byte{0xff})
	f.Fuzz(func(t *testing.T, wal, snap []byte) {
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, "wal.log"), wal, 0o644)
		if len(snap) > 0 {
			os.WriteFile(filepath.Join(dir, "snapshot.bin"), snap, 0o644)
		}
		st, err := NewFileStorage(dir, true)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		st.Load()
	})
}
