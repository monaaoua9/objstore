// Package blobstore is the per-node shard store: framed, checksummed files with atomic commit.
package blobstore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/monaaoua9/objstore/internal/frame"
)

var (
	ErrNotFound = errors.New("shard not found")
	ErrBadName  = errors.New("invalid shard name")
)

var versionRe = regexp.MustCompile(`^[0-9a-f]{8,64}$`)

type Store struct {
	root string

	shards atomic.Int64
	bytes  atomic.Int64
}

func Open(root string) (*Store, error) {
	s := &Store{root: root}
	for _, d := range []string{"shards", "tmp", "quarantine"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return nil, err
		}
	}
	err := s.Walk(func(e Entry) error {
		s.shards.Add(1)
		s.bytes.Add(e.Size)
		return nil
	})
	return s, err
}

func (s *Store) Root() string { return s.root }

func (s *Store) Count() int64 { return s.shards.Load() }
func (s *Store) Bytes() int64 { return s.bytes.Load() }

func check(version string, idx int) error {
	if !versionRe.MatchString(version) || idx < 0 || idx > 255 {
		return ErrBadName
	}
	return nil
}

func (s *Store) path(version string, idx int) string {
	return filepath.Join(s.root, "shards", version[:2], version+"."+strconv.Itoa(idx))
}

// Put validates every frame on the way in and only publishes the file if the stream ended
// cleanly on a frame boundary. A cancelled or corrupt upload leaves nothing behind.
func (s *Store) Put(version string, idx int, r io.Reader) (frames int, n int64, err error) {
	if err = check(version, idx); err != nil {
		return
	}
	tmp, err := os.CreateTemp(filepath.Join(s.root, "tmp"), "put-*")
	if err != nil {
		return
	}
	committed := false
	defer func() {
		if !committed {
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}()

	bw := bufio.NewWriterSize(tmp, 1<<20)
	fr := frame.NewReader(r)
	var buf []byte
	for {
		buf, err = fr.Next(buf)
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			return
		}
		if err = frame.Write(bw, buf); err != nil {
			return
		}
		n += int64(frame.HeaderSize + len(buf))
	}
	frames = fr.Count()
	if err = bw.Flush(); err != nil {
		return
	}
	if err = tmp.Sync(); err != nil {
		return
	}
	if err = tmp.Close(); err != nil {
		return
	}

	dst := s.path(version, idx)
	if err = os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return
	}
	var old int64 = -1
	if st, e := os.Stat(dst); e == nil {
		old = st.Size()
	}
	if err = os.Rename(tmp.Name(), dst); err != nil {
		return
	}
	committed = true
	syncDir(filepath.Dir(dst))
	if old < 0 {
		s.shards.Add(1)
		s.bytes.Add(n)
	} else {
		s.bytes.Add(n - old)
	}
	return
}

type File struct {
	*os.File
	Size int64
}

// Open returns the shard positioned at byte offset.
func (s *Store) Open(version string, idx int, offset int64) (*File, error) {
	if err := check(version, idx); err != nil {
		return nil, err
	}
	f, err := os.Open(s.path(version, idx))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if offset > 0 {
		if offset > st.Size() {
			f.Close()
			return nil, fmt.Errorf("offset %d beyond shard size %d", offset, st.Size())
		}
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			f.Close()
			return nil, err
		}
	}
	return &File{File: f, Size: st.Size()}, nil
}

func (s *Store) Stat(version string, idx int) (int64, error) {
	if err := check(version, idx); err != nil {
		return 0, err
	}
	st, err := os.Stat(s.path(version, idx))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// Delete is idempotent.
func (s *Store) Delete(version string, idx int) error {
	if err := check(version, idx); err != nil {
		return err
	}
	p := s.path(version, idx)
	st, err := os.Stat(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	s.shards.Add(-1)
	s.bytes.Add(-st.Size())
	return nil
}

// Verify re-reads the whole shard and checks every frame.
func (s *Store) Verify(version string, idx int, rate *Limiter) (frames int, err error) {
	f, err := s.Open(version, idx, 0)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var r io.Reader = f
	if rate != nil {
		r = rate.Reader(f)
	}
	fr := frame.NewReader(bufio.NewReaderSize(r, 1<<20))
	var buf []byte
	for {
		buf, err = fr.Next(buf)
		if err == io.EOF {
			return fr.Count(), nil
		}
		if err != nil {
			return fr.Count(), err
		}
	}
}

// Quarantine moves a bad shard aside so reads fail fast and repair can write a fresh copy.
func (s *Store) Quarantine(version string, idx int) error {
	if err := check(version, idx); err != nil {
		return err
	}
	p := s.path(version, idx)
	st, err := os.Stat(p)
	if err != nil {
		return nil
	}
	dst := filepath.Join(s.root, "quarantine", fmt.Sprintf("%s.%d.%d", version, idx, time.Now().UnixNano()))
	if err := os.Rename(p, dst); err != nil {
		return err
	}
	s.shards.Add(-1)
	s.bytes.Add(-st.Size())
	return nil
}

type Entry struct {
	Version string
	Index   int
	Size    int64
	ModTime time.Time
}

// Walk visits every committed shard. Order is filesystem order.
func (s *Store) Walk(fn func(Entry) error) error {
	root := filepath.Join(s.root, "shards")
	return filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		dot := strings.LastIndexByte(name, '.')
		if dot < 0 {
			return nil
		}
		idx, err := strconv.Atoi(name[dot+1:])
		if err != nil || !versionRe.MatchString(name[:dot]) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil // vanished between readdir and stat
		}
		return fn(Entry{name[:dot], idx, info.Size(), info.ModTime()})
	})
}

// Sweep removes abandoned temp uploads and old quarantined files.
func (s *Store) Sweep(tmpAge, quarantineAge time.Duration) (removed int) {
	clean := func(dir string, age time.Duration) {
		ents, _ := os.ReadDir(filepath.Join(s.root, dir))
		for _, e := range ents {
			info, err := e.Info()
			if err != nil || time.Since(info.ModTime()) < age {
				continue
			}
			if os.Remove(filepath.Join(s.root, dir, e.Name())) == nil {
				removed++
			}
		}
	}
	clean("tmp", tmpAge)
	clean("quarantine", quarantineAge)
	return
}

func syncDir(p string) {
	if d, err := os.Open(p); err == nil {
		d.Sync() // not supported on windows, which is fine
		d.Close()
	}
}
