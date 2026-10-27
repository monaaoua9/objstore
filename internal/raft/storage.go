package raft

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Storage is what the node needs to survive a restart. Every call must be durable on return.
type Storage interface {
	Load() (term uint64, votedFor string, snap Snapshot, entries []Entry, err error)
	SaveState(term uint64, votedFor string) error
	Append(entries []Entry) error
	TruncateFrom(index uint64) error
	// SaveSnapshot replaces the snapshot and rewrites the log to exactly `keep`.
	SaveSnapshot(snap Snapshot, keep []Entry) error
	Close() error
}

var crcTab = crc32.MakeTable(crc32.Castagnoli)

const (
	recEntry    = 1
	recTruncate = 2
)

type FileStorage struct {
	dir    string
	noSync bool

	mu  sync.Mutex
	wal *os.File
}

func NewFileStorage(dir string, noSync bool) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStorage{dir: dir, noSync: noSync}, nil
}

func (s *FileStorage) path(name string) string { return filepath.Join(s.dir, name) }

func (s *FileStorage) sync(f *os.File) error {
	if s.noSync {
		return nil
	}
	return f.Sync()
}

type stateFile struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for"`
}

func (s *FileStorage) Load() (uint64, string, Snapshot, []Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var term uint64
	var vote string
	if b, err := os.ReadFile(s.path("state.json")); err == nil {
		var st stateFile
		if err := json.Unmarshal(b, &st); err != nil {
			return 0, "", Snapshot{}, nil, fmt.Errorf("raft: corrupt state file: %w", err)
		}
		term, vote = st.Term, st.VotedFor
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, "", Snapshot{}, nil, err
	}

	snap, err := s.readSnapshot()
	if err != nil {
		return 0, "", Snapshot{}, nil, err
	}

	entries, good, err := s.replayWAL(snap.Index)
	if err != nil {
		return 0, "", Snapshot{}, nil, err
	}
	// cut a torn tail so new appends don't land after garbage
	f, err := os.OpenFile(s.path("wal.log"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return 0, "", Snapshot{}, nil, err
	}
	if err := f.Truncate(good); err != nil {
		f.Close()
		return 0, "", Snapshot{}, nil, err
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		f.Close()
		return 0, "", Snapshot{}, nil, err
	}
	s.wal = f
	return term, vote, snap, entries, nil
}

func (s *FileStorage) readSnapshot() (Snapshot, error) {
	b, err := os.ReadFile(s.path("snapshot.bin"))
	if errors.Is(err, os.ErrNotExist) {
		return Snapshot{}, nil
	}
	if err != nil {
		return Snapshot{}, err
	}
	if len(b) < 24 {
		return Snapshot{}, errors.New("raft: snapshot file truncated")
	}
	body, sum := b[:len(b)-4], binary.LittleEndian.Uint32(b[len(b)-4:])
	if crc32.Checksum(body, crcTab) != sum {
		return Snapshot{}, errors.New("raft: snapshot checksum mismatch")
	}
	pl := int(binary.LittleEndian.Uint32(body[16:20]))
	if 20+pl > len(body) {
		return Snapshot{}, errors.New("raft: snapshot header corrupt")
	}
	snap := Snapshot{
		Index: binary.LittleEndian.Uint64(body[0:8]),
		Term:  binary.LittleEndian.Uint64(body[8:16]),
		Data:  body[20+pl:],
	}
	if pl > 0 {
		if err := json.Unmarshal(body[20:20+pl], &snap.Peers); err != nil {
			return Snapshot{}, fmt.Errorf("raft: snapshot peers: %w", err)
		}
	}
	return snap, nil
}

// returns entries after snapIndex and the byte offset of the last intact record
func (s *FileStorage) replayWAL(snapIndex uint64) ([]Entry, int64, error) {
	f, err := os.Open(s.path("wal.log"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	var entries []Entry
	r := bufio.NewReaderSize(f, 1<<20)
	var off, good int64
	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			break
		}
		n := binary.LittleEndian.Uint32(hdr[0:4])
		want := binary.LittleEndian.Uint32(hdr[4:8])
		if n == 0 || n > 1<<30 {
			break
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			break
		}
		if crc32.Checksum(body, crcTab) != want {
			break
		}
		off += int64(8 + n)

		switch body[0] {
		case recEntry:
			e := Entry{
				Index: binary.LittleEndian.Uint64(body[1:9]),
				Term:  binary.LittleEndian.Uint64(body[9:17]),
				Type:  EntryType(body[17]),
				Data:  append([]byte(nil), body[18:]...),
			}
			if e.Index > snapIndex {
				entries = append(entries, e)
			}
		case recTruncate:
			from := binary.LittleEndian.Uint64(body[1:9])
			for len(entries) > 0 && entries[len(entries)-1].Index >= from {
				entries = entries[:len(entries)-1]
			}
		default:
			return nil, 0, fmt.Errorf("raft: unknown wal record type %d", body[0])
		}
		good = off
	}
	return entries, good, nil
}

func (s *FileStorage) SaveState(term uint64, votedFor string) error {
	b, _ := json.Marshal(stateFile{term, votedFor})
	return s.atomicWrite("state.json", b)
}

func (s *FileStorage) atomicWrite(name string, data []byte) error {
	tmp := s.path(name + ".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := s.sync(f); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, s.path(name))
}

func encodeRecord(typ byte, payload []byte) []byte {
	buf := make([]byte, 8+1+len(payload))
	buf[8] = typ
	copy(buf[9:], payload)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(1+len(payload)))
	binary.LittleEndian.PutUint32(buf[4:8], crc32.Checksum(buf[8:], crcTab))
	return buf
}

func entryRecord(e Entry) []byte {
	p := make([]byte, 17+len(e.Data))
	binary.LittleEndian.PutUint64(p[0:8], e.Index)
	binary.LittleEndian.PutUint64(p[8:16], e.Term)
	p[16] = byte(e.Type)
	copy(p[17:], e.Data)
	return encodeRecord(recEntry, p)
}

func (s *FileStorage) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	var buf bytes.Buffer
	for _, e := range entries {
		buf.Write(entryRecord(e))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.wal.Write(buf.Bytes()); err != nil {
		return err
	}
	return s.sync(s.wal)
}

func (s *FileStorage) TruncateFrom(index uint64) error {
	p := make([]byte, 8)
	binary.LittleEndian.PutUint64(p, index)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.wal.Write(encodeRecord(recTruncate, p)); err != nil {
		return err
	}
	return s.sync(s.wal)
}

func (s *FileStorage) SaveSnapshot(snap Snapshot, keep []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var peers []byte
	if snap.Peers != nil {
		peers, _ = json.Marshal(snap.Peers)
	}
	body := make([]byte, 20+len(peers)+len(snap.Data), 24+len(peers)+len(snap.Data))
	binary.LittleEndian.PutUint64(body[0:8], snap.Index)
	binary.LittleEndian.PutUint64(body[8:16], snap.Term)
	binary.LittleEndian.PutUint32(body[16:20], uint32(len(peers)))
	copy(body[20:], peers)
	copy(body[20+len(peers):], snap.Data)
	body = binary.LittleEndian.AppendUint32(body, crc32.Checksum(body, crcTab))
	if err := s.atomicWrite("snapshot.bin", body); err != nil {
		return err
	}

	// If we crash here the old wal still has the compacted entries; Load skips anything <= snapshot index.
	var buf bytes.Buffer
	for _, e := range keep {
		buf.Write(entryRecord(e))
	}
	tmp := s.path("wal.log.tmp")
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		return err
	}
	if s.wal != nil {
		s.wal.Close() // windows won't rename over an open file
	}
	if err := os.Rename(tmp, s.path("wal.log")); err != nil {
		return err
	}
	f, err := os.OpenFile(s.path("wal.log"), os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return err
	}
	s.wal = f
	return nil
}

func (s *FileStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal == nil {
		return nil
	}
	err := s.wal.Close()
	s.wal = nil
	return err
}

// MemStorage keeps everything in RAM; used by tests and for restart simulation.
type MemStorage struct {
	mu      sync.Mutex
	term    uint64
	vote    string
	snap    Snapshot
	entries []Entry
}

func NewMemStorage() *MemStorage { return &MemStorage{} }

func (m *MemStorage) Load() (uint64, string, Snapshot, []Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.term, m.vote, m.snap, append([]Entry(nil), m.entries...), nil
}

func (m *MemStorage) SaveState(t uint64, v string) error {
	m.mu.Lock()
	m.term, m.vote = t, v
	m.mu.Unlock()
	return nil
}

func (m *MemStorage) Append(es []Entry) error {
	m.mu.Lock()
	m.entries = append(m.entries, es...)
	m.mu.Unlock()
	return nil
}

func (m *MemStorage) TruncateFrom(i uint64) error {
	m.mu.Lock()
	for len(m.entries) > 0 && m.entries[len(m.entries)-1].Index >= i {
		m.entries = m.entries[:len(m.entries)-1]
	}
	m.mu.Unlock()
	return nil
}

func (m *MemStorage) SaveSnapshot(s Snapshot, keep []Entry) error {
	m.mu.Lock()
	m.snap = s
	m.entries = append([]Entry(nil), keep...)
	m.mu.Unlock()
	return nil
}

func (m *MemStorage) Close() error { return nil }
