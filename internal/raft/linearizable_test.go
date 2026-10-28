package raft

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Jepsen-style check. Clients append unique values and take linearizable reads (ReadBarrier, then
// look at the local state) against five nodes while a nemesis partitions, isolates leaders and
// crashes nodes. The state is an append-only list, so linearizability reduces to a handful of
// conditions on the final log that are easy to state and check exactly (see verify).

type opRec struct {
	val        string // write: the value; read: empty
	read       []string
	start, end time.Time
	ok         bool // false: outcome unknown (timeout, not leader, node down)
}

type lcluster struct {
	*cluster
	mu sync.RWMutex // guards the cluster's node/fsm maps while the nemesis swaps nodes
}

func (l *lcluster) pick(r *rand.Rand) (string, *Node, *listFSM) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	id := l.ids[r.Intn(len(l.ids))]
	return id, l.nodes[id], l.fsms[id]
}

func (l *lcluster) cutPair(a, b string, on bool) {
	l.net.mu.Lock()
	l.net.cut[[2]string{a, b}] = on
	l.net.cut[[2]string{b, a}] = on
	l.net.mu.Unlock()
}

func (l *lcluster) healAll() {
	l.net.mu.Lock()
	l.net.cut = map[[2]string]bool{}
	l.net.mu.Unlock()
}

func TestLinearizableUnderNemesis(t *testing.T) {
	seed := time.Now().UnixNano()
	if s := os.Getenv("RAFT_SEED"); s != "" {
		seed, _ = strconv.ParseInt(s, 10, 64)
	}
	t.Logf("seed %d (RAFT_SEED=%d to replay the fault schedule)", seed, seed)

	lc := &lcluster{cluster: newCluster(t, 5, func(c *Config) {
		c.SnapshotThreshold, c.SnapshotTrailing = 40, 10 // push lagging nodes through InstallSnapshot
	})}
	dur := 7 * time.Second

	var (
		hmu           sync.Mutex
		writes        []opRec
		reads         []opRec
		wg            sync.WaitGroup
		stopCtx, stop = context.WithTimeout(context.Background(), dur)
	)
	defer stop()

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed + int64(w)))
			for i := 0; stopCtx.Err() == nil; i++ {
				_, n, _ := lc.pick(r)
				if n == nil {
					time.Sleep(5 * time.Millisecond)
					continue
				}
				rec := opRec{val: fmt.Sprintf("w%d-%d", w, i), start: time.Now()}
				ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
				_, err := n.Propose(ctx, []byte(rec.val))
				cancel()
				rec.end, rec.ok = time.Now(), err == nil
				hmu.Lock()
				writes = append(writes, rec)
				hmu.Unlock()
				time.Sleep(time.Duration(r.Intn(8)) * time.Millisecond)
			}
		}(w)
	}
	for rd := 0; rd < 3; rd++ {
		wg.Add(1)
		go func(rd int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed + 100 + int64(rd)))
			for stopCtx.Err() == nil {
				_, n, f := lc.pick(r)
				if n == nil {
					time.Sleep(5 * time.Millisecond)
					continue
				}
				rec := opRec{start: time.Now()}
				ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
				err := n.ReadBarrier(ctx)
				cancel()
				if err == nil {
					rec.read = f.snapshotVals()
				}
				rec.end, rec.ok = time.Now(), err == nil
				if rec.ok {
					hmu.Lock()
					reads = append(reads, rec)
					hmu.Unlock()
				}
				time.Sleep(time.Duration(r.Intn(6)) * time.Millisecond)
			}
		}(rd)
	}

	// nemesis
	down := map[string]bool{}
	nr := rand.New(rand.NewSource(seed + 999))
	for stopCtx.Err() == nil {
		time.Sleep(time.Duration(100+nr.Intn(250)) * time.Millisecond)
		switch nr.Intn(6) {
		case 0:
			id := lc.ids[nr.Intn(5)]
			lc.net.isolate(id, lc.ids, true)
		case 1:
			for _, id := range lc.ids {
				lc.mu.RLock()
				n := lc.nodes[id]
				lc.mu.RUnlock()
				if n != nil && n.IsLeader() {
					lc.net.isolate(id, lc.ids, true)
					break
				}
			}
		case 2: // 2 | 3 split
			perm := nr.Perm(5)
			for _, a := range perm[:2] {
				for _, b := range perm[2:] {
					lc.cutPair(lc.ids[a], lc.ids[b], true)
				}
			}
		case 3: // crash one (never more than two down at once)
			id := lc.ids[nr.Intn(5)]
			if !down[id] && len(down) < 2 {
				lc.mu.Lock()
				lc.stop(id)
				lc.mu.Unlock()
				down[id] = true
			}
		case 4: // restart whoever is down
			for id := range down {
				lc.mu.Lock()
				lc.start(id)
				lc.mu.Unlock()
				delete(down, id)
			}
		case 5:
			lc.healAll()
		}
	}
	wg.Wait()

	lc.healAll()
	for id := range down {
		lc.mu.Lock()
		lc.start(id)
		lc.mu.Unlock()
	}
	// a final acknowledged write proves a leader exists and the log is live again
	var final []string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := lc.leader().Propose(ctx, []byte("final"))
		cancel()
		if err == nil {
			break
		}
	}
	final = lc.waitConverged()

	verify(t, final, writes, reads)
	var acked int
	for _, w := range writes {
		if w.ok {
			acked++
		}
	}
	t.Logf("%d writes (%d acked), %d reads, final log %d entries", len(writes), acked, len(reads), len(final))
	if acked < 20 || len(reads) < 20 {
		t.Fatalf("nemesis starved the workload: %d acked writes, %d reads", acked, len(reads))
	}
}

// verify checks the history against the final log, which is the linearization order of the writes:
//   - every acknowledged write is in the log, once; nothing appears twice
//   - writes ordered in real time (A acked before B began) are in that order in the log
//   - every read is a prefix of the log
//   - a read contains every write acknowledged before it began, and none that began after it ended
//   - reads ordered in real time never go backwards
func verify(t *testing.T, final []string, writes, reads []opRec) {
	t.Helper()
	pos := map[string]int{}
	for i, v := range final {
		if _, dup := pos[v]; dup {
			t.Fatalf("value %q applied twice", v)
		}
		pos[v] = i
	}
	byVal := map[string]opRec{}
	for _, w := range writes {
		byVal[w.val] = w
		if w.ok {
			if _, in := pos[w.val]; !in {
				t.Fatalf("acknowledged write %q is missing from the log", w.val)
			}
		}
	}
	// pairwise real-time order of writes: O(n^2) on a few thousand ops is fine
	for _, a := range writes {
		if !a.ok {
			continue
		}
		for _, b := range writes {
			if a.end.Before(b.start) {
				pb, inB := pos[b.val]
				if inB && pos[a.val] > pb {
					t.Fatalf("write %q was acknowledged before %q began but is ordered after it", a.val, b.val)
				}
			}
		}
	}
	for i, r := range reads {
		for j, v := range r.read {
			if j >= len(final) || final[j] != v {
				t.Fatalf("read %d is not a prefix of the log at index %d (%q)", i, j, v)
			}
		}
		in := map[string]bool{}
		for _, v := range r.read {
			in[v] = true
			if w, known := byVal[v]; known && w.start.After(r.end) {
				t.Fatalf("read %d returned %q, which began after the read had finished", i, v)
			}
		}
		for _, w := range writes {
			if w.ok && w.end.Before(r.start) && !in[w.val] {
				t.Fatalf("read %d missed %q, acknowledged before the read began (read has %d entries)", i, w.val, len(r.read))
			}
		}
	}
	for i, a := range reads {
		for j, b := range reads {
			if a.end.Before(b.start) && len(b.read) < len(a.read) {
				t.Fatalf("read %d (%d entries) finished before read %d (%d entries) began: state went backwards", i, len(a.read), j, len(b.read))
			}
		}
	}
}

// the checker is only worth something if it fails on bad histories
func TestVerifyCatchesViolations(t *testing.T) {
	t0 := time.Now()
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	final := []string{"a", "b", "c"}
	w := func(v string, s, e int) opRec { return opRec{val: v, start: at(s), end: at(e), ok: true} }
	r := func(s, e int, vals ...string) opRec { return opRec{read: vals, start: at(s), end: at(e), ok: true} }

	cases := map[string]struct {
		writes, reads []opRec
		final         []string
	}{
		"stale read":       {[]opRec{w("a", 0, 1), w("b", 2, 3)}, []opRec{r(10, 11, "a")}, final},
		"lost write":       {[]opRec{w("a", 0, 1), w("x", 2, 3)}, nil, final},
		"read from future": {[]opRec{w("a", 0, 1), w("b", 20, 21)}, []opRec{r(5, 6, "a", "b")}, final},
		"not a prefix":     {[]opRec{w("a", 0, 1), w("b", 2, 3)}, []opRec{r(10, 11, "b")}, final},
		"time goes back":   {nil, []opRec{r(0, 1, "a", "b"), r(2, 3, "a")}, final},
		"order inversion":  {[]opRec{w("b", 0, 1), w("a", 2, 3)}, nil, final},
		"duplicate":        {nil, nil, []string{"a", "a"}},
	}
	for name, c := range cases {
		c := c
		t.Run(name, func(t *testing.T) {
			ft := &testing.T{}
			done := make(chan struct{})
			go func() { // Fatalf exits the goroutine, so run it on a throwaway
				defer close(done)
				verify(ft, c.final, c.writes, c.reads)
			}()
			<-done
			if !ft.Failed() {
				t.Fatal("verify accepted a bad history")
			}
		})
	}
}
