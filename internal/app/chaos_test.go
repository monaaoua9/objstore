package app

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"
)

// Writers and readers hammer the cluster while storage nodes are repeatedly killed and brought back.
// Invariants: no GET ever returns wrong bytes, and every acknowledged PUT is intact at the end.
func TestChaosKeepsAcknowledgedWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	c := newCluster(t, 3, 5, 3, 2)
	c.mkbucket("n1", "chaos")

	var mu sync.Mutex
	acked := map[string][]byte{}
	stop := make(chan struct{})
	var wg sync.WaitGroup

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)))
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				via := fmt.Sprintf("n%d", r.Intn(3)+1) // voters stay up, they are the entry points
				key := fmt.Sprintf("w%d-%d", w, i)
				data := randBytes(20_000 + r.Intn(300_000))
				resp, _ := c.tryDo("PUT", c.base(via)+"/api/v1/buckets/chaos/objects/"+key, bytes.NewReader(data))
				if resp != nil && resp.StatusCode == 201 {
					mu.Lock()
					acked[key] = data
					mu.Unlock()
				}
				// read something back mid-chaos
				mu.Lock()
				var rk string
				var rd []byte
				for k, v := range acked {
					rk, rd = k, v
					break
				}
				mu.Unlock()
				if rk != "" {
					resp, body := c.tryDo("GET", c.base(via)+"/api/v1/buckets/chaos/objects/"+rk, nil)
					if resp != nil && resp.StatusCode == 200 && !bytes.Equal(body, rd) {
						t.Errorf("GET %s returned wrong bytes (%d vs %d)", rk, len(body), len(rd))
					}
				}
			}
		}()
	}

	storage := []string{"n4", "n5", "n6", "n7", "n8"}
	r := rand.New(rand.NewSource(99))
	end := time.Now().Add(20 * time.Second)
	for time.Now().Before(end) {
		id := storage[r.Intn(len(storage))]
		addr := c.nodes[id].Addr()
		c.nodes[id].Kill()
		time.Sleep(time.Duration(500+r.Intn(1500)) * time.Millisecond)
		var ln net.Listener
		var err error
		for try := 0; try < 50; try++ {
			if ln, err = net.Listen("tcp", addr); err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if err != nil {
			t.Fatal(err)
		}
		c.startNode(id, ln)
		time.Sleep(time.Second)
	}
	close(stop)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(acked) < 10 {
		t.Fatalf("only %d writes were acknowledged, test proved little", len(acked))
	}
	t.Logf("%d acknowledged writes under chaos", len(acked))
	for k, d := range acked {
		k, d := k, d
		eventually(t, 30*time.Second, "acked object "+k+" readable and intact", func() bool {
			resp, body := c.tryDo("GET", c.base("n2")+"/api/v1/buckets/chaos/objects/"+k, nil)
			return resp != nil && resp.StatusCode == 200 && bytes.Equal(body, d)
		})
	}
}
