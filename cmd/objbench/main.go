// objbench hammers a cluster through the REST API and prints throughput and latency percentiles.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/monaaoua9/objstore/internal/httpx"
)

var (
	endpoints = flag.String("e", "http://127.0.0.1:9000", "comma separated node URLs, requests are spread over them")
	token     = flag.String("t", os.Getenv("OBJCTL_TOKEN"), "API bearer token")
	size      = flag.Int("size", 1<<20, "object size in bytes")
	count     = flag.Int("n", 200, "objects to write, then read back")
	conc      = flag.Int("c", 16, "concurrent clients")
	bucket    = flag.String("bucket", "bench", "bucket (created if missing)")
)

var client = httpx.NewClient(nil)

func main() {
	flag.Parse()
	eps := strings.Split(*endpoints, ",")
	call := func(i int, method, path string, body []byte) (int, []byte, error) {
		req, _ := http.NewRequest(method, eps[i%len(eps)]+"/api/v1"+path, bytes.NewReader(body))
		if *token != "" {
			req.Header.Set("Authorization", "Bearer "+*token)
		}
		resp, err := client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		return resp.StatusCode, b, err
	}

	call(0, "PUT", "/buckets/"+*bucket, nil)
	payload := make([]byte, *size)
	rand.New(rand.NewSource(1)).Read(payload)
	run := time.Now().Unix()
	key := func(i int) string { return fmt.Sprintf("/buckets/%s/objects/b%d/%06d", *bucket, run, i) }

	phase := func(name string, fn func(i int) (int, error)) {
		lat := make([]time.Duration, *count)
		var failed, bytesMoved int64
		var firstErr error
		var mu sync.Mutex
		jobs := make(chan int)
		var wg sync.WaitGroup
		t0 := time.Now()
		for w := 0; w < *conc; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range jobs {
					s := time.Now()
					n, err := fn(i)
					lat[i] = time.Since(s)
					mu.Lock()
					if err != nil {
						failed++
						if firstErr == nil {
							firstErr = err
						}
					} else {
						bytesMoved += int64(n)
					}
					mu.Unlock()
				}
			}()
		}
		for i := 0; i < *count; i++ {
			jobs <- i
		}
		close(jobs)
		wg.Wait()
		el := time.Since(t0)
		if firstErr != nil {
			fmt.Println("  first error:", firstErr)
		}

		sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
		pct := func(p float64) time.Duration {
			return lat[min(int(float64(len(lat))*p), len(lat)-1)].Round(time.Microsecond)
		}
		fmt.Printf("%-6s %6.1f MiB/s %7.1f ops/s  p50 %v  p95 %v  p99 %v  max %v  failed %d\n", name,
			float64(bytesMoved)/(1<<20)/el.Seconds(), float64(*count)/el.Seconds(), pct(.5), pct(.95), pct(.99), lat[len(lat)-1].Round(time.Microsecond), failed)
	}

	fmt.Printf("%d objects x %d bytes, %d clients, %d endpoint(s)\n", *count, *size, *conc, len(eps))
	phase("write", func(i int) (int, error) {
		st, b, err := call(i, "PUT", key(i), payload)
		if err == nil && st != 201 {
			err = fmt.Errorf("%d %s", st, b)
		}
		return len(payload), err
	})
	phase("read", func(i int) (int, error) {
		st, b, err := call(i+1, "GET", key(i), nil)
		if err == nil && (st != 200 || len(b) != len(payload)) {
			err = fmt.Errorf("%d, %d bytes", st, len(b))
		}
		return len(b), err
	})
	phase("delete", func(i int) (int, error) {
		st, _, err := call(i, "DELETE", key(i), nil)
		if err == nil && st != 204 {
			err = fmt.Errorf("%d", st)
		}
		return 0, err
	})
}
