// Package metrics is a tiny Prometheus text-format registry. Enough for counters, gauges,
// one-label vectors and histograms; avoids pulling client_golang into a stdlib-only module.
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type collector interface {
	write(w io.Writer)
	name() string
}

type Registry struct {
	mu   sync.Mutex
	cols []collector
}

func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) add(c collector) {
	r.mu.Lock()
	r.cols = append(r.cols, c)
	r.mu.Unlock()
}

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		r.mu.Lock()
		cols := append([]collector(nil), r.cols...)
		r.mu.Unlock()
		sort.Slice(cols, func(i, j int) bool { return cols[i].name() < cols[j].name() })
		for _, c := range cols {
			c.write(w)
		}
	})
}

func head(w io.Writer, name, help, typ string) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
}

// ---- counter ----

type Counter struct {
	n, help string
	v       atomic.Uint64
}

func (r *Registry) Counter(name, help string) *Counter {
	c := &Counter{n: name, help: help}
	r.add(c)
	return c
}

func (c *Counter) Inc()          { c.v.Add(1) }
func (c *Counter) Add(d uint64)  { c.v.Add(d) }
func (c *Counter) Value() uint64 { return c.v.Load() }
func (c *Counter) name() string  { return c.n }
func (c *Counter) write(w io.Writer) {
	head(w, c.n, c.help, "counter")
	fmt.Fprintf(w, "%s %d\n", c.n, c.v.Load())
}

// ---- gauge ----

type Gauge struct {
	n, help string
	v       atomic.Int64
	fn      func() float64
}

func (r *Registry) Gauge(name, help string) *Gauge {
	g := &Gauge{n: name, help: help}
	r.add(g)
	return g
}

// GaugeFunc is evaluated at scrape time.
func (r *Registry) GaugeFunc(name, help string, fn func() float64) {
	r.add(&Gauge{n: name, help: help, fn: fn})
}

func (g *Gauge) Set(v int64)  { g.v.Store(v) }
func (g *Gauge) Add(d int64)  { g.v.Add(d) }
func (g *Gauge) name() string { return g.n }
func (g *Gauge) write(w io.Writer) {
	head(w, g.n, g.help, "gauge")
	if g.fn != nil {
		fmt.Fprintf(w, "%s %g\n", g.n, g.fn())
		return
	}
	fmt.Fprintf(w, "%s %d\n", g.n, g.v.Load())
}

// ---- counter vec (one label) ----

type CounterVec struct {
	n, help, label string
	mu             sync.Mutex
	m              map[string]*atomic.Uint64
}

func (r *Registry) CounterVec(name, help, label string) *CounterVec {
	v := &CounterVec{n: name, help: help, label: label, m: map[string]*atomic.Uint64{}}
	r.add(v)
	return v
}

func (v *CounterVec) Inc(lv string) { v.Add(lv, 1) }

func (v *CounterVec) Add(lv string, d uint64) {
	v.mu.Lock()
	c := v.m[lv]
	if c == nil {
		c = new(atomic.Uint64)
		v.m[lv] = c
	}
	v.mu.Unlock()
	c.Add(d)
}

func (v *CounterVec) name() string { return v.n }
func (v *CounterVec) write(w io.Writer) {
	head(w, v.n, v.help, "counter")
	v.mu.Lock()
	keys := make([]string, 0, len(v.m))
	for k := range v.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "%s{%s=%q} %d\n", v.n, v.label, k, v.m[k].Load())
	}
	v.mu.Unlock()
}

// ---- histogram ----

type Histogram struct {
	n, help string
	bounds  []float64
	counts  []atomic.Uint64 // per-bucket, non-cumulative
	total   atomic.Uint64
	sumBits atomic.Uint64
}

var DefLatencyBuckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}

func (r *Registry) Histogram(name, help string, bounds []float64) *Histogram {
	if bounds == nil {
		bounds = DefLatencyBuckets
	}
	h := &Histogram{n: name, help: help, bounds: bounds, counts: make([]atomic.Uint64, len(bounds)+1)}
	r.add(h)
	return h
}

func (h *Histogram) Observe(v float64) {
	i := sort.SearchFloat64s(h.bounds, v)
	h.counts[i].Add(1)
	h.total.Add(1)
	for {
		old := h.sumBits.Load()
		if h.sumBits.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
			return
		}
	}
}

func (h *Histogram) Since(t time.Time) { h.Observe(time.Since(t).Seconds()) }

func (h *Histogram) name() string { return h.n }
func (h *Histogram) write(w io.Writer) {
	head(w, h.n, h.help, "histogram")
	var cum uint64
	for i, b := range h.bounds {
		cum += h.counts[i].Load()
		fmt.Fprintf(w, "%s_bucket{le=\"%s\"} %d\n", h.n, trimFloat(b), cum)
	}
	cum += h.counts[len(h.bounds)].Load()
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", h.n, cum)
	fmt.Fprintf(w, "%s_sum %g\n%s_count %d\n", h.n, math.Float64frombits(h.sumBits.Load()), h.n, h.total.Load())
}

func trimFloat(f float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%f", f), "0"), ".")
}
