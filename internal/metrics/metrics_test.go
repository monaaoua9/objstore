package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExposition(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("x_total", "things")
	c.Add(3)
	v := r.CounterVec("req_total", "reqs", "code")
	v.Inc("2xx")
	v.Inc("2xx")
	v.Inc("5xx")
	h := r.Histogram("lat_seconds", "latency", []float64{0.1, 1})
	h.Observe(0.05)
	h.Observe(0.5)
	h.Observe(7)
	r.GaugeFunc("up", "alive", func() float64 { return 1 })

	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	out := rec.Body.String()
	for _, want := range []string{
		"x_total 3",
		`req_total{code="2xx"} 2`,
		`req_total{code="5xx"} 1`,
		`lat_seconds_bucket{le="0.1"} 1`,
		`lat_seconds_bucket{le="1"} 2`,
		`lat_seconds_bucket{le="+Inf"} 3`,
		"lat_seconds_count 3",
		"up 1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}
