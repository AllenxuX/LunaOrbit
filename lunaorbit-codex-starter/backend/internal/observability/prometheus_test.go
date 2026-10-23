package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPrometheusVectorsAndSampleTimestamps(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name, value, resultType, status string
		age                             time.Duration
		warnings                        bool
		httpStatus                      int
		want                            string
	}{{name: "fresh", value: "12.5", resultType: "vector", status: "success", httpStatus: 200, want: "observed"}, {name: "stale", value: "12", resultType: "vector", status: "success", age: 2 * time.Minute, httpStatus: 200, want: "stale"}, {name: "nan", value: "NaN", resultType: "vector", status: "success", httpStatus: 200, want: "unknown"}, {name: "inf", value: "+Inf", resultType: "vector", status: "success", httpStatus: 200, want: "unknown"}, {name: "future", value: "1", resultType: "vector", status: "success", age: -time.Minute, httpStatus: 200, want: "unknown"}, {name: "empty", resultType: "vector", status: "success", httpStatus: 200, want: "unknown"}, {name: "partial", value: "1", resultType: "vector", status: "success", warnings: true, httpStatus: 200, want: "unknown"}, {name: "matrix", value: "1", resultType: "matrix", status: "success", httpStatus: 200, want: "unknown"}, {name: "unauthorized", value: "1", httpStatus: 401, want: "unknown"}} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != "GET" || r.URL.Path != "/api/v1/query" {
					t.Error("unexpected metric API")
				}
				metric := LagMetric
				if strings.HasPrefix(r.URL.Query().Get("query"), "sum(") {
					metric = RecordsMetric
				}
				if got := r.URL.Query().Get("query"); got != query(metric, "scope\"},evil_metric{a=\"", "link\\\"\n") {
					t.Error("selector escaping failed")
				}
				w.WriteHeader(tc.httpStatus)
				if tc.httpStatus != 200 {
					w.Write([]byte("private-metric-error"))
					return
				}
				result := []any{}
				if tc.value != "" {
					result = append(result, map[string]any{"metric": map[string]string{}, "value": []any{float64(now.Add(-tc.age).Unix()), tc.value}})
				}
				response := map[string]any{"status": tc.status, "data": map[string]any{"resultType": tc.resultType, "result": result}}
				if tc.warnings {
					response["warnings"] = []string{"partial-private-error"}
				}
				json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			p, e := NewHTTPProvider(HTTPConfig{Endpoint: server.URL, AllowedOrigins: []string{server.URL}, AllowedCIDRs: []string{"127.0.0.0/8"}, TTL: time.Minute})
			if e != nil {
				t.Fatal(e)
			}
			defer p.Close()
			p.now = func() time.Time { return now }
			samples, e := p.Read(context.Background(), "scope\"},evil_metric{a=\"", "link\\\"\n", "")
			if e != nil || len(samples) != 2 || calls != 2 {
				t.Fatal(samples, e, calls)
			}
			for _, s := range samples {
				if s.State != tc.want || s.Synthetic || s.State == "healthy" || strings.Contains(s.Reason, "private") {
					t.Fatal(s)
				}
				if tc.want == "unknown" && s.Value != nil {
					t.Fatal(s)
				}
			}
		})
	}
}

type resolverFunc func(context.Context, string) ([]net.IPAddr, error)

func (f resolverFunc) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	return f(ctx, host)
}
func TestPrometheusSSRFAllDNSAnswersAndRedirect(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Location", "http://127.0.0.1:1/private")
		w.WriteHeader(302)
	}))
	defer server.Close()
	for _, answers := range [][]net.IPAddr{{{IP: net.ParseIP("127.0.0.1")}, {IP: net.ParseIP("192.0.2.1")}}, {{IP: net.ParseIP("192.0.2.1")}}} {
		p, e := NewHTTPProvider(HTTPConfig{Endpoint: server.URL, AllowedOrigins: []string{server.URL}, AllowedCIDRs: []string{"127.0.0.0/8"}, Resolver: resolverFunc(func(context.Context, string) ([]net.IPAddr, error) { return answers, nil })})
		if e != nil {
			t.Fatal(e)
		}
		samples, e := p.Read(context.Background(), "scope", "link", "")
		p.Close()
		if e != nil || samples[0].State != "unknown" || calls.Load() != 0 {
			t.Fatal(samples, e, calls.Load())
		}
	}
	p, _ := NewHTTPProvider(HTTPConfig{Endpoint: server.URL, AllowedOrigins: []string{server.URL}, AllowedCIDRs: []string{"127.0.0.0/8"}})
	defer p.Close()
	samples, e := p.Read(context.Background(), "scope", "link", "")
	if e != nil || samples[0].State != "unknown" || calls.Load() != 2 {
		t.Fatal(samples, e, calls.Load())
	}
	for _, bad := range []string{"http://localhost:9090", "http://192.0.2.1:9090", "https://example.test/private", "https://user:secret@example.test", "https://example.test?query=secret"} {
		if _, e = NewHTTPProvider(HTTPConfig{Endpoint: bad, AllowedOrigins: []string{bad}, AllowedCIDRs: []string{"0.0.0.0/0"}}); e == nil {
			t.Fatal("unsafe endpoint accepted")
		}
	}
}
func TestSampleNormalizationRejectsInvalidEvidence(t *testing.T) {
	now := time.Now()
	for _, v := range []float64{math.NaN(), math.Inf(1), -1} {
		s := normalize(Sample{Value: &v, SampledAt: now}, now, time.Minute)
		if s.Value != nil || s.State != "unknown" {
			t.Fatal(s)
		}
	}
	v := 1.
	s := normalize(Sample{Value: &v, SampledAt: now.Add(time.Nanosecond)}, now, time.Minute)
	if s.State != "unknown" {
		t.Fatal(s)
	}
	s = normalize(Sample{Value: &v, SampledAt: now.Add(-time.Hour)}, now, time.Minute)
	if !s.Stale || s.State != "stale" || !s.ObservedAt.Equal(now) {
		t.Fatal(s)
	}
}
func TestPrometheusTimeoutAndBodyLimit(t *testing.T) {
	for _, huge := range []bool{false, true} {
		t.Run(fmt.Sprint(huge), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if huge {
					w.Write([]byte(strings.Repeat("x", (1<<20)+1)))
				} else {
					time.Sleep(50 * time.Millisecond)
					w.Write([]byte(`{}`))
				}
			}))
			defer server.Close()
			p, _ := NewHTTPProvider(HTTPConfig{Endpoint: server.URL, AllowedOrigins: []string{server.URL}, AllowedCIDRs: []string{"127.0.0.0/8"}, Timeout: 10 * time.Millisecond})
			defer p.Close()
			samples, e := p.Read(context.Background(), "scope", "link", "")
			if e != nil || samples[0].Value != nil || samples[0].State != "unknown" {
				t.Fatal(samples, e)
			}
		})
	}
}
