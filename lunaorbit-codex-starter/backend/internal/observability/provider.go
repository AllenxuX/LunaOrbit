package observability

import (
	"context"
	"errors"
	"math"
	"time"
)

const LagMetric = "mm2_lag_seconds"
const RecordsMetric = "mm2_records_total"

type Sample struct {
	LinkID     string    `json:"link_id"`
	Metric     string    `json:"metric"`
	Value      *float64  `json:"value"`
	Unit       string    `json:"unit"`
	Source     string    `json:"source"`
	ObservedAt time.Time `json:"observed_at"`
	ExpiresAt  time.Time `json:"expires_at"`
	SampledAt  time.Time `json:"sampled_at"`
	Stale      bool      `json:"stale"`
	Synthetic  bool      `json:"synthetic"`
	State      string    `json:"state"`
	Reason     string    `json:"reason,omitempty"`
}
type MetricProvider interface {
	Read(context.Context, string, string, string) ([]Sample, error)
}
type Error struct{ Code string }

func (e *Error) Error() string { return "observability: " + e.Code }
func fail(code string) error   { return &Error{Code: code} }
func CodeOf(e error) string {
	if e == nil {
		return ""
	}
	var safe *Error
	if errors.As(e, &safe) {
		return safe.Code
	}
	return "UNAVAILABLE"
}
func unknown(link, metric, source, reason string, synthetic bool, now time.Time) Sample {
	unit := "seconds"
	if metric == RecordsMetric {
		unit = "records"
	}
	return Sample{LinkID: link, Metric: metric, Unit: unit, Source: source, ObservedAt: now.UTC(), Synthetic: synthetic, State: "unknown", Reason: reason}
}
func normalize(s Sample, now time.Time, ttl time.Duration) Sample {
	s.ObservedAt = now.UTC()
	s.ExpiresAt = time.Time{}
	if s.Value == nil || math.IsNaN(*s.Value) || math.IsInf(*s.Value, 0) || *s.Value < 0 || s.SampledAt.IsZero() || s.SampledAt.After(now) {
		s.Value = nil
		s.State = "unknown"
		s.Reason = "INVALID_OR_MISSING_SAMPLE"
		s.Stale = false
		return s
	}
	s.ExpiresAt = s.SampledAt.Add(ttl).UTC()
	s.Stale = !now.Before(s.ExpiresAt)
	if s.Stale {
		s.State = "stale"
		s.Reason = "SAMPLE_EXPIRED"
	} else {
		s.State = "observed"
		s.Reason = "METRIC_OBSERVATION_ONLY"
	}
	return s
}

type MockProvider struct {
	ttl time.Duration
	now func() time.Time
}

func NewMockProvider(ttl time.Duration) *MockProvider {
	if ttl <= 0 {
		ttl = time.Minute
	}
	return &MockProvider{ttl: ttl, now: time.Now}
}
func (p *MockProvider) Read(ctx context.Context, scope, link, scenario string) ([]Sample, error) {
	if ctx.Err() != nil {
		return nil, fail("TIMEOUT")
	}
	if scenario == "" {
		scenario = "success"
	}
	if scenario != "success" && scenario != "stale" && scenario != "unavailable" && scenario != "no_data" {
		return nil, fail("INVALID_ARGUMENT")
	}
	now := p.now().UTC()
	out := []Sample{}
	for _, metric := range []string{LagMetric, RecordsMetric} {
		s := unknown(link, metric, "mock", "NO_DATA", true, now)
		switch scenario {
		case "success", "stale":
			value := 12.0
			if metric == RecordsMetric {
				value = 4200
			}
			s.Value = &value
			s.SampledAt = now.Add(-time.Second)
			if scenario == "stale" {
				s.SampledAt = now.Add(-p.ttl - time.Second)
			}
			s = normalize(s, now, p.ttl)
		case "unavailable":
			s.Reason = "SOURCE_UNAVAILABLE"
		}
		out = append(out, s)
	}
	return out, nil
}
