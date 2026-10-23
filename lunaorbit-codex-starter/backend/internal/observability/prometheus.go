package observability

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Resolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}
type HTTPConfig struct {
	Endpoint                     string
	AllowedOrigins, AllowedCIDRs []string
	Timeout, TTL                 time.Duration
	RootCAs                      *x509.CertPool
	Resolver                     Resolver
}
type HTTPProvider struct {
	endpoint     string
	client       *http.Client
	nets         []*net.IPNet
	resolver     Resolver
	timeout, ttl time.Duration
	slots        chan struct{}
	now          func() time.Time
}

func NewHTTPProvider(cfg HTTPConfig) (*HTTPProvider, error) {
	u, e := url.Parse(cfg.Endpoint)
	if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || (u.Path != "" && u.Path != "/") || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, fail("INVALID_CONFIGURATION")
	}
	if u.Scheme == "http" {
		ip := net.ParseIP(u.Hostname())
		if ip == nil || !ip.IsLoopback() {
			return nil, fail("INVALID_CONFIGURATION")
		}
	}
	endpoint := u.Scheme + "://" + u.Host
	allowed := false
	for _, a := range cfg.AllowedOrigins {
		if a == endpoint {
			allowed = true
		}
	}
	if !allowed {
		return nil, fail("INVALID_CONFIGURATION")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.TTL == 0 {
		cfg.TTL = time.Minute
	}
	if cfg.Timeout <= 0 || cfg.Timeout > time.Minute || cfg.TTL <= 0 || cfg.TTL > 24*time.Hour {
		return nil, fail("INVALID_CONFIGURATION")
	}
	p := &HTTPProvider{endpoint: endpoint, timeout: cfg.Timeout, ttl: cfg.TTL, resolver: cfg.Resolver, slots: make(chan struct{}, 2), now: time.Now}
	if p.resolver == nil {
		p.resolver = net.DefaultResolver
	}
	for _, cidr := range cfg.AllowedCIDRs {
		_, n, e := net.ParseCIDR(cidr)
		if e != nil {
			return nil, fail("INVALID_CONFIGURATION")
		}
		p.nets = append(p.nets, n)
	}
	if len(p.nets) == 0 {
		return nil, fail("INVALID_CONFIGURATION")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: cfg.RootCAs}, TLSHandshakeTimeout: cfg.Timeout, ResponseHeaderTimeout: cfg.Timeout, MaxIdleConns: 2, MaxIdleConnsPerHost: 2, MaxConnsPerHost: 2, IdleConnTimeout: 30 * time.Second}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, e := net.SplitHostPort(address)
		if e != nil {
			return nil, fail("NETWORK_DENIED")
		}
		if host != u.Hostname() {
			return nil, fail("NETWORK_DENIED")
		}
		ips, e := p.resolver.LookupIPAddr(ctx, host)
		if e != nil || len(ips) == 0 {
			return nil, fail("NETWORK_DENIED")
		}
		for _, ip := range ips {
			allowed := false
			for _, n := range p.nets {
				if n.Contains(ip.IP) {
					allowed = true
				}
			}
			if !allowed {
				return nil, fail("NETWORK_DENIED")
			}
		}
		return (&net.Dialer{Timeout: cfg.Timeout}).DialContext(ctx, "tcp", net.JoinHostPort(ips[0].IP.String(), port))
	}
	p.client = &http.Client{Transport: transport, Timeout: cfg.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return p, nil
}
func (p *HTTPProvider) Close() { p.client.CloseIdleConnections() }
func query(metric, scope, link string) string {
	function := "max"
	if metric == RecordsMetric {
		function = "sum"
	}
	return function + "(" + metric + "{scope_id=" + strconv.Quote(scope) + ",link_id=" + strconv.Quote(link) + "})"
}
func validLabelValue(value string) bool {
	return value != "" && len(value) <= 256 && !strings.ContainsRune(value, 0)
}
func (p *HTTPProvider) Read(ctx context.Context, scope, link, scenario string) ([]Sample, error) {
	if !validLabelValue(scope) || !validLabelValue(link) || (scenario != "" && scenario != "success") {
		return nil, fail("INVALID_ARGUMENT")
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	select {
	case p.slots <- struct{}{}:
		defer func() { <-p.slots }()
	case <-ctx.Done():
		return nil, fail("TIMEOUT")
	}
	out := []Sample{}
	for _, metric := range []string{LagMetric, RecordsMetric} {
		s := unknown(link, metric, "prometheus_httpapi", "SOURCE_UNAVAILABLE", false, p.now())
		sample, e := p.readMetric(ctx, scope, link, metric)
		if e != nil {
			s.Reason = CodeOf(e)
		} else {
			s = sample
		}
		out = append(out, s)
	}
	return out, nil
}
func (p *HTTPProvider) readMetric(ctx context.Context, scope, link, metric string) (Sample, error) {
	values := url.Values{"query": []string{query(metric, scope, link)}}
	req, e := http.NewRequestWithContext(ctx, "GET", p.endpoint+"/api/v1/query?"+values.Encode(), nil)
	if e != nil {
		return Sample{}, fail("INVALID_ARGUMENT")
	}
	req.Header.Set("Accept", "application/json")
	r, e := p.client.Do(req)
	if e != nil {
		if ctx.Err() != nil {
			return Sample{}, fail("TIMEOUT")
		}
		return Sample{}, fail("SOURCE_UNAVAILABLE")
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		switch r.StatusCode {
		case 401, 403:
			return Sample{}, fail("SOURCE_AUTHORIZATION_DENIED")
		}
		return Sample{}, fail("SOURCE_UNAVAILABLE")
	}
	body, e := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	if e != nil || len(body) > 1<<20 {
		return Sample{}, fail("INVALID_RESPONSE")
	}
	var response struct {
		Status   string   `json:"status"`
		Warnings []string `json:"warnings"`
		Infos    []string `json:"infos"`
		Data     struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Value []json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &response) != nil || response.Status != "success" || response.Data.ResultType != "vector" || len(response.Warnings) > 0 || len(response.Infos) > 0 {
		return Sample{}, fail("INVALID_RESPONSE")
	}
	if len(response.Data.Result) == 0 {
		return Sample{}, fail("NO_DATA")
	}
	if len(response.Data.Result) != 1 {
		return Sample{}, fail("AMBIGUOUS_SAMPLES")
	}
	value := response.Data.Result[0].Value
	if len(value) != 2 {
		return Sample{}, fail("INVALID_RESPONSE")
	}
	var ts float64
	var raw string
	if json.Unmarshal(value[0], &ts) != nil || json.Unmarshal(value[1], &raw) != nil || math.IsNaN(ts) || math.IsInf(ts, 0) || ts <= 0 || ts > float64(p.now().Unix())+1 {
		return Sample{}, fail("INVALID_SAMPLE")
	}
	v, e := strconv.ParseFloat(raw, 64)
	if e != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return Sample{}, fail("INVALID_SAMPLE")
	}
	s := unknown(link, metric, "prometheus_httpapi", "", false, p.now())
	s.Value = &v
	seconds, frac := math.Modf(ts)
	s.SampledAt = time.Unix(int64(seconds), int64(frac*1e9)).UTC()
	return normalize(s, p.now(), p.ttl), nil
}
