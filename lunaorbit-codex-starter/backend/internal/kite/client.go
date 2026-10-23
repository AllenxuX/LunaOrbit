package kite

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const MaxBodyBytes = 1 << 20

type Config struct {
	BaseURL        string
	AllowedOrigins []string
	APIKey         string
	Timeout        time.Duration
	MaxRetries     int
	RetryDelay     time.Duration
}

type HTTPClient struct {
	baseURL   string
	key       string
	timeout   time.Duration
	retries   int
	delay     time.Duration
	http      *http.Client
	transport *http.Transport
}

var _ KiteClient = (*HTTPClient)(nil)
var dnsLabel = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$`)

func origin(raw string) (string, bool) {
	u, err := url.Parse(raw)
	if err != nil || u.User != nil || u.Opaque != "" || u.Host == "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") || u.RawPath != "" {
		return "", false
	}
	if u.Scheme != "https" {
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "http" || ip == nil || !ip.IsLoopback() {
			return "", false
		}
	}
	if u.Hostname() == "" {
		return "", false
	}
	return u.Scheme + "://" + strings.ToLower(u.Host), true
}

func headerValue(value string) bool {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 4096 {
		return false
	}
	for _, char := range value {
		if char < 32 || char > 126 {
			return false
		}
	}
	return true
}

func New(cfg Config) (*HTTPClient, error) {
	invalid := func() (*HTTPClient, error) { return nil, &Error{Code: "INVALID_CONFIG"} }
	base, ok := origin(cfg.BaseURL)
	if !ok || !headerValue(cfg.APIKey) || strings.HasPrefix(strings.ToLower(cfg.APIKey), "bearer ") || cfg.Timeout <= 0 || cfg.Timeout > time.Minute || cfg.MaxRetries < 0 || cfg.MaxRetries > 2 || cfg.RetryDelay < 0 || cfg.RetryDelay > time.Second {
		return invalid()
	}
	allowed := false
	for _, entry := range cfg.AllowedOrigins {
		candidate, valid := origin(entry)
		if !valid {
			return invalid()
		}
		if candidate == base {
			allowed = true
		}
	}
	if !allowed {
		return invalid()
	}
	if cfg.RetryDelay == 0 {
		cfg.RetryDelay = 100 * time.Millisecond
	}
	transport := &http.Transport{
		DialContext:       (&net.Dialer{Timeout: cfg.Timeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2: true, MaxIdleConns: 16, MaxIdleConnsPerHost: 2,
		IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: 5 * time.Second,
		ResponseHeaderTimeout: cfg.Timeout,
	}
	return &HTTPClient{baseURL: base, key: cfg.APIKey, timeout: cfg.Timeout, retries: cfg.MaxRetries, delay: cfg.RetryDelay, transport: transport, http: &http.Client{Transport: transport, Timeout: cfg.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (c *HTTPClient) Close() { c.transport.CloseIdleConnections() }

func validLabel(value string) bool {
	return len(value) > 0 && len(value) <= 63 && dnsLabel.MatchString(value)
}
func validName(value string) bool {
	if len(value) == 0 || len(value) > 253 {
		return false
	}
	for _, part := range strings.Split(value, ".") {
		if !validLabel(part) {
			return false
		}
	}
	return true
}

func (c *HTTPClient) ListNamespaces(ctx context.Context, cluster string) (Document, error) {
	return c.query(ctx, cluster, "namespaces", "", "")
}
func (c *HTTPClient) ListConfigMaps(ctx context.Context, cluster, ns string) (Document, error) {
	return c.query(ctx, cluster, "configmaps", ns, "")
}
func (c *HTTPClient) GetConfigMap(ctx context.Context, cluster, ns, name string) (Document, error) {
	if !validName(name) {
		return Document{}, &Error{Code: "INVALID_ARGUMENT"}
	}
	return c.query(ctx, cluster, "configmaps", ns, name)
}
func (c *HTTPClient) ListDeployments(ctx context.Context, cluster, ns string) (Document, error) {
	return c.query(ctx, cluster, "deployments", ns, "")
}
func (c *HTTPClient) GetDeployment(ctx context.Context, cluster, ns, name string) (Document, error) {
	if !validName(name) {
		return Document{}, &Error{Code: "INVALID_ARGUMENT"}
	}
	return c.query(ctx, cluster, "deployments", ns, name)
}
func (c *HTTPClient) ListPods(ctx context.Context, cluster, ns string) (Document, error) {
	return c.query(ctx, cluster, "pods", ns, "")
}
func (c *HTTPClient) GetPod(ctx context.Context, cluster, ns, name string) (Document, error) {
	if !validName(name) {
		return Document{}, &Error{Code: "INVALID_ARGUMENT"}
	}
	return c.query(ctx, cluster, "pods", ns, name)
}

func contextError(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return &Error{Code: "CANCELED"}
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &Error{Code: "TIMEOUT"}
	}
	return &Error{Code: "NETWORK_ERROR"}
}

func statusError(status int) *Error {
	code := "UNEXPECTED_STATUS"
	switch {
	case status >= 300 && status < 400:
		code = "REDIRECT_BLOCKED"
	case status == 401:
		code = "UNAUTHENTICATED"
	case status == 403:
		code = "PERMISSION_DENIED"
	case status == 404:
		code = "NOT_FOUND"
	case status == 429:
		code = "RATE_LIMITED"
	case status >= 500 && status < 600:
		code = "UPSTREAM_UNAVAILABLE"
	}
	return &Error{Code: code, HTTPStatus: status}
}

func (c *HTTPClient) query(ctx context.Context, cluster, kind, ns, name string) (Document, error) {
	if !headerValue(cluster) {
		return Document{}, &Error{Code: "INVALID_ARGUMENT"}
	}
	path := "/api/v1/" + kind
	switch kind {
	case "namespaces":
		path += "/_all"
	case "configmaps", "deployments", "pods":
		if !validLabel(ns) {
			return Document{}, &Error{Code: "INVALID_ARGUMENT"}
		}
		path += "/" + ns
		if name != "" {
			if !validName(name) {
				return Document{}, &Error{Code: "INVALID_ARGUMENT"}
			}
			path += "/" + name
		}
	default:
		return Document{}, &Error{Code: "INVALID_ARGUMENT"}
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	for attempt := 0; attempt <= c.retries; attempt++ {
		if ctx.Err() != nil {
			return Document{}, contextError(ctx)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
		if err != nil {
			return Document{}, &Error{Code: "INVALID_CONFIG"}
		}
		req.Header.Set("Authorization", c.key)
		req.Header.Set("x-cluster-name", cluster)
		req.Header.Set("Accept", "application/json")
		res, err := c.http.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return Document{}, contextError(ctx)
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				return Document{}, &Error{Code: "TIMEOUT"}
			}
			return Document{}, &Error{Code: "NETWORK_ERROR"}
		}
		if res.StatusCode != http.StatusOK {
			_ = res.Body.Close()
			if (res.StatusCode == 429 || (res.StatusCode >= 500 && res.StatusCode < 600)) && attempt < c.retries {
				timer := time.NewTimer(c.delay * time.Duration(attempt+1))
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return Document{}, contextError(ctx)
				}
				continue
			}
			return Document{}, statusError(res.StatusCode)
		}
		body, readErr := io.ReadAll(io.LimitReader(res.Body, MaxBodyBytes+1))
		_ = res.Body.Close()
		if readErr != nil {
			return Document{}, contextError(ctx)
		}
		if len(body) > MaxBodyBytes {
			return Document{}, &Error{Code: "RESPONSE_TOO_LARGE"}
		}
		return Document{kind: kind, single: name != "", body: body}, nil
	}
	return Document{}, &Error{Code: "UPSTREAM_UNAVAILABLE"}
}
