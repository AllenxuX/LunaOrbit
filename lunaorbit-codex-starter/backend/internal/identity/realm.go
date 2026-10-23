package identity

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"io"
	"lunaorbit/backend/internal/registry"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type RealmConfig struct {
	Mode                     string
	AllowedOrigins           []string
	DevelopmentLoopback      bool
	TTL                      time.Duration
	Capacity, LoginPerMinute int
}
type session struct {
	subject Subject
	expires time.Time
}
type Realm struct {
	provider Provider
	cfg      RealmConfig
	mu       sync.Mutex
	sessions map[string]session
	attempts []time.Time
	now      func() time.Time
}

func NewRealm(provider Provider, cfg RealmConfig) (*Realm, error) {
	if provider == nil || (cfg.Mode != "ldap" && cfg.Mode != "mock") {
		return nil, ErrConfig
	}
	if cfg.Mode == "mock" && !cfg.DevelopmentLoopback {
		return nil, ErrConfig
	}
	if cfg.TTL == 0 {
		cfg.TTL = 8 * time.Hour
	}
	if cfg.Capacity == 0 {
		cfg.Capacity = 128
	}
	if cfg.LoginPerMinute == 0 {
		cfg.LoginPerMinute = 20
	}
	if cfg.TTL < time.Millisecond || cfg.TTL > 24*time.Hour || cfg.Capacity < 1 || cfg.Capacity > 10000 || cfg.LoginPerMinute < 1 || cfg.LoginPerMinute > 1000 {
		return nil, ErrConfig
	}
	for _, raw := range cfg.AllowedOrigins {
		u, e := url.Parse(raw)
		if e != nil || u.User != nil || u.Host == "" || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(cfg.DevelopmentLoopback && u.Scheme == "http" && isLoopback(u.Host))) {
			return nil, ErrConfig
		}
	}
	cfg.AllowedOrigins = append([]string(nil), cfg.AllowedOrigins...)
	return &Realm{provider: provider, cfg: cfg, sessions: map[string]session{}, now: time.Now}, nil
}
func isLoopback(hostport string) bool {
	host, _, e := net.SplitHostPort(hostport)
	if e != nil {
		host = hostport
	}
	ip := net.ParseIP(host)
	return host == "localhost" || (ip != nil && ip.IsLoopback())
}
func (r *Realm) Mode() string { return r.cfg.Mode }
func (r *Realm) allowed(req *http.Request) bool {
	local := r.cfg.DevelopmentLoopback && isLoopback(req.Host) && isLoopback(req.RemoteAddr)
	if !local && req.TLS == nil {
		return false
	}
	if r.cfg.Mode == "mock" && !local {
		return false
	}
	scheme := "https"
	if req.TLS == nil {
		scheme = "http"
	}
	self := scheme + "://" + req.Host
	hostAllowed := local
	for _, raw := range r.cfg.AllowedOrigins {
		if raw == self {
			hostAllowed = true
		}
	}
	if !hostAllowed || req.Header.Get("Sec-Fetch-Site") == "cross-site" {
		return false
	}
	origin := req.Header.Get("Origin")
	unsafe := req.Method != "GET" && req.Method != "HEAD" && req.Method != "OPTIONS"
	if origin == "" {
		return !unsafe
	}
	if origin == self {
		return true
	}
	for _, raw := range r.cfg.AllowedOrigins {
		if raw == origin {
			if local && !isLoopback(strings.TrimPrefix(strings.TrimPrefix(raw, "https://"), "http://")) {
				return false
			}
			return true
		}
	}
	return false
}
func (r *Realm) prune(now time.Time) {
	for token, s := range r.sessions {
		if !now.Before(s.expires) {
			delete(r.sessions, token)
		}
	}
}
func (r *Realm) Resolve(req *http.Request) (registry.Actor, error) {
	if !r.allowed(req) {
		return registry.Actor{}, errors.New("UNAUTHENTICATED")
	}
	cookie, e := req.Cookie("lunaorbit_session")
	if e != nil {
		return registry.Actor{}, errors.New("UNAUTHENTICATED")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prune(r.now())
	s, ok := r.sessions[cookie.Value]
	if !ok {
		return registry.Actor{}, errors.New("UNAUTHENTICATED")
	}
	return registry.Actor{ID: s.subject.ID, Role: s.subject.Role, Groups: append([]string(nil), s.subject.Groups...)}, nil
}
func authError(c *gin.Context, status int, code string) {
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"code": code, "message": "Authentication could not be completed"}})
}
func (r *Realm) cookie(c *gin.Context, token string, maxAge int) {
	http.SetCookie(c.Writer, &http.Cookie{Name: "lunaorbit_session", Value: token, Path: "/api/v1", HttpOnly: true, Secure: c.Request.TLS != nil, SameSite: http.SameSiteStrictMode, MaxAge: maxAge})
}
func (r *Realm) Mount(router *gin.Engine) {
	group := router.Group("/api/v1/auth")
	group.Use(func(c *gin.Context) {
		if !r.allowed(c.Request) {
			authError(c, 403, "ORIGIN_DENIED")
			return
		}
		c.Next()
	})
	group.POST("/login", func(c *gin.Context) {
		if c.ContentType() != "application/json" {
			authError(c, 415, "JSON_REQUIRED")
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 8192)
		var in struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		dec := json.NewDecoder(c.Request.Body)
		dec.DisallowUnknownFields()
		if e := dec.Decode(&in); e != nil {
			authError(c, 400, "INVALID_ARGUMENT")
			return
		}
		var extra any
		if e := dec.Decode(&extra); e != io.EOF {
			authError(c, 400, "INVALID_ARGUMENT")
			return
		}
		now := r.now()
		r.mu.Lock()
		r.prune(now)
		recent := r.attempts[:0]
		for _, at := range r.attempts {
			if now.Sub(at) < time.Minute {
				recent = append(recent, at)
			}
		}
		r.attempts = recent
		if len(r.attempts) >= r.cfg.LoginPerMinute {
			r.mu.Unlock()
			authError(c, 429, "RATE_LIMITED")
			return
		}
		r.attempts = append(r.attempts, now)
		if len(r.sessions) >= r.cfg.Capacity {
			r.mu.Unlock()
			authError(c, 429, "CAPACITY_EXCEEDED")
			return
		}
		r.mu.Unlock()
		ctx, cancel := context.WithTimeout(c.Request.Context(), 8*time.Second)
		defer cancel()
		s, e := r.provider.Authenticate(ctx, in.Username, in.Password)
		in.Password = ""
		if e != nil {
			if errors.Is(e, ErrCredentials) {
				authError(c, 401, "INVALID_CREDENTIALS")
			} else {
				authError(c, 503, "IDENTITY_UNAVAILABLE")
			}
			return
		}
		if s.ID == "" || !validRole(s.Role) {
			authError(c, 503, "IDENTITY_UNAVAILABLE")
			return
		}
		random := make([]byte, 32)
		if _, e = rand.Read(random); e != nil {
			authError(c, 503, "IDENTITY_UNAVAILABLE")
			return
		}
		token := hex.EncodeToString(random)
		r.mu.Lock()
		r.prune(r.now())
		if len(r.sessions) >= r.cfg.Capacity {
			r.mu.Unlock()
			authError(c, 429, "CAPACITY_EXCEEDED")
			return
		}
		if old, e := c.Request.Cookie("lunaorbit_session"); e == nil {
			delete(r.sessions, old.Value)
		}
		r.sessions[token] = session{subject: s, expires: r.now().Add(r.cfg.TTL)}
		r.mu.Unlock()
		r.cookie(c, token, int(r.cfg.TTL.Seconds()))
		c.JSON(200, gin.H{"user_id": s.ID, "role": s.Role, "mode": r.cfg.Mode})
	})
	group.GET("/session", func(c *gin.Context) {
		a, e := r.Resolve(c.Request)
		if e != nil {
			authError(c, 401, "UNAUTHENTICATED")
			return
		}
		c.JSON(200, gin.H{"user_id": a.ID, "role": a.Role, "mode": r.cfg.Mode})
	})
	group.POST("/logout", func(c *gin.Context) {
		if cookie, e := c.Request.Cookie("lunaorbit_session"); e == nil {
			r.mu.Lock()
			delete(r.sessions, cookie.Value)
			r.mu.Unlock()
		}
		r.cookie(c, "", -1)
		c.Status(204)
	})
}
