package registry

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// IdentityProvider is a future authenticated production integration boundary.
// Dev sessions below are deliberately only enabled by loopback Mock startup.
type IdentityProvider interface {
	Resolve(*http.Request) (Actor, error)
}
type devSession struct {
	Actor   Actor
	expires time.Time
}
type devSessions struct {
	mu     sync.Mutex
	values map[string]devSession
}

var registrationID = regexp.MustCompile(`^[a-f0-9]{32}$`)

func apiFailure(c *gin.Context, status int, code string) {
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"code": code, "message": "Request could not be completed", "request_id": c.GetString("request_id")}})
}
func apiError(c *gin.Context, err error) {
	code := ErrorCode(err)
	status := 500
	switch code {
	case "INVALID_ARGUMENT", "INVALID_CREDENTIALS":
		status = 400
	case "FORBIDDEN":
		status = 403
	case "NOT_FOUND":
		status = 404
	case "CONFLICT", "PROBE_IN_PROGRESS", "DISABLED":
		status = 409
	case "RATE_LIMITED", "CAPACITY_EXCEEDED":
		status = 429
	case "SECRET_UNAVAILABLE", "STORAGE_UNAVAILABLE":
		status = 503
	}
	apiFailure(c, status, code)
}
func body(c *gin.Context, target any) bool {
	if c.ContentType() != "application/json" {
		apiFailure(c, 415, "JSON_REQUIRED")
		return false
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 256<<10)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		apiFailure(c, 400, "INVALID_ARGUMENT")
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		apiFailure(c, 400, "INVALID_ARGUMENT")
		return false
	}
	return true
}
func loopbackHost(hostport string) bool {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		host = hostport
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
func safeBrowserRequest(c *gin.Context, allowedOrigins []string) bool {
	if !loopbackHost(c.Request.Host) {
		return false
	}
	if site := c.GetHeader("Sec-Fetch-Site"); site == "cross-site" {
		return false
	}
	if raw := c.GetHeader("Origin"); raw != "" {
		origin, err := url.Parse(raw)
		if err != nil || origin.User != nil || origin.Path != "" || origin.RawQuery != "" || origin.Fragment != "" || (origin.Scheme != "http" && origin.Scheme != "https") {
			return false
		}
		if origin.Host != c.Request.Host {
			allowed := false
			for _, candidate := range allowedOrigins {
				if raw == candidate && loopbackHost(origin.Host) {
					allowed = true
				}
			}
			if !allowed {
				return false
			}
		}
	}
	return true
}

// Mount installs a development-only authenticated registry. Production must
// supply its own IdentityProvider and SecretStore rather than exposing this mode.
type Extension func(*gin.RouterGroup, func(*gin.Context) Actor)

func Mount(router *gin.Engine, service *Service, persistent bool, sessionKeys ...[]byte) error {
	return mount(router, service, persistent, sessionKeys, nil)
}
func MountModules(router *gin.Engine, service *Service, persistent bool, key []byte, extensions ...Extension) error {
	var keys [][]byte
	if len(key) > 0 {
		keys = [][]byte{key}
	}
	return mount(router, service, persistent, keys, extensions)
}
func mount(router *gin.Engine, service *Service, persistent bool, sessionKeys [][]byte, extensions []Extension) error {
	key := make([]byte, 32)
	if len(sessionKeys) == 1 && len(sessionKeys[0]) == 32 {
		copy(key, sessionKeys[0])
	} else {
		if service.IdentityProvider == nil && (persistent || len(sessionKeys) > 0) {
			return errors.New("development session signing key required")
		}
		if _, err := rand.Read(key); err != nil {
			return errors.New("development session initialization failed")
		}
	}
	sessions := &devSessions{values: make(map[string]devSession)}
	resolve := func(c *gin.Context) (Actor, bool) {
		if service.IdentityProvider != nil {
			actor, e := service.IdentityProvider.Resolve(c.Request)
			return actor, e == nil && actor.ID != ""
		}
		token, err := c.Cookie("lunaorbit_dev_session")
		if err != nil {
			return Actor{}, false
		}
		return sessionActor(token, key)
	}
	group := router.Group("/api/v1")
	group.Use(func(c *gin.Context) {
		if service.IdentityProvider == nil && !safeBrowserRequest(c, service.BrowserOrigins) {
			apiFailure(c, 403, "ORIGIN_DENIED")
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 8*time.Second)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})
	if service.IdentityProvider == nil {
		group.POST("/dev/session", func(c *gin.Context) {
			var input struct{}
			if !body(c, &input) {
				return
			}
			if actor, ok := resolve(c); ok {
				c.JSON(200, gin.H{"user_id": actor.ID, "mode": "mock", "persistent": persistent, "lab_catalog": service.CanSeedLabCatalog(actor)})
				return
			}
			sessions.mu.Lock()
			defer sessions.mu.Unlock()
			now := time.Now()
			for token, entry := range sessions.values {
				if now.After(entry.expires) {
					delete(sessions.values, token)
				}
			}
			if len(sessions.values) >= 128 {
				apiFailure(c, 429, "CAPACITY_EXCEEDED")
				return
			}
			user := randomID()
			if user == "" {
				apiFailure(c, 500, "INTERNAL_ERROR")
				return
			}
			actor := Actor{ID: user, Role: "admin"}
			token := signedSession(user, now.Add(8*time.Hour), key)
			sessions.values[token] = devSession{Actor: actor, expires: now.Add(8 * time.Hour)}
			http.SetCookie(c.Writer, &http.Cookie{Name: "lunaorbit_dev_session", Value: token, Path: "/api/v1", HttpOnly: true, Secure: c.Request.TLS != nil, SameSite: http.SameSiteStrictMode, MaxAge: 8 * 60 * 60})
			c.JSON(200, gin.H{"user_id": actor.ID, "mode": "mock", "persistent": persistent, "lab_catalog": service.CanSeedLabCatalog(actor)})
		})
	}
	protected := group.Group("")
	protected.Use(func(c *gin.Context) {
		actor, ok := resolve(c)
		if !ok {
			apiFailure(c, 401, "UNAUTHENTICATED")
			return
		}
		c.Set("registry_actor", actor)
		c.Next()
	})
	actor := func(c *gin.Context) Actor { return c.MustGet("registry_actor").(Actor) }
	for _, extension := range extensions {
		extension(protected, actor)
	}
	protected.GET("/session", func(c *gin.Context) {
		mode := "mock"
		if service.IdentityProvider != nil {
			mode = "identity"
			if named, ok := service.IdentityProvider.(interface{ Mode() string }); ok {
				mode = named.Mode()
			}
		}
		c.JSON(200, gin.H{"user_id": actor(c).ID, "role": actor(c).Role, "mode": mode, "persistent": persistent, "lab_catalog": service.CanSeedLabCatalog(actor(c))})
	})
	if service.HasLabCatalog() {
		protected.POST("/lab/registrations", func(c *gin.Context) {
			var input struct{}
			if !body(c, &input) {
				return
			}
			rows, e := service.SeedLabCatalog(c.Request.Context(), actor(c))
			if e != nil {
				apiError(c, e)
				return
			}
			c.JSON(200, gin.H{"items": rows})
		})
	}
	clusters := protected.Group("/kafka/clusters")
	clusters.GET("", func(c *gin.Context) {
		page, e := strconv.Atoi(c.DefaultQuery("page", "1"))
		size, se := strconv.Atoi(c.DefaultQuery("page_size", "20"))
		query := c.Query("query")
		if e != nil || se != nil || page < 1 || page > 100000 || size < 1 || size > 100 || len(query) > 128 {
			apiFailure(c, 400, "INVALID_ARGUMENT")
			return
		}
		rows, err := service.List(c.Request.Context(), actor(c))
		if err != nil {
			apiError(c, err)
			return
		}
		filtered := make([]Cluster, 0, len(rows))
		for _, row := range rows {
			if strings.Contains(strings.ToLower(row.DisplayName+" "+row.Room+" "+row.Environment), strings.ToLower(query)) {
				filtered = append(filtered, row)
			}
		}
		sort.Slice(filtered, func(i, j int) bool { return filtered[i].CreatedAt.After(filtered[j].CreatedAt) })
		total := len(filtered)
		start := (page - 1) * size
		if start > total {
			start = total
		}
		end := start + size
		if end > total {
			end = total
		}
		c.JSON(200, gin.H{"items": filtered[start:end], "total": total, "page": page, "page_size": size})
	})
	clusters.POST("", func(c *gin.Context) {
		var input CreateInput
		if !body(c, &input) {
			return
		}
		row, err := service.Create(c.Request.Context(), actor(c), input)
		if err != nil {
			apiError(c, err)
			return
		}
		c.JSON(201, row)
	})
	item := clusters.Group("/:id")
	item.Use(func(c *gin.Context) {
		if !registrationID.MatchString(c.Param("id")) {
			apiFailure(c, 404, "NOT_FOUND")
			return
		}
		c.Next()
	})
	mountOperations(item, service, actor)
	MountResourceGrants(item, service, actor)
	item.GET("", func(c *gin.Context) {
		row, err := service.Get(c.Request.Context(), actor(c), c.Param("id"))
		if err != nil {
			apiError(c, err)
			return
		}
		c.JSON(200, row)
	})
	item.PATCH("", func(c *gin.Context) {
		var input PatchInput
		if !body(c, &input) {
			return
		}
		row, err := service.Patch(c.Request.Context(), actor(c), c.Param("id"), input)
		if err != nil {
			apiError(c, err)
			return
		}
		c.JSON(200, row)
	})
	item.PUT("/credentials", func(c *gin.Context) {
		var input CredentialUpdate
		if !body(c, &input) {
			return
		}
		row, err := service.ReplaceCredentials(c.Request.Context(), actor(c), c.Param("id"), input)
		if err != nil {
			apiError(c, err)
			return
		}
		c.JSON(200, row)
	})
	item.DELETE("", func(c *gin.Context) {
		var input struct {
			Revision int64 `json:"revision"`
		}
		if !body(c, &input) {
			return
		}
		if err := service.Delete(c.Request.Context(), actor(c), c.Param("id"), input.Revision); err != nil {
			apiError(c, err)
			return
		}
		c.Status(204)
	})
	item.POST("/probe", func(c *gin.Context) {
		var input struct {
			Scenario string `json:"mock_scenario"`
		}
		if !body(c, &input) {
			return
		}
		job, err := service.StartProbe(c.Request.Context(), actor(c), c.Param("id"), input.Scenario)
		if err != nil {
			apiError(c, err)
			return
		}
		c.JSON(202, job)
	})
	item.GET("/probe", func(c *gin.Context) {
		job, err := service.ProbeStatus(c.Request.Context(), actor(c), c.Param("id"))
		if err != nil {
			apiError(c, err)
			return
		}
		c.JSON(200, job)
	})
	return nil
}
