package discovery

import (
	"context"
	"lunaorbit/backend/internal/discovery/mm2auth"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// TopicSummary is one topic of an auto endpoint.
type TopicSummary struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
	Internal   bool   `json:"internal"`
}

// EndpointAccess performs read-only Kafka queries for auto endpoints. Implementations must
// use the read-only allowlist client; the credential never leaves this call.
type EndpointAccess interface {
	Probe(ctx context.Context, ep AutoEndpoint, cred mm2auth.Credential) ProbeResult
	Topics(ctx context.Context, ep AutoEndpoint, cred mm2auth.Credential) ([]TopicSummary, error)
	Topic(ctx context.Context, ep AutoEndpoint, cred mm2auth.Credential, name string) (any, error)
	Groups(ctx context.Context, ep AutoEndpoint, cred mm2auth.Credential) ([]string, error)
}

// SetEndpointAccess installs the read-only Kafka access used when credentials are enabled.
func (s *Service) SetEndpointAccess(a EndpointAccess) {
	s.scopeMu.Lock()
	s.access = a
	s.scopeMu.Unlock()
}

// probeEndpoints connects read-only to every endpoint that is ready (switch on only).
func (s *Service) probeEndpoints(ctx context.Context, eps []AutoEndpoint) {
	s.scopeMu.RLock()
	creds, access := s.mm2Creds, s.access
	s.scopeMu.RUnlock()
	if creds == nil || access == nil {
		return
	}
	for _, ep := range eps {
		if ep.Access != AccessPending {
			continue
		}
		cred, ok := creds.Get(ep.Key)
		if !ok {
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		result := access.Probe(pctx, ep, cred)
		cancel()
		if result.CheckedAt.IsZero() {
			result.CheckedAt = time.Now().UTC()
		}
		s.scopeMu.Lock()
		if s.probes == nil {
			s.probes = map[string]ProbeResult{}
		}
		s.probes[ep.Key] = result
		s.scopeMu.Unlock()
	}
}

// Catalog merges auto endpoints with the owner's registrations.
func (s *Service) Catalog(ctx context.Context, actor Actor) ([]CatalogEndpoint, error) {
	var regs []Registration
	if s.lookup != nil {
		rows, e := s.lookup(ctx, actor.ID)
		if e != nil {
			return nil, fail("REGISTRY_UNAVAILABLE")
		}
		regs = rows
	}
	eps := s.Endpoints()
	s.scopeMu.RLock()
	probes := make(map[string]ProbeResult, len(s.probes))
	for k, v := range s.probes {
		probes[k] = v
	}
	s.scopeMu.RUnlock()
	return BuildCatalog(eps, regs, probes), nil
}

// catalogReader resolves a catalog row to a readable auto endpoint and its credential.
func (s *Service) catalogReader(c *gin.Context, actor Actor) (AutoEndpoint, mm2auth.Credential, EndpointAccess, bool) {
	rows, e := s.Catalog(c.Request.Context(), actor)
	if e != nil {
		apiError(c, e)
		return AutoEndpoint{}, mm2auth.Credential{}, nil, false
	}
	var row *CatalogEndpoint
	for i := range rows {
		if rows[i].ID == c.Param("endpoint") {
			row = &rows[i]
		}
	}
	if row == nil {
		apiError(c, fail("NOT_FOUND"))
		return AutoEndpoint{}, mm2auth.Credential{}, nil, false
	}
	s.scopeMu.RLock()
	creds, access := s.mm2Creds, s.access
	s.scopeMu.RUnlock()
	conflict := func(code string, extra gin.H) {
		body := gin.H{"error": gin.H{"code": code, "message": "Endpoint cannot be read directly", "request_id": c.GetString("request_id")}}
		for k, v := range extra {
			body[k] = v
		}
		c.AbortWithStatusJSON(409, body)
	}
	if row.ReadVia == "registration" {
		conflict("USE_REGISTRATION", gin.H{"registration_id": row.RegistrationID})
		return AutoEndpoint{}, mm2auth.Credential{}, nil, false
	}
	if creds == nil || access == nil {
		conflict("CREDENTIALS_OFF", nil)
		return AutoEndpoint{}, mm2auth.Credential{}, nil, false
	}
	byKey := map[string]AutoEndpoint{}
	for _, ep := range s.Endpoints() {
		byKey[ep.Key] = ep
	}
	for _, key := range row.Members {
		if cred, ok := creds.Get(key); ok {
			ep := byKey[key]
			ep.Addresses = row.Addresses
			return ep, cred, access, true
		}
	}
	conflict("CREDENTIALS_OFF", nil)
	return AutoEndpoint{}, mm2auth.Credential{}, nil, false
}

func mountCatalog(group *gin.RouterGroup, service *Service, actor func(*gin.Context) Actor) {
	group.GET("/catalog", func(c *gin.Context) {
		rows, e := service.Catalog(c.Request.Context(), actor(c))
		if e != nil {
			apiError(c, e)
			return
		}
		c.JSON(200, gin.H{"items": rows})
	})
	ep := group.Group("/catalog/:endpoint")
	readErr := func(c *gin.Context) {
		c.AbortWithStatusJSON(502, gin.H{"error": gin.H{"code": "ENDPOINT_UNREADABLE", "message": "Kafka endpoint could not be read", "request_id": c.GetString("request_id")}})
	}
	ep.GET("/topics", func(c *gin.Context) {
		page, e := strconv.Atoi(c.DefaultQuery("page", "1"))
		size, se := strconv.Atoi(c.DefaultQuery("page_size", "20"))
		if e != nil || se != nil || page < 1 || size < 1 || size > 100 {
			apiError(c, fail("INVALID_ARGUMENT"))
			return
		}
		target, cred, access, ok := service.catalogReader(c, actor(c))
		if !ok {
			return
		}
		topics, err := access.Topics(c.Request.Context(), target, cred)
		if err != nil {
			readErr(c)
			return
		}
		q := strings.ToLower(strings.TrimSpace(c.Query("query")))
		filtered := []TopicSummary{}
		for _, t := range topics {
			if q == "" || strings.Contains(strings.ToLower(t.Name), q) {
				filtered = append(filtered, t)
			}
		}
		start, end := min((page-1)*size, len(filtered)), min(page*size, len(filtered))
		c.JSON(200, gin.H{"items": filtered[start:end], "total": len(filtered), "page": page, "page_size": size})
	})
	ep.GET("/topics/:topic", func(c *gin.Context) {
		target, cred, access, ok := service.catalogReader(c, actor(c))
		if !ok {
			return
		}
		detail, err := access.Topic(c.Request.Context(), target, cred, c.Param("topic"))
		if err != nil {
			readErr(c)
			return
		}
		c.JSON(200, detail)
	})
	ep.GET("/groups", func(c *gin.Context) {
		target, cred, access, ok := service.catalogReader(c, actor(c))
		if !ok {
			return
		}
		groups, err := access.Groups(c.Request.Context(), target, cred)
		if err != nil {
			readErr(c)
			return
		}
		c.JSON(200, gin.H{"items": groups})
	})
}
