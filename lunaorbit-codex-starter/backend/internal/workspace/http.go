package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"io"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

func fail(c *gin.Context, e error) {
	code := e.Error()
	var op *kafkaops.Error
	if errors.As(e, &op) {
		code = op.Code
	}
	if rc := registry.ErrorCode(e); rc != "" && rc != "INTERNAL_ERROR" {
		code = rc
	}
	status := 503
	switch code {
	case "INVALID_ARGUMENT":
		status = 400
	case "FORBIDDEN", "AUTHORIZATION_DENIED", "SELF_APPROVAL_DENIED":
		status = 403
	case "NOT_FOUND":
		status = 404
	case "CONFLICT", "DISABLED":
		status = 409
	case "CAPACITY_EXCEEDED", "RATE_LIMITED":
		status = 429
	case "UNSUPPORTED_API", "AUTHORIZER_UNVERIFIED":
		status = 501
	case "STORAGE_UNAVAILABLE", "NETWORK_ERROR", "NETWORK_DENIED", "BROKER_UNREACHABLE", "ADVERTISED_UNREACHABLE", "TLS_FAILED", "TLS_HANDSHAKE_FAILED", "AUTHENTICATION_FAILED", "TIMEOUT":
	default:
		code = "STORAGE_UNAVAILABLE"
	}
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"code": code, "message": "Request could not be completed"}})
}
func decode(c *gin.Context, v any) bool {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 64<<10)
	d := json.NewDecoder(c.Request.Body)
	d.DisallowUnknownFields()
	if d.Decode(v) != nil {
		fail(c, errInvalid)
		return false
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		fail(c, errInvalid)
		return false
	}
	return true
}
func page[T any](c *gin.Context, items []T) {
	p, e := strconv.Atoi(c.DefaultQuery("page", "1"))
	z, ee := strconv.Atoi(c.DefaultQuery("page_size", "20"))
	if e != nil || ee != nil || p < 1 || p > 100000 || z < 1 || z > 100 {
		fail(c, errInvalid)
		return
	}
	start := (p - 1) * z
	if start > len(items) {
		start = len(items)
	}
	end := start + z
	if end > len(items) {
		end = len(items)
	}
	c.JSON(200, gin.H{"items": items[start:end], "total": len(items), "page": p, "page_size": z})
}
func Mount(g *gin.RouterGroup, s *Service, actor func(*gin.Context) registry.Actor) {
	w := g.Group("/kafka/clusters/:id/workspace")
	w.Use(func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 35*time.Second)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		if e := s.auth(ctx, actor(c), c.Param("id"), "read", ""); e != nil {
			fail(c, e)
			return
		}
		c.Next()
	})
	mountAccounts(w, s, actor)
	mountResources(w, s, actor)
	w.GET("/meta", func(c *gin.Context) {
		ctx := c.Request.Context()
		a := actor(c)
		id := c.Param("id")
		permissions := map[string]bool{}
		for key, action := range map[string]string{"read": "read", "topic_manage": "topic.manage", "applications_manage": "applications.manage", "acl_manage": "acl.manage", "audit_read": "audit.read", "team_manage": "team.manage"} {
			permissions[key] = s.auth(ctx, a, id, action, "") == nil
		}
		permissions["account_manage"] = s.Registry.AuthorizeUnscopedResource(ctx, a, id, "account.manage") == nil
		permissions["credentials_read"] = s.Registry.AuthorizeUnscopedResource(ctx, a, id, "credentials.read") == nil
		permissions["applications_manage"] = s.Registry.AuthorizeUnscopedResource(ctx, a, id, "applications.manage") == nil
		enabled := s.WriteEnabled != nil && s.WriteEnabled(ctx, a, id) == nil
		c.JSON(200, gin.H{"synthetic": s.Gateway != nil && s.Gateway.Synthetic(), "write_enabled": enabled, "permissions": permissions, "approval_required": !(s.LabAutoApprove && enabled), "capabilities": gin.H{"topic_manage": s.Gateway != nil, "acl_manage": s.Gateway != nil}})
	})
	w.GET("/apps", func(c *gin.Context) {
		if len(c.Query("query")) > 128 {
			fail(c, errInvalid)
			return
		}
		st, e := s.Store.View(c.Request.Context(), c.Param("id"))
		if e != nil {
			fail(c, e)
			return
		}
		v := []Application{}
		for _, a := range st.Applications {
			if strings.Contains(strings.ToLower(a.Name+" "+a.Principal+" "+a.Owner+" "+a.Description), strings.ToLower(c.Query("query"))) {
				v = append(v, a)
			}
		}
		sort.Slice(v, func(i, j int) bool { return v[i].Name < v[j].Name })
		page(c, v)
	})
	saveApp := func(c *gin.Context) {
		var in Application
		if !decode(c, &in) {
			return
		}
		if key := c.Param("app"); key != "" {
			if in.ID != "" && in.ID != key {
				fail(c, errInvalid)
				return
			}
			in.ID = key
		} else if in.ID != "" || in.Revision != 0 {
			fail(c, errInvalid)
			return
		}
		v, e := s.SaveApplication(c.Request.Context(), actor(c), c.Param("id"), in)
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, v)
	}
	w.POST("/apps", saveApp)
	w.PATCH("/apps/:app", saveApp)
	w.GET("/apps/:app", func(c *gin.Context) {
		st, e := s.Store.View(c.Request.Context(), c.Param("id"))
		if e != nil {
			fail(c, e)
			return
		}
		v, ok := st.Applications[c.Param("app")]
		if !ok {
			fail(c, errors.New("NOT_FOUND"))
			return
		}
		c.JSON(200, v)
	})
	w.DELETE("/apps/:app", func(c *gin.Context) {
		revision, e := strconv.ParseInt(c.Query("revision"), 10, 64)
		if e != nil {
			fail(c, errInvalid)
			return
		}
		if e = s.DeleteApplication(c.Request.Context(), actor(c), c.Param("id"), c.Param("app"), revision); e != nil {
			fail(c, e)
			return
		}
		c.Status(204)
	})
	w.POST("/plans", func(c *gin.Context) {
		var in Input
		if !decode(c, &in) {
			return
		}
		v, e := s.Preview(c.Request.Context(), actor(c), c.Param("id"), in)
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, v)
	})
	w.POST("/requests", func(c *gin.Context) {
		var in Input
		if !decode(c, &in) {
			return
		}
		v, e := s.CreateRequest(c.Request.Context(), actor(c), c.Param("id"), in)
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(201, v)
	})
	w.GET("/requests", func(c *gin.Context) {
		st, e := s.Store.View(c.Request.Context(), c.Param("id"))
		if e != nil {
			fail(c, e)
			return
		}
		v := []Request{}
		for _, r := range st.Requests {
			if s.authorizePlanAction(c.Request.Context(), actor(c), c.Param("id"), r.Plan, "read") == nil {
				v = append(v, r)
			}
		}
		sort.Slice(v, func(i, j int) bool { return v[i].CreatedAt.After(v[j].CreatedAt) })
		page(c, v)
	})
	w.GET("/requests/:request", func(c *gin.Context) {
		st, e := s.Store.View(c.Request.Context(), c.Param("id"))
		if e != nil {
			fail(c, e)
			return
		}
		v, ok := st.Requests[c.Param("request")]
		if !ok {
			fail(c, errors.New("NOT_FOUND"))
			return
		}
		if e = s.authorizePlanAction(c.Request.Context(), actor(c), c.Param("id"), v.Plan, "read"); e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, v)
	})
	for _, action := range []string{"approve", "execute"} {
		action := action
		w.POST("/requests/:request/"+action, func(c *gin.Context) {
			var in struct {
				Revision int64 `json:"revision"`
			}
			if !decode(c, &in) {
				return
			}
			var v Request
			var e error
			if action == "approve" {
				v, e = s.Approve(c.Request.Context(), actor(c), c.Param("id"), c.Param("request"), in.Revision)
			} else {
				v, e = s.Execute(c.Request.Context(), actor(c), c.Param("id"), c.Param("request"), in.Revision)
			}
			if e != nil {
				fail(c, e)
				return
			}
			c.JSON(200, v)
		})
	}
	w.GET("/topic-configs", func(c *gin.Context) {
		topic := c.Query("topic")
		if !nameRE.MatchString(topic) {
			fail(c, errInvalid)
			return
		}
		if e := s.auth(c.Request.Context(), actor(c), c.Param("id"), "read", "topic:"+topic); e != nil {
			fail(c, e)
			return
		}
		v, e := s.Gateway.TopicConfigs(c.Request.Context(), actor(c), c.Param("id"), topic)
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, gin.H{"topic": topic, "configs": v, "synthetic": s.Gateway.Synthetic()})
	})
	w.GET("/acl-capabilities", func(c *gin.Context) {
		c.JSON(200, gin.H{"resources": kafkaops.ACLCapabilities(), "pattern_types": []string{"LITERAL", "PREFIXED"}, "permissions": []string{"ALLOW", "DENY"}})
	})
	w.GET("/acls", func(c *gin.Context) {
		snap, e := s.ListACLRules(c.Request.Context(), actor(c), c.Param("id"), c.Query("principal"))
		if e != nil {
			fail(c, e)
			return
		}
		page(c, snap.Items)
	})
	w.GET("/audit", func(c *gin.Context) {
		if e := s.auth(c.Request.Context(), actor(c), c.Param("id"), "audit.read", ""); e != nil {
			fail(c, e)
			return
		}
		st, e := s.Store.View(c.Request.Context(), c.Param("id"))
		if e != nil {
			fail(c, e)
			return
		}
		v := []Audit{}
		unrestricted := s.Registry.AuthorizeUnscopedResource(c.Request.Context(), actor(c), c.Param("id"), "audit.read") == nil
		for _, entry := range st.Audit {
			if unrestricted {
				v = append(v, entry)
				continue
			}
			r, ok := st.Requests[entry.Target]
			if ok && s.authorizePlanAction(c.Request.Context(), actor(c), c.Param("id"), r.Plan, "audit.read") == nil {
				v = append(v, entry)
			}
		}
		sort.SliceStable(v, func(i, j int) bool { return v[i].At.After(v[j].At) })
		page(c, v)
	})
}
