package access

import (
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
)

func fail(c *gin.Context, e error) {
	code := e.Error()
	var op *kafkaops.Error
	if errors.As(e, &op) {
		code = op.Code
	}
	if v := registry.ErrorCode(e); v != "INTERNAL_ERROR" && v != "" {
		code = v
	}
	status := 503
	switch code {
	case "FORBIDDEN", "SELF_APPROVAL_DENIED", "AUTHORIZATION_DENIED":
		status = 403
	case "NOT_FOUND":
		status = 404
	case "INVALID_ARGUMENT":
		status = 400
	case "CONFLICT", "DISABLED":
		status = 409
	case "STORAGE_UNAVAILABLE", "NETWORK_ERROR", "NETWORK_DENIED", "BROKER_UNREACHABLE", "ADVERTISED_UNREACHABLE", "TLS_HANDSHAKE_FAILED", "TLS_FAILED", "AUTHENTICATION_FAILED":
		status = 503
	case "UNSUPPORTED_API", "AUTHORIZER_UNVERIFIED":
		status = 501
	case "TIMEOUT":
		status = 504
	default:
		code = "STORAGE_UNAVAILABLE"
	}
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"code": code, "message": "Request could not be completed"}})
}
func decode(c *gin.Context, v any) bool {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10)
	d := json.NewDecoder(c.Request.Body)
	d.DisallowUnknownFields()
	if d.Decode(v) != nil {
		c.AbortWithStatusJSON(400, gin.H{"error": gin.H{"code": "INVALID_ARGUMENT"}})
		return false
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		c.AbortWithStatusJSON(400, gin.H{"error": gin.H{"code": "INVALID_ARGUMENT"}})
		return false
	}
	return true
}
func pagination(c *gin.Context) (int, int, string, bool) {
	p, e := strconv.Atoi(c.DefaultQuery("page", "1"))
	z, se := strconv.Atoi(c.DefaultQuery("page_size", "20"))
	q := c.Query("query")
	if e != nil || se != nil || p < 1 || p > 100000 || z < 1 || z > 100 || len(q) > 128 {
		c.AbortWithStatusJSON(400, gin.H{"error": gin.H{"code": "INVALID_ARGUMENT"}})
		return 0, 0, "", false
	}
	return p, z, q, true
}
func bounds(p, z, n int) (int, int) {
	a := (p - 1) * z
	if a > n {
		a = n
	}
	b := a + z
	if b > n {
		b = n
	}
	return a, b
}
func Mount(g *gin.RouterGroup, s *Service, actor func(*gin.Context) registry.Actor) {
	g.POST("/acl-requests/:request/execute", func(c *gin.Context) {
		var in struct {
			Revision int64 `json:"revision"`
		}
		if !decode(c, &in) {
			return
		}
		r, e := s.Execute(c.Request.Context(), actor(c), c.Param("request"), in.Revision)
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, r)
	})
	g.GET("/acl-requests/inbox", func(c *gin.Context) {
		page, size, query, ok := pagination(c)
		if !ok {
			return
		}
		rows, e := s.Inbox(c.Request.Context(), actor(c))
		if e != nil {
			fail(c, e)
			return
		}
		filtered := []Request{}
		for _, row := range rows {
			if strings.Contains(strings.ToLower(row.Rule.Principal+" "+row.Rule.ResourceName+" "+row.Status), strings.ToLower(query)) {
				filtered = append(filtered, row)
			}
		}
		total := len(filtered)
		start, end := bounds(page, size, total)
		c.JSON(200, gin.H{"items": filtered[start:end], "total": total, "page": page, "page_size": size})
	})
	g.GET("/kafka/clusters/:id/acls", func(c *gin.Context) {
		p, z, q, ok := pagination(c)
		if !ok {
			return
		}
		v, e := s.Read(c.Request.Context(), actor(c), c.Param("id"), c.Query("scenario"))
		if e != nil {
			fail(c, e)
			return
		}
		items := v.Items[:0]
		for _, r := range v.Items {
			if strings.Contains(strings.ToLower(r.Principal+" "+r.ResourceName), strings.ToLower(q)) {
				items = append(items, r)
			}
		}
		n := len(items)
		a, b := bounds(p, z, n)
		c.JSON(200, gin.H{"items": items[a:b], "total": n, "page": p, "page_size": z, "synthetic": v.Synthetic, "state": v.State, "reason": v.Reason, "observed_at": v.ObservedAt})
	})
	g.POST("/kafka/clusters/:id/acl-requests", func(c *gin.Context) {
		var in Input
		if !decode(c, &in) {
			return
		}
		r, e := s.Submit(c.Request.Context(), actor(c), c.Param("id"), in)
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, r)
	})
	g.GET("/kafka/clusters/:id/acl-requests", func(c *gin.Context) {
		p, z, q, ok := pagination(c)
		if !ok {
			return
		}
		rows, e := s.List(c.Request.Context(), actor(c), c.Param("id"))
		if e != nil {
			fail(c, e)
			return
		}
		items := []Request{}
		for _, r := range rows {
			if strings.Contains(strings.ToLower(r.Rule.Principal+" "+r.Rule.ResourceName+" "+r.Status), strings.ToLower(q)) {
				items = append(items, r)
			}
		}
		sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.After(items[j].CreatedAt) })
		n := len(items)
		a, b := bounds(p, z, n)
		c.JSON(200, gin.H{"items": items[a:b], "total": n, "page": p, "page_size": z, "can_approve": false})
	})
	g.POST("/acl-requests/:request/approve", func(c *gin.Context) {
		var in struct {
			Revision int64 `json:"revision"`
		}
		if !decode(c, &in) {
			return
		}
		r, e := s.Approve(c.Request.Context(), actor(c), c.Param("request"), in.Revision)
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, r)
	})
}
