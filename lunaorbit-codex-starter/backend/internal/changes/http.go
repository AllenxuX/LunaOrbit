package changes

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"io"
	"lunaorbit/backend/internal/registry"
	"net/http"
	"strconv"
	"strings"
)

func failure(c *gin.Context, e error) {
	code := safeReason(e)
	status := 500
	switch code {
	case "UNAUTHENTICATED":
		status = 401
	case "FORBIDDEN", "SELF_APPROVAL_DENIED", "WRITES_DISABLED":
		status = 403
	case "NOT_FOUND":
		status = 404
	case "CONFLICT":
		status = 409
	case "INVALID_ARGUMENT":
		status = 400
	case "TIMEOUT":
		status = 504
	case "STORAGE_UNAVAILABLE", "ADAPTER_UNAVAILABLE":
		status = 503
	}
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"code": code, "message": "Request could not be completed"}})
}
func decode(c *gin.Context, v any) bool {
	if c.ContentType() != "application/json" {
		c.AbortWithStatusJSON(415, gin.H{"error": gin.H{"code": "JSON_REQUIRED"}})
		return false
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 8<<10)
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
func Mount(g *gin.RouterGroup, s *Service, actor func(*gin.Context) registry.Actor) {
	g.GET("/changes/targets", func(c *gin.Context) {
		v, e := s.Targets(c.Request.Context(), actor(c))
		if e != nil {
			failure(c, e)
			return
		}
		c.JSON(200, gin.H{"items": v})
	})
	g.POST("/changes", func(c *gin.Context) {
		var in Input
		if !decode(c, &in) {
			return
		}
		r, e := s.Submit(c.Request.Context(), actor(c), in)
		if e != nil {
			failure(c, e)
			return
		}
		c.JSON(200, r)
	})
	g.GET("/changes", func(c *gin.Context) {
		page, e := strconv.Atoi(c.DefaultQuery("page", "1"))
		size, se := strconv.Atoi(c.DefaultQuery("page_size", "20"))
		q := c.Query("query")
		if e != nil || se != nil || page < 1 || page > 100000 || size < 1 || size > 100 || len(q) > 128 {
			c.AbortWithStatusJSON(400, gin.H{"error": gin.H{"code": "INVALID_ARGUMENT"}})
			return
		}
		rows, e := s.List(c.Request.Context(), actor(c))
		if e != nil {
			failure(c, e)
			return
		}
		filtered := []Request{}
		for _, r := range rows {
			if strings.Contains(strings.ToLower(r.TargetID+" "+r.Status), strings.ToLower(q)) {
				filtered = append(filtered, r)
			}
		}
		total := len(filtered)
		a := (page - 1) * size
		if a > total {
			a = total
		}
		b := a + size
		if b > total {
			b = total
		}
		c.JSON(200, gin.H{"items": filtered[a:b], "total": total, "page": page, "page_size": size})
	})
	g.GET("/changes/:id", func(c *gin.Context) {
		r, e := s.Get(c.Request.Context(), actor(c), c.Param("id"))
		if e != nil {
			failure(c, e)
			return
		}
		c.JSON(200, r)
	})
	g.POST("/changes/:id/approve", func(c *gin.Context) {
		var in struct {
			Revision int64 `json:"revision"`
		}
		if !decode(c, &in) {
			return
		}
		r, e := s.Approve(c.Request.Context(), actor(c), c.Param("id"), in.Revision)
		if e != nil {
			failure(c, e)
			return
		}
		c.JSON(200, r)
	})
	g.POST("/changes/:id/execute", func(c *gin.Context) {
		var in struct {
			Revision int64  `json:"revision"`
			Scenario string `json:"scenario"`
		}
		if !decode(c, &in) {
			return
		}
		r, e := s.Execute(c.Request.Context(), actor(c), c.Param("id"), in.Revision, in.Scenario)
		if e != nil {
			failure(c, e)
			return
		}
		status := 200
		if r.Status == "conflict" {
			status = 409
		}
		c.JSON(status, r)
	})
}
