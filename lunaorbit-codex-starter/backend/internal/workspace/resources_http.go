package workspace

import (
	"github.com/gin-gonic/gin"
	"lunaorbit/backend/internal/registry"
	"sort"
)

func mountResources(w *gin.RouterGroup, s *Service, actor func(*gin.Context) registry.Actor) {
	w.GET("/resource-configs", func(c *gin.Context) {
		v, e := s.ResourceConfigs(c.Request.Context(), actor(c), c.Param("id"), ResourceTarget{Type: c.Query("resource_type"), Name: c.Query("resource_name")})
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, v)
	})
	w.POST("/resource-plans", func(c *gin.Context) {
		var in ResourceIntent
		if !decode(c, &in) {
			return
		}
		v, e := s.PreviewResource(c.Request.Context(), actor(c), c.Param("id"), in)
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, v)
	})
	w.GET("/resource-operations", func(c *gin.Context) {
		st, e := s.Store.View(c.Request.Context(), c.Param("id"))
		if e != nil {
			fail(c, e)
			return
		}
		out := []ResourceOperation{}
		for _, op := range st.ResourceOperations {
			if c.Query("resource_type") != "" && op.Plan.Input.Resource.Type != c.Query("resource_type") {
				continue
			}
			if c.Request.URL.Query().Has("resource_name") && op.Plan.Input.Resource.Name != c.Query("resource_name") {
				continue
			}
			if s.resourceAuth(c.Request.Context(), actor(c), c.Param("id"), op.Plan.Input.Resource, false) == nil {
				out = append(out, op)
			}
		}
		sort.Slice(out, func(i, j int) bool {
			if out[i].CreatedAt.Equal(out[j].CreatedAt) {
				return out[i].ID < out[j].ID
			}
			return out[i].CreatedAt.After(out[j].CreatedAt)
		})
		page(c, out)
	})
	w.POST("/resource-operations", func(c *gin.Context) {
		var in struct {
			Intent      ResourceIntent `json:"intent"`
			Fingerprint string         `json:"fingerprint"`
			RequestID   string         `json:"request_id"`
		}
		if !decode(c, &in) {
			return
		}
		v, e := s.CreateResourceOperation(c.Request.Context(), actor(c), c.Param("id"), in.Intent, in.Fingerprint, in.RequestID)
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(201, v)
	})
	for _, action := range []string{"approve", "execute", "refresh"} {
		action := action
		w.POST("/resource-operations/:operation/"+action, func(c *gin.Context) {
			var in struct {
				Revision int64 `json:"revision"`
			}
			if !decode(c, &in) {
				return
			}
			ctx, a, id, key := c.Request.Context(), actor(c), c.Param("id"), c.Param("operation")
			var v ResourceOperation
			var e error
			switch action {
			case "approve":
				v, e = s.ApproveResourceOperation(ctx, a, id, key, in.Revision)
			case "execute":
				v, e = s.ExecuteResourceOperation(ctx, a, id, key, in.Revision)
			case "refresh":
				v, e = s.RefreshResourceOperation(ctx, a, id, key, in.Revision)
			}
			if e != nil {
				fail(c, e)
				return
			}
			c.JSON(200, v)
		})
	}
}
