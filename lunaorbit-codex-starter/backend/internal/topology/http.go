package topology

import (
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"io"
	"lunaorbit/backend/internal/discovery"
	"net/http"
)

// Mount uses the platform's authenticated group and Origin protection.
func Mount(group *gin.RouterGroup, store Store, source *discovery.Service, actor func(*gin.Context) discovery.Actor) {
	routes := group.Group("/topology/scopes/:scope/view")
	routes.Use(func(c *gin.Context) {
		if _, err := source.Snapshot(c.Request.Context(), actor(c), c.Param("scope")); err != nil {
			status := 503
			if discovery.CodeOf(err) == "NOT_FOUND" {
				status = 404
			}
			if discovery.CodeOf(err) == "FORBIDDEN" {
				status = 403
			}
			failure(c, status)
			return
		}
		c.Next()
	})
	routes.GET("", func(c *gin.Context) {
		v, err := store.Load(c.Request.Context(), actor(c).ID, c.Param("scope"))
		if err != nil {
			failure(c, 503)
			return
		}
		c.JSON(200, v)
	})
	routes.PUT("", func(c *gin.Context) {
		if c.ContentType() != "application/json" {
			failure(c, 400)
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 512<<10)
		d := json.NewDecoder(c.Request.Body)
		d.DisallowUnknownFields()
		var v View
		var extra any
		if d.Decode(&v) != nil || d.Decode(&extra) != io.EOF {
			failure(c, 400)
			return
		}
		saved, err := store.Save(c.Request.Context(), actor(c).ID, c.Param("scope"), v)
		if err != nil {
			status := 503
			if errors.Is(err, ErrConflict) {
				status = 409
			}
			if errors.Is(err, ErrInvalid) {
				status = 400
			}
			failure(c, status)
			return
		}
		c.JSON(200, saved)
	})
}
func failure(c *gin.Context, status int) {
	code := map[int]string{400: "INVALID_ARGUMENT", 403: "FORBIDDEN", 404: "NOT_FOUND", 409: "CONFLICT", 503: "STORAGE_UNAVAILABLE"}[status]
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"code": code, "message": "Topology view request could not be completed", "request_id": c.GetString("request_id")}})
}
