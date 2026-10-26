package discovery

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

func apiError(c *gin.Context, e error) {
	code := CodeOf(e)
	status := 500
	switch code {
	case "NOT_FOUND":
		status = 404
	case "FORBIDDEN":
		status = 403
	case "INVALID_ARGUMENT":
		status = 400
	case "RATE_LIMITED":
		status = 429
	case "CONFLICT", "SCOPE_REMOVED":
		status = 409
	case "REGISTRY_UNAVAILABLE", "STORAGE_UNAVAILABLE":
		status = 503
	}
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"code": code, "message": "Discovery request could not be completed", "request_id": c.GetString("request_id")}})
}
func jsonBody(c *gin.Context, target any) bool {
	if c.ContentType() != "application/json" {
		apiError(c, fail("INVALID_ARGUMENT"))
		return false
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		apiError(c, fail("INVALID_ARGUMENT"))
		return false
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		apiError(c, fail("INVALID_ARGUMENT"))
		return false
	}
	return true
}

// Mount must be called on an already-authenticated group with Origin protection.
func Mount(group *gin.RouterGroup, service *Service, actor func(*gin.Context) Actor) {
	discovery := group.Group("/discovery")
	discovery.GET("/endpoints", func(c *gin.Context) {
		c.JSON(200, gin.H{"items": service.Endpoints()})
	})
	discovery.GET("/scopes", func(c *gin.Context) {
		rows, e := service.Scopes(actor(c))
		if e != nil {
			apiError(c, e)
			return
		}
		c.JSON(200, gin.H{"items": rows})
	})
	scope := discovery.Group("/scopes/:scope")
	scope.Use(func(c *gin.Context) {
		if !safeName.MatchString(c.Param("scope")) {
			apiError(c, ErrNotFound)
			return
		}
		c.Next()
	})
	scope.POST("/runs", func(c *gin.Context) {
		var input struct {
			Scenario string `json:"scenario"`
		}
		if !jsonBody(c, &input) {
			return
		}
		job, e := service.Start(c.Request.Context(), actor(c), c.Param("scope"), input.Scenario)
		if e != nil {
			apiError(c, e)
			return
		}
		c.JSON(202, job)
	})
	scope.GET("/runs/latest", func(c *gin.Context) {
		job, e := service.Latest(c.Request.Context(), actor(c), c.Param("scope"))
		if e != nil {
			apiError(c, e)
			return
		}
		c.JSON(200, job)
	})
	scope.GET("/snapshot", func(c *gin.Context) {
		page, e := strconv.Atoi(c.DefaultQuery("page", "1"))
		size, se := strconv.Atoi(c.DefaultQuery("page_size", "20"))
		if e != nil || se != nil || page < 1 || page > 100000 || size < 1 || size > 100 {
			apiError(c, fail("INVALID_ARGUMENT"))
			return
		}
		snapshot, e := service.Snapshot(c.Request.Context(), actor(c), c.Param("scope"))
		if e != nil {
			apiError(c, e)
			return
		}
		total := len(snapshot.Links)
		start := (page - 1) * size
		if start > total {
			start = total
		}
		end := start + size
		if end > total {
			end = total
		}
		snapshot.Links = snapshot.Links[start:end]
		links, nodes := map[string]bool{}, map[string]bool{}
		for _, link := range snapshot.Links {
			links[link.ID] = true
		}
		edges := []Edge{}
		for _, edge := range snapshot.Edges {
			if links[edge.LinkID] {
				edges = append(edges, edge)
				nodes[edge.Source] = true
				nodes[edge.Target] = true
			}
		}
		snapshot.Edges = edges
		visible := []Node{}
		for _, node := range snapshot.Nodes {
			if nodes[node.ID] {
				visible = append(visible, node)
			}
		}
		snapshot.Nodes = visible
		c.JSON(200, gin.H{"snapshot": snapshot, "total": total, "page": page, "page_size": size})
	})
	scope.PUT("/bindings", func(c *gin.Context) {
		var input struct {
			Namespace      string `json:"namespace"`
			WorkloadUID    string `json:"workload_uid"`
			Alias          string `json:"alias"`
			RegistrationID string `json:"registration_id"`
		}
		if !jsonBody(c, &input) {
			return
		}
		binding, e := service.Bind(c.Request.Context(), actor(c), c.Param("scope"), ManualBinding{Namespace: input.Namespace, WorkloadUID: input.WorkloadUID, Alias: input.Alias, RegistrationID: input.RegistrationID})
		if e != nil {
			apiError(c, e)
			return
		}
		c.JSON(200, binding)
	})
}
