package registry

import (
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// MountInformation exposes only shared information and bounded Kafka reads.
// Cookies, bearer tokens and identity providers never participate in resolution.
func MountInformation(router *gin.Engine, service *Service, workspaceID string, allowedHosts, allowedOrigins []string, extensions ...Extension) error {
	if workspaceID == "" {
		return errors.New("information workspace required")
	}
	protected := router.Group("/api/v1")
	protected.Use(func(c *gin.Context) {
		hostAllowed := loopbackHost(c.Request.Host)
		for _, h := range allowedHosts {
			if c.Request.Host == h {
				hostAllowed = true
			}
		}
		if !hostAllowed {
			apiFailure(c, 403, "ORIGIN_DENIED")
			return
		}
		if c.Request.Method != "GET" && c.Request.Method != "HEAD" {
			if c.GetHeader("Sec-Fetch-Site") == "cross-site" {
				apiFailure(c, 403, "ORIGIN_DENIED")
				return
			}
			if raw := c.GetHeader("Origin"); raw != "" {
				u, e := url.Parse(raw)
				allowed := false
				if e == nil && u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == "" && (u.Scheme == "http" || u.Scheme == "https") {
					allowed = u.Host == c.Request.Host
					for _, o := range allowedOrigins {
						if o == raw {
							allowed = true
						}
					}
				}
				if !allowed {
					apiFailure(c, 403, "ORIGIN_DENIED")
					return
				}
			}
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 8*time.Second)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})
	actor := func(*gin.Context) Actor { return Actor{ID: workspaceID, Role: "information"} }
	for _, extension := range extensions {
		extension(protected, actor)
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
	item.PUT("/connection-credentials", func(c *gin.Context) {
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
