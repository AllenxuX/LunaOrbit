package lineage

import (
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"strconv"
	"time"
)

func Mount(group *gin.RouterGroup, s *Service) {
	group.GET("/topic-lineage", func(c *gin.Context) {
		depth, e := strconv.Atoi(c.DefaultQuery("depth", "2"))
		if e != nil {
			respondError(c, 400, "INVALID_ARGUMENT")
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 8*time.Second)
		defer cancel()
		g, e := s.Trace(ctx, Query{c.Query("cluster_id"), c.Query("topic"), depth, c.DefaultQuery("direction", "both")})
		if e != nil {
			status, code := 503, "LINEAGE_UNAVAILABLE"
			if errors.Is(e, ErrInvalid) {
				status, code = 400, "INVALID_ARGUMENT"
			}
			if errors.Is(e, ErrNotFound) {
				status, code = 404, "NOT_FOUND"
			}
			respondError(c, status, code)
			return
		}
		c.JSON(200, g)
	})
}
func respondError(c *gin.Context, status int, code string) {
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"code": code, "message": "无法完成 Topic 全链路查询", "request_id": c.GetString("request_id")}})
}
