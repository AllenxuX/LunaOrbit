package httpapi

import (
	"crypto/rand"
	"encoding/hex"
	"github.com/gin-gonic/gin"
	"log/slog"
	"net/http"
	"time"
)

func failure(c *gin.Context, status int, code, message string) {
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"code": code, "message": message, "request_id": c.GetString("request_id")}})
}

func NewRouter(logger *slog.Logger) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.HandleMethodNotAllowed = true
	r.Use(func(c *gin.Context) {
		start := time.Now()
		id := make([]byte, 16)
		if _, err := rand.Read(id); err != nil {
			failure(c, 500, "INTERNAL_ERROR", "Internal server error")
			return
		}
		requestID := hex.EncodeToString(id)
		c.Set("request_id", requestID)
		c.Header("X-Request-ID", requestID)
		defer func() {
			if recover() != nil {
				failure(c, 500, "INTERNAL_ERROR", "Internal server error")
			}
			// Log the route template only: URLs, queries, bodies and panic values may contain secrets.
			logger.Info("http_request", "request_id", requestID, "method", c.Request.Method, "route", c.FullPath(), "status", c.Writer.Status(), "duration_ms", time.Since(start).Milliseconds())
		}()
		c.Next()
	})
	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "mode": "foundation", "external_connections": "disabled"})
	})
	r.NoRoute(func(c *gin.Context) { failure(c, 404, "NOT_FOUND", "Resource not found") })
	r.NoMethod(func(c *gin.Context) { failure(c, 405, "METHOD_NOT_ALLOWED", "Method not allowed") })
	return r
}
