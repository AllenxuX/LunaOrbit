package observability

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"io"
	"net/http"
)

func apiError(c *gin.Context, e error) {
	code := CodeOf(e)
	status := 503
	switch code {
	case "FORBIDDEN":
		status = 403
	case "NOT_FOUND":
		status = 404
	case "INVALID_ARGUMENT":
		status = 400
	case "LINK_INACTIVE":
		status = 409
	case "CAPACITY_EXCEEDED":
		status = 429
	}
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"code": code, "message": "Observability request could not be completed", "request_id": c.GetString("request_id")}})
}

// Mount requires an already authenticated group with Origin protection.
func Mount(group *gin.RouterGroup, service *Service, actor func(*gin.Context) Actor) {
	scope := group.Group("/observability/scopes/:scope")
	scope.GET("", func(c *gin.Context) {
		rows, e := service.Read(c.Request.Context(), actor(c), c.Param("scope"), c.Query("scenario"))
		if e != nil {
			apiError(c, e)
			return
		}
		c.JSON(200, rows)
	})
	scope.GET("/impact", func(c *gin.Context) {
		rows, e := service.Impact(c.Request.Context(), actor(c), c.Param("scope"), c.Query("node_id"))
		if e != nil {
			apiError(c, e)
			return
		}
		c.JSON(200, rows)
	})
	scope.GET("/lineage-confirmations", func(c *gin.Context) {
		rows, e := service.Confirmations(c.Request.Context(), actor(c), c.Param("scope"))
		if e != nil {
			apiError(c, e)
			return
		}
		c.JSON(200, gin.H{"items": rows})
	})
	scope.POST("/lineage-confirmations", func(c *gin.Context) {
		if c.ContentType() != "application/json" {
			apiError(c, fail("INVALID_ARGUMENT"))
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
		decoder := json.NewDecoder(c.Request.Body)
		decoder.DisallowUnknownFields()
		var in ConfirmationInput
		if decoder.Decode(&in) != nil {
			apiError(c, fail("INVALID_ARGUMENT"))
			return
		}
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			apiError(c, fail("INVALID_ARGUMENT"))
			return
		}
		row, e := service.Confirm(c.Request.Context(), actor(c), c.Param("scope"), in)
		if e != nil {
			apiError(c, e)
			return
		}
		c.JSON(201, row)
	})
}
