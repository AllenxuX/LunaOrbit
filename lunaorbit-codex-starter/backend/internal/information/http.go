package information

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"io"
	"net/http"
	"strconv"
)

// Mount registers shared, manually maintained metadata. The caller supplies common Origin middleware.
func Mount(group *gin.RouterGroup, s Store, workspace string, clusterExists func(context.Context, string) bool) {
	r := group.Group("/information/applications")
	r.GET("", func(c *gin.Context) {
		page, e := strconv.Atoi(c.DefaultQuery("page", "1"))
		size, e2 := strconv.Atoi(c.DefaultQuery("page_size", "20"))
		if e != nil || e2 != nil {
			failure(c, ErrInvalid)
			return
		}
		v, err := s.List(c.Request.Context(), workspace, page, size, c.Query("query"))
		if err != nil {
			failure(c, err)
			return
		}
		c.JSON(200, v)
	})
	write := func(create bool) gin.HandlerFunc {
		return func(c *gin.Context) {
			if c.ContentType() != "application/json" {
				failure(c, ErrInvalid)
				return
			}
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 256<<10)
			// Separate request shape rejects generated IDs and unknown/sensitive properties.
			var body struct {
				Location    string     `json:"location"`
				Name        string     `json:"name"`
				Owner       string     `json:"owner"`
				Environment string     `json:"environment"`
				Description string     `json:"description"`
				Revision    *int64     `json:"revision"`
				Relations   []Relation `json:"relations"`
			}
			d := json.NewDecoder(c.Request.Body)
			d.DisallowUnknownFields()
			var extra any
			if d.Decode(&body) != nil || d.Decode(&extra) != io.EOF || (create && body.Revision != nil) || (!create && body.Revision == nil) {
				failure(c, ErrInvalid)
				return
			}
			a := Application{Location: body.Location, Name: body.Name, Owner: body.Owner, Environment: body.Environment, Description: body.Description, Relations: body.Relations}
			if body.Revision != nil {
				a.Revision = *body.Revision
			}
			if !valid(a) {
				failure(c, ErrInvalid)
				return
			}
			for _, rel := range a.Relations {
				if clusterExists == nil || !clusterExists(c.Request.Context(), rel.ClusterID) {
					failure(c, ErrInvalid)
					return
				}
			}
			var saved Application
			var err error
			status := 200
			if create {
				a.ID = uuid.NewString()
				saved, err = s.Create(c.Request.Context(), workspace, a)
				status = 201
			} else {
				saved, err = s.Update(c.Request.Context(), workspace, c.Param("id"), a)
			}
			if err != nil {
				failure(c, err)
				return
			}
			c.JSON(status, saved)
		}
	}
	r.POST("", write(true))
	r.PATCH("/:id", write(false))
	r.DELETE("/:id", func(c *gin.Context) {
		rev, err := strconv.ParseInt(c.Query("revision"), 10, 64)
		if err != nil {
			failure(c, ErrInvalid)
			return
		}
		if err = s.Delete(c.Request.Context(), workspace, c.Param("id"), rev); err != nil {
			failure(c, err)
			return
		}
		c.Status(204)
	})
}
func failure(c *gin.Context, err error) {
	status := 503
	code := "STORAGE_UNAVAILABLE"
	if errors.Is(err, ErrInvalid) {
		status = 400
		code = "INVALID_ARGUMENT"
	}
	if errors.Is(err, ErrConflict) {
		status = 409
		code = "CONFLICT"
	}
	if errors.Is(err, ErrNotFound) {
		status = 404
		code = "NOT_FOUND"
	}
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"code": code, "message": "Information application request could not be completed", "request_id": c.GetString("request_id")}})
}
