package workspace

import (
	"github.com/gin-gonic/gin"
	"lunaorbit/backend/internal/registry"
)

func mountAccounts(w *gin.RouterGroup, s *Service, actor func(*gin.Context) registry.Actor) {
	w.GET("/accounts", func(c *gin.Context) {
		v, e := s.ListAccounts(c.Request.Context(), actor(c), c.Param("id"))
		if e != nil {
			fail(c, e)
			return
		}
		if application := c.Query("application_id"); application != "" {
			filtered := []Account{}
			for _, account := range v {
				for _, id := range account.ApplicationIDs {
					if id == application {
						filtered = append(filtered, account)
						break
					}
				}
			}
			v = filtered
		}
		page(c, v)
	})
	w.POST("/accounts", func(c *gin.Context) {
		var in AccountInput
		if !decode(c, &in) {
			return
		}
		v, e := s.SaveAccount(c.Request.Context(), actor(c), c.Param("id"), in)
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(201, gin.H{"account": v, "operation": v.Operation})
	})
	w.POST("/accounts/:account/reveal", func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		var in struct {
			Revision int64 `json:"revision"`
		}
		if !decode(c, &in) {
			return
		}
		v, e := s.RevealAccount(c.Request.Context(), actor(c), c.Param("id"), c.Param("account"), in.Revision)
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, gin.H{"password": v})
	})
	for _, action := range []string{"approve", "execute"} {
		action := action
		w.POST("/accounts/:account/"+action, func(c *gin.Context) {
			var in struct {
				Revision int64 `json:"revision"`
			}
			if !decode(c, &in) {
				return
			}
			var v Account
			var e error
			if action == "approve" {
				v, e = s.ApproveAccount(c.Request.Context(), actor(c), c.Param("id"), c.Param("account"), in.Revision)
			} else {
				v, e = s.ExecuteAccount(c.Request.Context(), actor(c), c.Param("id"), c.Param("account"), in.Revision)
			}
			if e != nil {
				fail(c, e)
				return
			}
			c.JSON(200, gin.H{"account": v, "operation": v.Operation})
		})
	}
}
