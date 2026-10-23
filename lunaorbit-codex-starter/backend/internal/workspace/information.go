package workspace

import (
	"github.com/gin-gonic/gin"
	"lunaorbit/backend/internal/registry"
)

// MountInformation deliberately registers only safe resource queries.
func MountInformation(g *gin.RouterGroup, s *Service, actor func(*gin.Context) registry.Actor) {
	w := g.Group("/kafka/clusters/:id/workspace")
	w.Use(func(c *gin.Context) {
		if e := s.auth(c.Request.Context(), actor(c), c.Param("id"), "read", ""); e != nil {
			fail(c, e)
			return
		}
		c.Next()
	})
	w.GET("/meta", func(c *gin.Context) {
		c.JSON(200, gin.H{"synthetic": s.Gateway != nil && s.Gateway.Synthetic(), "write_enabled": false, "approval_required": false, "permissions": gin.H{"read": true, "topic_manage": false, "applications_manage": false, "acl_manage": false, "audit_read": false, "team_manage": false, "account_manage": false, "credentials_read": false}, "capabilities": gin.H{"topic_manage": false, "acl_manage": false}})
	})
	w.GET("/resource-configs", func(c *gin.Context) {
		v, e := s.ResourceConfigs(c.Request.Context(), actor(c), c.Param("id"), ResourceTarget{Type: c.Query("resource_type"), Name: c.Query("resource_name")})
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, v)
	})
	w.GET("/topic-configs", func(c *gin.Context) {
		topic := c.Query("topic")
		if !nameRE.MatchString(topic) {
			fail(c, errInvalid)
			return
		}
		v, e := s.ResourceConfigs(c.Request.Context(), actor(c), c.Param("id"), ResourceTarget{Type: "TOPIC", Name: topic})
		if e != nil {
			fail(c, e)
			return
		}
		c.JSON(200, gin.H{"topic": topic, "configs": v.Entries, "synthetic": s.Gateway.Synthetic()})
	})
}
