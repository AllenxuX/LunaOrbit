package registry

import (
	"github.com/gin-gonic/gin"
)

type ResourcePermissions struct {
	Actions               map[string]bool `json:"actions"`
	Scopes                []GrantInput    `json:"scopes"`
	CanManage             bool            `json:"can_manage"`
	CanManageTopics       bool            `json:"can_manage_topics"`
	CanManageApplications bool            `json:"can_manage_applications"`
	CanManageACL          bool            `json:"can_manage_acl"`
	CanReadAudit          bool            `json:"can_read_audit"`
	CanManageTeam         bool            `json:"can_manage_team"`
}

func MountResourceGrants(item *gin.RouterGroup, s *Service, actor func(*gin.Context) Actor) {
	item.GET("/permissions", func(c *gin.Context) {
		a := actor(c)
		id := c.Param("id")
		r, gs, e := s.resource(c.Request.Context(), a, id)
		if e != nil {
			apiError(c, e)
			return
		}
		p := ResourcePermissions{Actions: map[string]bool{}, Scopes: []GrantInput{}}
		for _, v := range []string{"read", "topic.manage", "applications.manage", "acl.manage", "audit.read", "team.manage", "account.manage", "credentials.read"} {
			p.Actions[v] = s.AuthorizeResource(c.Request.Context(), a, id, v, "") == nil
		}
		for _, g := range gs {
			p.Scopes = append(p.Scopes, g.GrantInput)
		}
		if r.CreatedBy == a.ID {
			p.Scopes = append(p.Scopes, GrantInput{SubjectType: "user", SubjectID: a.ID, Role: a.Role})
		}
		p.CanManage = p.Actions["team.manage"]
		p.CanManageTeam = p.CanManage
		p.CanManageTopics = p.Actions["topic.manage"]
		p.CanManageApplications = p.Actions["applications.manage"]
		p.CanManageACL = p.Actions["acl.manage"]
		p.CanReadAudit = p.Actions["audit.read"]
		c.JSON(200, p)
	})
	item.GET("/team-grants", func(c *gin.Context) {
		rows, e := s.ListGrants(c.Request.Context(), actor(c), c.Param("id"))
		if e != nil {
			apiError(c, e)
			return
		}
		c.JSON(200, gin.H{"items": rows})
	})
	item.POST("/team-grants", func(c *gin.Context) {
		var in GrantInput
		if !body(c, &in) {
			return
		}
		row, e := s.CreateGrant(c.Request.Context(), actor(c), c.Param("id"), in)
		if e != nil {
			apiError(c, e)
			return
		}
		c.JSON(201, row)
	})
	item.DELETE("/team-grants/:grant", func(c *gin.Context) {
		var in struct {
			Revision int64 `json:"revision"`
		}
		if !body(c, &in) {
			return
		}
		if e := s.RevokeGrant(c.Request.Context(), actor(c), c.Param("id"), c.Param("grant"), in.Revision); e != nil {
			apiError(c, e)
			return
		}
		c.Status(204)
	})
}
