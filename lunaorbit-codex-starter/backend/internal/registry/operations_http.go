package registry

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"strconv"
	"strings"
)

func mountOperations(item *gin.RouterGroup, s *Service, actor func(*gin.Context) Actor) {
	for _, route := range []struct {
		path, kind string
		detail     bool
	}{{"/brokers", "brokers", false}, {"/topics", "topics", false}, {"/groups", "groups", false}, {"/topics/:topic", "topics", true}, {"/groups/:group/offsets", "offsets", false}} {
		r := route
		item.GET(r.path, func(c *gin.Context) {
			page, e := strconv.Atoi(c.DefaultQuery("page", "1"))
			size, se := strconv.Atoi(c.DefaultQuery("page_size", "20"))
			query := c.Query("query")
			if e != nil || se != nil || page < 1 || page > 100000 || size < 1 || size > 100 || len(query) > 128 {
				apiFailure(c, 400, "INVALID_ARGUMENT")
				return
			}
			resource := c.Param("topic")
			if r.kind == "offsets" {
				resource = c.Param("group")
			}
			snap, err := s.ReadOperations(c.Request.Context(), actor(c), c.Param("id"), r.kind, resource, c.Query("scenario"))
			if err != nil {
				code := ErrorCode(err)
				switch code {
				case "AUTHORIZATION_DENIED", "NETWORK_DENIED":
					apiFailure(c, 403, code)
				case "AUTHENTICATION_FAILED":
					apiFailure(c, 502, code)
				case "BROKER_UNREACHABLE", "TLS_HANDSHAKE_FAILED", "ADVERTISED_UNREACHABLE", "BROKER_LIMIT", "CLUSTER_MISMATCH":
					apiFailure(c, 503, code)
				case "UNSUPPORTED_API":
					apiFailure(c, 501, code)
				case "TIMEOUT":
					apiFailure(c, 504, code)
				default:
					apiError(c, err)
				}
				return
			}
			response := gin.H{"synthetic": snap.Synthetic, "observed_at": snap.ObservedAt, "stale": snap.Stale, "state": snap.State, "reason": snap.Reason}
			if r.detail {
				for _, t := range snap.Topics {
					if t.Name == resource {
						response["item"] = t
						c.JSON(200, response)
						return
					}
				}
				apiFailure(c, 404, "NOT_FOUND")
				return
			}
			items := []any{}
			match := func(v string) bool { return strings.Contains(strings.ToLower(v), strings.ToLower(query)) }
			switch r.kind {
			case "brokers":
				for _, v := range snap.Brokers {
					if match(fmt.Sprintf("%d %s", v.ID, v.Host)) {
						items = append(items, v)
					}
				}
			case "topics":
				for _, v := range snap.Topics {
					if match(v.Name) {
						items = append(items, v)
					}
				}
			case "groups":
				for _, v := range snap.Groups {
					if match(v.ID) {
						items = append(items, v)
					}
				}
			case "offsets":
				for _, v := range snap.Offsets {
					if match(v.Topic) {
						items = append(items, v)
					}
				}
			}
			total := len(items)
			start := (page - 1) * size
			if start > total {
				start = total
			}
			end := start + size
			if end > total {
				end = total
			}
			response["items"] = items[start:end]
			response["total"] = total
			response["page"] = page
			response["page_size"] = size
			response["capabilities"] = snap.Capabilities
			c.JSON(200, response)
		})
	}
}
