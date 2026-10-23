package kafkaops

import (
	"context"
	"time"
)

type ACLRule struct {
	Principal            string `json:"principal"`
	Host                 string `json:"host"`
	ResourceType         string `json:"resource_type"`
	ResourceName         string `json:"resource_name"`
	PatternType          string `json:"pattern_type"`
	Operation            string `json:"operation"`
	Permission           string `json:"permission"`
	SourceRegistrationID string `json:"source_registration_id"`
}
type ACLSnapshot struct {
	Items      []ACLRule `json:"items"`
	Synthetic  bool      `json:"synthetic"`
	State      string    `json:"state"`
	Reason     string    `json:"reason"`
	ObservedAt time.Time `json:"observed_at"`
}
type ACLReader interface {
	ReadACLs(context.Context, string, string) (ACLSnapshot, error)
}
