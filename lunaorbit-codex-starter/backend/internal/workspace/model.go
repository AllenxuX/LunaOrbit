package workspace

import (
	"context"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"time"
)

type Application struct {
	RequestID   string `json:"request_id,omitempty"`
	ID          string `json:"id"`
	Name        string `json:"name"`
	Principal   string `json:"principal"`
	Owner       string `json:"owner"`
	Description string `json:"description"`
	Revision    int64  `json:"revision"`
}
type Resource struct {
	Name        string `json:"name"`
	PatternType string `json:"pattern_type"`
}
type Input struct {
	AccountID         string             `json:"account_id,omitempty"`
	DesiredRules      []kafkaops.ACLRule `json:"desired_rules,omitempty"`
	Principal         string             `json:"principal,omitempty"`
	Kind              string             `json:"kind"`
	ApplicationID     string             `json:"application_id"`
	Mode              string             `json:"mode"`
	Topics            []Resource         `json:"topics"`
	Group             *Resource          `json:"group,omitempty"`
	SelectedRules     []kafkaops.ACLRule `json:"selected_rules,omitempty"`
	Topic             string             `json:"topic"`
	Partitions        int                `json:"partitions"`
	ReplicationFactor int                `json:"replication_factor"`
	RetentionMS       *int64             `json:"retention_ms,omitempty"`
	Fingerprint       string             `json:"fingerprint"`
}
type Plan struct {
	Retained            []kafkaops.ACLRule `json:"retained"`
	Input               Input              `json:"input"`
	Fingerprint         string             `json:"fingerprint"`
	ApplicationRevision int64              `json:"application_revision"`
	Additions           []kafkaops.ACLRule `json:"additions"`
	Existing            []kafkaops.ACLRule `json:"existing"`
	Removals            []kafkaops.ACLRule `json:"removals"`
	Before              map[string]string  `json:"before"`
	After               map[string]string  `json:"after"`
}
type Result struct {
	Rule  *kafkaops.ACLRule `json:"rule,omitempty"`
	State string            `json:"state"`
	Code  string            `json:"code,omitempty"`
}
type Request struct {
	ID         string    `json:"id"`
	Revision   int64     `json:"revision"`
	Status     string    `json:"status"`
	Plan       Plan      `json:"plan"`
	CreatedBy  string    `json:"created_by"`
	ApprovedBy string    `json:"approved_by"`
	Results    []Result  `json:"results"`
	CreatedAt  time.Time `json:"created_at"`
}
type Audit struct {
	Actor  string    `json:"actor"`
	Action string    `json:"action"`
	Target string    `json:"target"`
	At     time.Time `json:"at"`
}
type State struct {
	ResourceOperations map[string]ResourceOperation `json:"resource_operations"`
	Accounts           map[string]Account           `json:"accounts"`
	AccountSecrets     map[string]AccountSecret     `json:"account_secrets"`
	Applications       map[string]Application       `json:"applications"`
	Requests           map[string]Request           `json:"requests"`
	Audit              []Audit                      `json:"audit"`
}
type Store interface {
	View(context.Context, string) (State, error)
	Update(context.Context, string, func(*State) error) error
}
type Gateway interface {
	Synthetic() bool
	ACLs(context.Context, registry.Actor, string) (kafkaops.ACLSnapshot, error)
	CreateACL(context.Context, registry.Actor, string, kafkaops.ACLRule) error
	DeleteACL(context.Context, registry.Actor, string, kafkaops.ACLRule) error
	TopicConfigs(context.Context, registry.Actor, string, string) (map[string]string, error)
	CreateTopic(context.Context, registry.Actor, string, string, int, int, *int64) error
	ExpandTopic(context.Context, registry.Actor, string, string, int) error
	AlterTopicRetention(context.Context, registry.Actor, string, string, int64) error
}
