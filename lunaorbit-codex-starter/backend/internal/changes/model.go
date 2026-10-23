package changes

import (
	"context"
	"errors"
	"lunaorbit/backend/internal/registry"
	"time"
)

var ErrNotFound = errors.New("NOT_FOUND")
var ErrConflict = errors.New("CONFLICT")
var ErrStorage = errors.New("STORAGE_UNAVAILABLE")

const maxObjects = 1000
const maxSnapshot = 1 << 20
const maxSnapshotTotal = 64 << 20

type Target struct {
	ConfigMapName   string `json:"-"`
	DeploymentName  string `json:"-"`
	ConfigDataKey   string `json:"-"`
	ID              string `json:"id"`
	Name            string `json:"name"`
	ScopeID         string `json:"scope_id"`
	Namespace       string `json:"namespace"`
	WorkloadUID     string `json:"workload_uid"`
	ConfigMapUID    string `json:"-"`
	DeploymentUID   string `json:"-"`
	ResourceVersion string `json:"resource_version"`
	TopicRuleKey    string `json:"-"`
	Synthetic       bool   `json:"synthetic"`
}
type Evidence struct {
	Generation         int64 `json:"generation"`
	ObservedGeneration int64 `json:"observed_generation"`
	Ready              bool  `json:"ready"`
	ReplicationHealthy *bool `json:"replication_healthy"`
}
type Request struct {
	ID                      string    `json:"id"`
	OwnerID                 string    `json:"-"`
	TargetID                string    `json:"target_id"`
	TopicRule               string    `json:"topic_rule"`
	ExpectedResourceVersion string    `json:"expected_resource_version"`
	WorkloadUID             string    `json:"workload_uid"`
	CurrentResourceVersion  string    `json:"current_resource_version"`
	Revision                int64     `json:"revision"`
	Status                  string    `json:"status"`
	Applicant               string    `json:"applicant"`
	Approver                string    `json:"approver,omitempty"`
	CanApprove              bool      `json:"can_approve"`
	CanExecute              bool      `json:"can_execute"`
	Synthetic               bool      `json:"synthetic"`
	Reason                  string    `json:"reason"`
	CreatedAt               time.Time `json:"created_at"`
	Evidence                Evidence  `json:"evidence"`
	ApprovalHash            string    `json:"-"`
	ConfigMapUID            string    `json:"-"`
}
type Input struct {
	TargetID                string `json:"target_id"`
	TopicRule               string `json:"topic_rule"`
	ExpectedResourceVersion string `json:"expected_resource_version"`
	Preview                 bool   `json:"preview"`
}
type Store interface {
	Get(context.Context, string) (Request, error)
	List(context.Context) ([]Request, error)
	Save(context.Context, Request, int64, string, string, []byte) error
	Backup(context.Context, string) ([]byte, error)
}
type SourceAuthorizer func(context.Context, registry.Actor, Target) error
type StaticApprovals map[string][]string

func (s StaticApprovals) Allows(a registry.Actor, target string) bool {
	if a.ID == "" || a.Role != "admin" {
		return false
	}
	for _, id := range s[a.ID] {
		if id == target {
			return true
		}
	}
	return false
}

type ConfigDocument struct {
	UID, ResourceVersion string
	Raw                  []byte
}
type Deployment struct {
	UID, ResourceVersion string
	Generation           int64
	RestartedAt          time.Time
}

// Adapter never exposes arbitrary URLs, arbitrary keys or untyped config mutations.
// Real lab writes require explicit per-service and per-adapter opt-in.
type Adapter interface {
	Synthetic() bool
	ReadConfig(context.Context, Target) (ConfigDocument, error)
	UpdateConfig(context.Context, Target, ConfigDocument, string) (ConfigDocument, error)
	ReadDeployment(context.Context, Target) (Deployment, error)
	Restart(context.Context, Target, Deployment, time.Time) (Deployment, error)
	Verify(context.Context, Target, Deployment) (Evidence, error)
}
