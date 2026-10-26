package discovery

import (
	"context"
	"errors"
	"time"
)

var ErrNotFound = errors.New("discovery snapshot not found")
var ErrConflict = errors.New("discovery revision conflict")
var ErrStorage = errors.New("discovery storage unavailable")

type Issue struct {
	Code        string `json:"code"`
	Namespace   string `json:"namespace,omitempty"`
	WorkloadUID string `json:"workload_uid,omitempty"`
	ConfigUID   string `json:"config_uid,omitempty"`
}
type Evidence struct {
	ScopeID         string    `json:"scope_id"`
	Namespace       string    `json:"namespace"`
	DeploymentUID   string    `json:"deployment_uid"`
	DeploymentName  string    `json:"deployment_name"`
	ConfigMapName   string    `json:"configmap_name"`
	ConfigMapUID    string    `json:"configmap_uid"`
	ResourceVersion string    `json:"resource_version"`
	ConfigKey       string    `json:"config_key"`
	ReferenceKind   string    `json:"reference_kind"`
	Fingerprint     string    `json:"fingerprint"`
	ObservedAt      time.Time `json:"observed_at"`
}
type Mapping struct {
	Alias          string   `json:"alias"`
	Status         string   `json:"status"`
	Method         string   `json:"method"`
	Confidence     string   `json:"confidence"`
	RegistrationID string   `json:"registration_id,omitempty"`
	CandidateIDs   []string `json:"candidate_ids"`
	ClusterID      string   `json:"cluster_id,omitempty"`
	// BootstrapAddresses is host:port only (no scheme, user info or path), parsed from the MM2 configuration.
	BootstrapAddresses []string `json:"bootstrap_addresses"`
}
type Runtime struct {
	Generation         int64  `json:"generation"`
	ObservedGeneration int64  `json:"observed_generation"`
	Replicas           int32  `json:"replicas"`
	ReadyReplicas      int32  `json:"ready_replicas"`
	PodCandidates      int    `json:"pod_candidates"`
	PodAssociation     string `json:"pod_association"`
}
type Declaration struct {
	InternalSeparatorEnabled   bool   `json:"internal_separator_enabled"`
	TopicsExcludeRaw           string `json:"topics_exclude_raw"`
	Separator                  string `json:"separator"`
	TopicFilterClass           string `json:"topic_filter_class"`
	SemanticsKnown             bool   `json:"semantics_known"`
	TopicsRaw                  string `json:"topics_raw"`
	Policy                     string `json:"policy"`
	Enabled                    bool   `json:"enabled"`
	EnabledExplicit            bool   `json:"enabled_explicit"`
	SourceBootstrapFingerprint string `json:"source_bootstrap_fingerprint"`
	TargetBootstrapFingerprint string `json:"target_bootstrap_fingerprint"`
	ConfigMapUID               string `json:"configmap_uid"`
	ConfigKey                  string `json:"config_key"`
}
type Link struct {
	InternalSeparatorEnabled bool          `json:"internal_separator_enabled"`
	TopicsExcludeRaw         string        `json:"topics_exclude_raw"`
	Separator                string        `json:"separator"`
	TopicFilterClass         string        `json:"topic_filter_class"`
	SemanticsKnown           bool          `json:"semantics_known"`
	ID                       string        `json:"id"`
	Namespace                string        `json:"namespace"`
	WorkloadUID              string        `json:"workload_uid"`
	WorkloadName             string        `json:"workload_name"`
	SourceAlias              string        `json:"source_alias"`
	TargetAlias              string        `json:"target_alias"`
	TopicsRaw                string        `json:"topics_raw"`
	Policy                   string        `json:"policy"`
	Enabled                  bool          `json:"enabled"`
	Confidence               string        `json:"confidence"`
	Declared                 bool          `json:"declared"`
	Observed                 bool          `json:"observed"`
	Verified                 bool          `json:"verified"`
	Stale                    bool          `json:"stale"`
	State                    string        `json:"state"`
	ObservedAt               time.Time     `json:"observed_at"`
	Runtime                  Runtime       `json:"runtime"`
	Source                   Mapping       `json:"source"`
	Target                   Mapping       `json:"target"`
	Evidence                 []Evidence    `json:"evidence"`
	Notes                    []string      `json:"notes"`
	Declarations             []Declaration `json:"declarations"`
	// KubeCluster is set only for auto-enumerated scopes.
	KubeCluster string `json:"k8s_cluster,omitempty"`
}
type Node struct {
	ID                string `json:"id"`
	Kind              string `json:"kind"`
	Label             string `json:"label"`
	RegistrationID    string `json:"registration_id,omitempty"`
	ReplicationFactor *int   `json:"replication_factor"`
	RFStatus          string `json:"rf_status"`
}
type Edge struct {
	ID       string     `json:"id"`
	Source   string     `json:"source"`
	Target   string     `json:"target"`
	LinkID   string     `json:"link_id"`
	Relation string     `json:"relation"`
	Evidence []Evidence `json:"evidence"`
	Stale    bool       `json:"stale"`
}
type Snapshot struct {
	ScopeID     string    `json:"scope_id"`
	Synthetic   bool      `json:"synthetic"`
	ObservedAt  time.Time `json:"observed_at"`
	AttemptedAt time.Time `json:"attempted_at"`
	Stale       bool      `json:"stale"`
	Links       []Link    `json:"links"`
	Nodes       []Node    `json:"nodes"`
	Edges       []Edge    `json:"edges"`
	Issues      []Issue   `json:"issues"`
	// Auto scopes only: MM2 identification and scan counters.
	Workloads []WorkloadClass `json:"workloads,omitempty"`
	Scan      *ScanSummary    `json:"scan,omitempty"`
}

// WorkloadClass is a deployment that looked like MM2. "confirmed" produced at
// least one replication link; "suspected" did not and never enters the graph.
// Deployments without MM2 features are "unidentified" and are not listed.
type WorkloadClass struct {
	KubeCluster string   `json:"k8s_cluster"`
	Namespace   string   `json:"namespace"`
	Name        string   `json:"name"`
	UID         string   `json:"workload_uid"`
	Status      string   `json:"status"`
	Signals     []string `json:"signals"`
}
type ScanSummary struct {
	KubeCluster       string `json:"k8s_cluster"`
	Namespaces        int    `json:"namespaces"`
	Deployments       int    `json:"deployments"`
	NamespacesWithMM2 int    `json:"namespaces_with_mm2"`
	Confirmed         int    `json:"confirmed"`
	Suspected         int    `json:"suspected"`
}
type ManualBinding struct {
	ScopeID        string    `json:"scope_id"`
	Namespace      string    `json:"namespace"`
	WorkloadUID    string    `json:"workload_uid"`
	Alias          string    `json:"alias"`
	RegistrationID string    `json:"registration_id"`
	ApprovedBy     string    `json:"approved_by"`
	ApprovedAt     time.Time `json:"approved_at"`
}
type Store interface {
	Load(context.Context, string, string) (Snapshot, error)
	Save(context.Context, string, Snapshot) error
	ListBindings(context.Context, string, string) ([]ManualBinding, error)
	PutBinding(context.Context, string, ManualBinding) error
}
type Registration struct {
	ID                  string
	DisplayName         string
	BootstrapServers    []string
	DiscoveredClusterID string
	IdentityVerified    bool
	Enabled             bool
	// Business fields attached to merged auto endpoints.
	Room, Environment, Description string
}
type AliasIdentity struct {
	ScopeID, Namespace, WorkloadUID, Alias, ClusterID string
	Verified                                          bool
	Synthetic                                         bool
}
