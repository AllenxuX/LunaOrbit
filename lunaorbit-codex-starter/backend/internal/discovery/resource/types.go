// Package resource contains server-only Kubernetes projections. It is not a Kite envelope.
package resource

import (
	"encoding/json"
	"fmt"
	"time"
)

type Metadata struct {
	Name            string            `json:"name"`
	Namespace       string            `json:"namespace"`
	UID             string            `json:"uid"`
	ResourceVersion string            `json:"resourceVersion"`
	Generation      int64             `json:"generation"`
	Labels          map[string]string `json:"labels,omitempty"`
}
type Namespace struct {
	Metadata Metadata `json:"metadata"`
}
type KeyRef struct {
	Name string `json:"name"`
	Key  string `json:"key,omitempty"`
}
type EnvVar struct {
	ValueFrom struct {
		ConfigMapKeyRef *KeyRef `json:"configMapKeyRef,omitempty"`
	} `json:"valueFrom"`
}
type EnvFrom struct {
	ConfigMapRef *KeyRef `json:"configMapRef,omitempty"`
}
type Container struct {
	Name    string    `json:"name"`
	Image   string    `json:"image"`
	Command []string  `json:"command"`
	Args    []string  `json:"args"`
	Env     []EnvVar  `json:"env"`
	EnvFrom []EnvFrom `json:"envFrom"`
}
type Volume struct {
	Name      string  `json:"name"`
	ConfigMap *KeyRef `json:"configMap,omitempty"`
}
type Deployment struct {
	Metadata Metadata `json:"metadata"`
	Spec     struct {
		Replicas *int32 `json:"replicas"`
		Selector struct {
			MatchLabels map[string]string `json:"matchLabels"`
		} `json:"selector"`
		Template struct {
			Spec struct {
				Containers     []Container `json:"containers"`
				InitContainers []Container `json:"initContainers"`
				Volumes        []Volume    `json:"volumes"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
	Status struct {
		ObservedGeneration int64 `json:"observedGeneration"`
		Replicas           int32 `json:"replicas"`
		ReadyReplicas      int32 `json:"readyReplicas"`
	} `json:"status"`
}
type ConfigMap struct {
	Metadata Metadata          `json:"metadata"`
	Data     map[string]string `json:"data"`
}
type Pod struct {
	Metadata Metadata `json:"metadata"`
	Status   struct {
		Phase      string `json:"phase"`
		Conditions []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
	} `json:"status"`
}

func (c ConfigMap) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Metadata Metadata `json:"metadata"`
		Content  string   `json:"content"`
	}{c.Metadata, "redacted"})
}
func (c ConfigMap) Format(state fmt.State, verb rune) {
	_, _ = state.Write([]byte("resource.ConfigMap[redacted]"))
}
func (d Deployment) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Metadata Metadata `json:"metadata"`
		Content  string   `json:"content"`
	}{d.Metadata, "redacted"})
}
func (d Deployment) Format(state fmt.State, verb rune) {
	_, _ = state.Write([]byte("resource.Deployment[redacted]"))
}

type Scope struct {
	ID         string   `json:"id"`
	Cluster    string   `json:"-"`
	Namespaces []string `json:"-"`
	Synthetic  bool     `json:"synthetic"`
	// Auto marks a scope enumerated from Kite's cluster list (one scope per
	// Kubernetes cluster, every namespace except the excluded ones).
	Auto bool `json:"-"`
}
type NamespaceBatch struct {
	Namespace string
	Complete  bool
	ErrorCode string
	// Signals holds MM2 identification evidence per deployment UID (auto scopes).
	Signals     map[string][]string
	Deployments []Deployment
	ConfigMaps  []ConfigMap
	Pods        []Pod
}
type Collection struct {
	Scope      Scope
	ObservedAt time.Time
	Namespaces []NamespaceBatch
	ErrorCode  string
	// Scan counters for auto scopes: namespaces and deployments examined.
	NamespacesScanned  int
	DeploymentsScanned int
}
