package registry

import (
	"context"
	"errors"
	"time"
)

var ErrNotFound = errors.New("registration not found")
var ErrConflict = errors.New("registration revision conflict")
var ErrStorage = errors.New("registration storage unavailable")

type Capability struct {
	Feature   string `json:"feature"`
	Supported bool   `json:"supported"`
	Reason    string `json:"reason"`
}
type Broker struct {
	ID   int32  `json:"id"`
	Host string `json:"host"`
	Port int32  `json:"port"`
}

// Registration is server-side storage only. HTTP must explicitly construct a DTO.
type Registration struct {
	ID, DisplayName, Room, Environment, Description                                 string
	BootstrapServers                                                                []string
	Enabled                                                                         bool
	CreatedBy                                                                       string
	CreatedAt, UpdatedAt                                                            time.Time
	DeletedAt                                                                       *time.Time
	Revision                                                                        int64
	SecurityProtocol, SASLMechanism, UsernameMasked                                 string
	UsernameRef, PasswordRef, TLSCARef, TLSCertRef, TLSKeyRef, TLSServerName        string
	CredentialRevision                                                              int64
	VersionReported, VersionSource, Status, LastProbeErrorCode, DiscoveredClusterID string
	LastProbeAt                                                                     *time.Time
	Capabilities                                                                    []Capability
	Brokers                                                                         []Broker
}

// Store operations include immutable ownership filtering and atomic audit writes.
// Save expected=0 inserts; otherwise update only if revision matches. Caller sets
// new Revision=expected+1. Audit action is a fixed service constant, never request input.
type Store interface {
	Get(context.Context, string, string) (Registration, error)
	List(context.Context, string) ([]Registration, error)
	Save(context.Context, Registration, int64, string) error
	RecordAudit(context.Context, string, string, string, string) error
}

// SecretStore intentionally has no production implementation in this phase.
type SecretStore interface {
	Put(context.Context, []byte) (string, error)
	Get(context.Context, string) ([]byte, error)
	Delete(context.Context, string) error
}

type KafkaRegistry interface {
	Get(context.Context, string, string) (Registration, error)
	List(context.Context, string) ([]Registration, error)
}

type ProbeResult struct {
	ClusterID    string
	Capabilities []Capability
	Brokers      []Broker
}
type KafkaAdminGateway interface {
	Probe(context.Context, string) (ProbeResult, error)
}

// Role names are assigned exclusively by a server-side identity provider.
type Role = string

const (
	RoleAdmin         Role = "admin"
	RoleOperator      Role = "operator"
	RoleViewer        Role = "viewer"
	RoleSecurityAdmin Role = "security-admin"
)
