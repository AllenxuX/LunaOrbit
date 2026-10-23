package registry

import "time"

type Actor struct {
	ID, Role string
	Groups   []string
}
type Error struct{ Code string }

func (e *Error) Error() string { return "registry: " + e.Code }
func ErrorCode(err error) string {
	if err == nil {
		return ""
	}
	if e, ok := err.(*Error); ok {
		return e.Code
	}
	return "INTERNAL_ERROR"
}
func failure(code string) error { return &Error{Code: code} }

type CredentialInput struct {
	SecurityProtocol string  `json:"security_protocol"`
	SASLMechanism    string  `json:"sasl_mechanism"`
	Username         *string `json:"username,omitempty"`
	Password         *string `json:"password,omitempty"`
	TLSCAPEM         *string `json:"tls_ca_pem,omitempty"`
	TLSCertPEM       *string `json:"tls_cert_pem,omitempty"`
	TLSKeyPEM        *string `json:"tls_key_pem,omitempty"`
	TLSServerName    string  `json:"tls_server_name"`
}
type CreateInput struct {
	DisplayName      string   `json:"display_name"`
	Room             string   `json:"room"`
	Environment      string   `json:"environment"`
	Description      string   `json:"description"`
	BootstrapServers []string `json:"bootstrap_servers"`
	Enabled          bool     `json:"enabled"`
	VersionReported  string   `json:"version_reported"`
	CredentialInput
}
type PatchInput struct {
	Revision         int64     `json:"revision"`
	DisplayName      *string   `json:"display_name,omitempty"`
	Room             *string   `json:"room,omitempty"`
	Environment      *string   `json:"environment,omitempty"`
	Description      *string   `json:"description,omitempty"`
	BootstrapServers *[]string `json:"bootstrap_servers,omitempty"`
	Enabled          *bool     `json:"enabled,omitempty"`
	VersionReported  *string   `json:"version_reported,omitempty"`
}
type CredentialUpdate struct {
	Revision int64 `json:"revision"`
	CredentialInput
}
type Cluster struct {
	ID                    string       `json:"id"`
	Revision              int64        `json:"revision"`
	DisplayName           string       `json:"display_name"`
	Room                  string       `json:"room"`
	Environment           string       `json:"environment"`
	Description           string       `json:"description"`
	BootstrapServers      []string     `json:"bootstrap_servers"`
	Enabled               bool         `json:"enabled"`
	SecurityProtocol      string       `json:"security_protocol"`
	SASLMechanism         string       `json:"sasl_mechanism"`
	UsernameMasked        string       `json:"username_masked"`
	CredentialsConfigured bool         `json:"credentials_configured"`
	TLSCAConfigured       bool         `json:"tls_ca_configured"`
	TLSClientConfigured   bool         `json:"tls_client_configured"`
	TLSServerName         string       `json:"tls_server_name"`
	CredentialRevision    int64        `json:"credential_revision"`
	Status                string       `json:"status"`
	LastProbeAt           *time.Time   `json:"last_probe_at,omitempty"`
	LastProbeErrorCode    string       `json:"last_probe_error_code,omitempty"`
	DiscoveredClusterID   string       `json:"discovered_cluster_id,omitempty"`
	VersionReported       string       `json:"version_reported"`
	VersionSource         string       `json:"version_source"`
	Capabilities          []Capability `json:"capabilities"`
	Brokers               []Broker     `json:"brokers"`
	CreatedAt             time.Time    `json:"created_at"`
	UpdatedAt             time.Time    `json:"updated_at"`
}
type Job struct {
	ID        string `json:"job_id"`
	Status    string `json:"status"`
	ErrorCode string `json:"error_code,omitempty"`
	started   time.Time
	cancel    func()
}
