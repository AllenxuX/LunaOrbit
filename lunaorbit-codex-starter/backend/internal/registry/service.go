package registry

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"lunaorbit/backend/internal/kafkaops"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// GatewayFactory binds an authorized immutable registration to an ID-only gateway.
type GatewayFactory func(Registration, string) KafkaAdminGateway
type Service struct {
	IdentityProvider  IdentityProvider
	store             Store
	secrets           SecretStore
	mu                sync.Mutex
	jobs              map[string]Job
	slots             chan struct{}
	ctx               context.Context
	cancel            context.CancelFunc
	workers           sync.WaitGroup
	OperationsFactory OperationsFactory
	operationStarts   map[string][]time.Time
	GatewayFactory    GatewayFactory
	BrowserOrigins    []string
	probeStarts       map[string][]time.Time
	readOnlyNative    bool
	labMu             sync.Mutex
	labCatalog        []CreateInput
	labOwners         map[string][]string
	labCatalogIDs     map[string][]string
}

func NewService(store Store, secrets SecretStore) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	return &Service{OperationsFactory: func(_ Registration, scenario string) kafkaops.Reader { return kafkaops.MockReader{Scenario: scenario} }, store: store, secrets: secrets, jobs: make(map[string]Job), probeStarts: make(map[string][]time.Time), slots: make(chan struct{}, 2), ctx: ctx, cancel: cancel, GatewayFactory: func(row Registration, scenario string) KafkaAdminGateway {
		return mockGateway{row: row, scenario: scenario}
	}}
}
func (s *Service) Close() { s.cancel(); s.workers.Wait() }
func serviceStoreError(err error) error {
	if _, ok := err.(*Error); ok {
		return err
	}
	switch {
	case errors.Is(err, ErrNotFound):
		return failure("NOT_FOUND")
	case errors.Is(err, ErrConflict):
		return failure("CONFLICT")
	default:
		return failure("STORAGE_UNAVAILABLE")
	}
}
func (s *Service) authorize(ctx context.Context, actor Actor, write, probe bool, id string) error {
	allowed := actor.ID != "" && (actor.Role == "information" || actor.Role == "admin" || (!write && actor.Role == "operator") || (!write && !probe && (actor.Role == "viewer" || actor.Role == "security-admin")))
	if !allowed {
		if actor.ID != "" {
			_ = s.store.RecordAudit(ctx, actor.ID, "kafka.denied", id, "denied")
		}
		return failure("FORBIDDEN")
	}
	return nil
}
func (s *Service) dto(ctx context.Context, row Registration) Cluster {
	has := func(ref string) bool {
		if ref == "" {
			return false
		}
		value, err := s.secrets.Get(ctx, ref)
		return err == nil && len(value) > 0
	}
	configured := true
	if strings.HasPrefix(row.SecurityProtocol, "SASL_") {
		configured = has(row.UsernameRef) && has(row.PasswordRef)
	}
	caps := row.Capabilities
	if caps == nil {
		caps = []Capability{}
	}
	brokers := row.Brokers
	if brokers == nil {
		brokers = []Broker{}
	}
	return Cluster{ID: row.ID, Revision: row.Revision, DisplayName: row.DisplayName, Room: row.Room, Environment: row.Environment, Description: row.Description, BootstrapServers: row.BootstrapServers, Enabled: row.Enabled, SecurityProtocol: row.SecurityProtocol, SASLMechanism: row.SASLMechanism, UsernameMasked: row.UsernameMasked, CredentialsConfigured: configured, TLSCAConfigured: has(row.TLSCARef), TLSClientConfigured: has(row.TLSCertRef) && has(row.TLSKeyRef), TLSServerName: row.TLSServerName, CredentialRevision: row.CredentialRevision, Status: row.Status, LastProbeAt: row.LastProbeAt, LastProbeErrorCode: row.LastProbeErrorCode, DiscoveredClusterID: row.DiscoveredClusterID, VersionReported: row.VersionReported, VersionSource: row.VersionSource, Capabilities: caps, Brokers: brokers, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt}
}
func (s *Service) Get(ctx context.Context, actor Actor, id string) (Cluster, error) {
	if err := s.authorize(ctx, actor, false, false, id); err != nil {
		return Cluster{}, err
	}
	row, _, err := s.resource(ctx, actor, id)
	if err != nil {
		return Cluster{}, serviceStoreError(err)
	}
	return s.dto(ctx, row), nil
}
func (s *Service) List(ctx context.Context, actor Actor) ([]Cluster, error) {
	if err := s.authorize(ctx, actor, false, false, "registry"); err != nil {
		return nil, err
	}
	rows, err := s.store.List(ctx, actor.ID)
	if err != nil {
		return nil, serviceStoreError(err)
	}
	if gs, ok := s.store.(grantStore); ok && actor.Role != "information" {
		grants, e := gs.AllGrants(ctx)
		if e != nil {
			return nil, serviceStoreError(e)
		}
		seen := map[string]bool{}
		for _, r := range rows {
			seen[r.ID] = true
		}
		for _, g := range grants {
			if !seen[g.ClusterID] && matchesGrant(actor, g) {
				r, _, e := s.resource(ctx, actor, g.ClusterID)
				if e == nil {
					rows = append(rows, r)
					seen[r.ID] = true
				}
			}
		}
	}
	result := make([]Cluster, 0, len(rows))
	for _, row := range rows {
		result = append(result, s.dto(ctx, row))
	}
	return result, nil
}

var dnsHost = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9.-]*[a-zA-Z0-9])?$`)

func validateRegistration(row Registration) error {
	if strings.TrimSpace(row.DisplayName) == "" || len(row.DisplayName) > 128 || len(row.Room) > 128 || len(row.Environment) > 128 || len(row.Description) > 2048 || len(row.VersionReported) > 64 || len(row.BootstrapServers) == 0 || len(row.BootstrapServers) > 8 {
		return failure("INVALID_ARGUMENT")
	}
	seen := map[string]bool{}
	for i, seed := range row.BootstrapServers {
		host, port, err := net.SplitHostPort(seed)
		number, e := strconv.Atoi(port)
		if err != nil || e != nil || number < 1 || number > 65535 || len(host) > 253 || host == "" || (net.ParseIP(host) == nil && (!dnsHost.MatchString(host) || strings.Contains(host, ".."))) {
			return failure("INVALID_ARGUMENT")
		}
		normalized := net.JoinHostPort(strings.ToLower(host), strconv.Itoa(number))
		if seen[normalized] {
			return failure("INVALID_ARGUMENT")
		}
		seen[normalized] = true
		row.BootstrapServers[i] = normalized
	}
	return nil
}
func resetProbe(row *Registration) {
	row.Status = "unverified"
	if !row.Enabled {
		row.Status = "disabled"
	}
	row.LastProbeAt = nil
	row.LastProbeErrorCode = ""
	row.DiscoveredClusterID = ""
	row.Capabilities = nil
	row.Brokers = nil
}
func secretRefs(row Registration) []string {
	return []string{row.UsernameRef, row.PasswordRef, row.TLSCARef, row.TLSCertRef, row.TLSKeyRef}
}
func (s *Service) cleanup(ctx context.Context, old, new Registration) {
	current := map[string]bool{}
	for _, ref := range secretRefs(new) {
		current[ref] = true
	}
	for _, ref := range secretRefs(old) {
		if ref != "" && !current[ref] {
			_ = s.secrets.Delete(ctx, ref)
		}
	}
}
func (s *Service) credentials(ctx context.Context, row *Registration, input CredentialInput) (err error) {
	old := *row
	var created []string
	defer func() {
		keep := map[string]bool{}
		retained := *row
		if err != nil {
			retained = old
		}
		for _, ref := range secretRefs(retained) {
			keep[ref] = true
		}
		for _, ref := range created {
			if !keep[ref] {
				_ = s.secrets.Delete(context.Background(), ref)
			}
		}
		if err != nil {
			s.cleanup(context.Background(), *row, old)
		}
	}()
	protocol := input.SecurityProtocol
	sasl := strings.HasPrefix(protocol, "SASL_")
	secure := protocol == "SSL" || protocol == "SASL_SSL"
	if protocol != "PLAINTEXT" && protocol != "SSL" && protocol != "SASL_PLAINTEXT" && protocol != "SASL_SSL" {
		return failure("INVALID_CREDENTIALS")
	}
	if sasl && (input.SASLMechanism != "PLAIN" && input.SASLMechanism != "SCRAM-SHA-256" && input.SASLMechanism != "SCRAM-SHA-512") {
		return failure("INVALID_CREDENTIALS")
	}
	if !sasl && input.SASLMechanism != "" {
		return failure("INVALID_CREDENTIALS")
	}
	for _, value := range []*string{input.Username, input.Password} {
		if !sasl && value != nil && *value != "" {
			return failure("INVALID_CREDENTIALS")
		}
	}
	for _, value := range []*string{input.TLSCAPEM, input.TLSCertPEM, input.TLSKeyPEM} {
		if !secure && value != nil && *value != "" {
			return failure("INVALID_CREDENTIALS")
		}
	}
	put := func(value *string, ref *string) error {
		if value == nil {
			return nil
		}
		if len(*value) > 65536 {
			return failure("INVALID_CREDENTIALS")
		}
		if *value == "" {
			*ref = ""
			return nil
		}
		id, e := s.secrets.Put(ctx, []byte(*value))
		if e != nil {
			return failure("SECRET_UNAVAILABLE")
		}
		*ref = id
		created = append(created, id)
		return nil
	}
	for _, field := range []struct {
		value *string
		ref   *string
	}{{input.Username, &row.UsernameRef}, {input.Password, &row.PasswordRef}, {input.TLSCAPEM, &row.TLSCARef}, {input.TLSCertPEM, &row.TLSCertRef}, {input.TLSKeyPEM, &row.TLSKeyRef}} {
		if e := put(field.value, field.ref); e != nil {
			return e
		}
	}
	read := func(ref string) ([]byte, error) {
		if ref == "" {
			return nil, nil
		}
		value, e := s.secrets.Get(ctx, ref)
		if e != nil {
			return nil, failure("SECRET_UNAVAILABLE")
		}
		return value, nil
	}
	if sasl {
		u, e := read(row.UsernameRef)
		if e != nil {
			return e
		}
		p, e := read(row.PasswordRef)
		if e != nil {
			return e
		}
		if len(u) > 256 || len(p) > 4096 {
			return failure("INVALID_CREDENTIALS")
		}
		row.UsernameMasked = ""
		if len(u) > 0 {
			row.UsernameMasked = "***"
		}
	} else {
		row.UsernameRef = ""
		row.PasswordRef = ""
		row.UsernameMasked = ""
	}
	if secure {
		if len(input.TLSServerName) > 253 || (input.TLSServerName != "" && net.ParseIP(input.TLSServerName) == nil && !dnsHost.MatchString(input.TLSServerName)) {
			return failure("INVALID_CREDENTIALS")
		}
		ca, e := read(row.TLSCARef)
		if e != nil {
			return e
		}
		if len(ca) > 0 && !x509.NewCertPool().AppendCertsFromPEM(ca) {
			return failure("INVALID_CREDENTIALS")
		}
		cert, e := read(row.TLSCertRef)
		if e != nil {
			return e
		}
		key, e := read(row.TLSKeyRef)
		if e != nil {
			return e
		}
		if (len(cert) == 0) != (len(key) == 0) {
			return failure("INVALID_CREDENTIALS")
		}
		if len(cert) > 0 {
			if _, e := tls.X509KeyPair(cert, key); e != nil {
				return failure("INVALID_CREDENTIALS")
			}
		}
		row.TLSServerName = input.TLSServerName
	} else {
		row.TLSCARef = ""
		row.TLSCertRef = ""
		row.TLSKeyRef = ""
		row.TLSServerName = ""
	}
	row.SecurityProtocol = protocol
	row.SASLMechanism = input.SASLMechanism
	row.CredentialRevision++
	return nil
}

func (s *Service) Create(ctx context.Context, actor Actor, input CreateInput) (Cluster, error) {
	if err := s.authorize(ctx, actor, true, false, "registry"); err != nil {
		return Cluster{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, e := s.store.List(ctx, actor.ID)
	if e != nil {
		return Cluster{}, serviceStoreError(e)
	}
	if len(rows) >= 100 {
		return Cluster{}, failure("CAPACITY_EXCEEDED")
	}
	now := time.Now().UTC()
	row := Registration{ID: randomID(), Revision: 1, DisplayName: input.DisplayName, Room: input.Room, Environment: input.Environment, Description: input.Description, BootstrapServers: append([]string(nil), input.BootstrapServers...), Enabled: input.Enabled, CreatedBy: actor.ID, CreatedAt: now, UpdatedAt: now, VersionReported: input.VersionReported, VersionSource: "unknown"}
	if row.ID == "" {
		return Cluster{}, failure("INTERNAL_ERROR")
	}
	if input.VersionReported != "" {
		row.VersionSource = "manual"
	}
	if e := validateRegistration(row); e != nil {
		return Cluster{}, e
	}
	resetProbe(&row)
	if e := s.credentials(ctx, &row, input.CredentialInput); e != nil {
		return Cluster{}, e
	}
	if e := s.store.Save(ctx, row, 0, "kafka.register"); e != nil {
		s.cleanup(context.Background(), row, Registration{})
		return Cluster{}, serviceStoreError(e)
	}
	return s.dto(ctx, row), nil
}
func (s *Service) mutable(ctx context.Context, actor Actor, id string, revision int64) (Registration, error) {
	row, e := s.store.Get(ctx, actor.ID, id)
	if e != nil {
		return Registration{}, serviceStoreError(e)
	}
	if revision != row.Revision {
		return Registration{}, failure("CONFLICT")
	}
	if job, ok := s.jobs[id]; ok && job.Status != "done" {
		return Registration{}, failure("PROBE_IN_PROGRESS")
	}
	return row, nil
}
func (s *Service) Patch(ctx context.Context, actor Actor, id string, input PatchInput) (Cluster, error) {
	if e := s.authorize(ctx, actor, true, false, id); e != nil {
		return Cluster{}, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	row, e := s.mutable(ctx, actor, id, input.Revision)
	if e != nil {
		return Cluster{}, e
	}
	for _, field := range []struct {
		value  *string
		target *string
	}{{input.DisplayName, &row.DisplayName}, {input.Room, &row.Room}, {input.Environment, &row.Environment}, {input.Description, &row.Description}, {input.VersionReported, &row.VersionReported}} {
		if field.value != nil {
			*field.target = *field.value
		}
	}
	if input.BootstrapServers != nil {
		row.BootstrapServers = append([]string(nil), (*input.BootstrapServers)...)
	}
	if input.Enabled != nil {
		row.Enabled = *input.Enabled
	}
	if e = validateRegistration(row); e != nil {
		return Cluster{}, e
	}
	row.VersionSource = "unknown"
	if row.VersionReported != "" {
		row.VersionSource = "manual"
	}
	resetProbe(&row)
	row.Revision++
	row.UpdatedAt = time.Now().UTC()
	if e = s.store.Save(ctx, row, input.Revision, "kafka.update"); e != nil {
		return Cluster{}, serviceStoreError(e)
	}
	return s.dto(ctx, row), nil
}
func (s *Service) ReplaceCredentials(ctx context.Context, actor Actor, id string, input CredentialUpdate) (Cluster, error) {
	if e := s.authorize(ctx, actor, true, false, id); e != nil {
		return Cluster{}, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	row, e := s.mutable(ctx, actor, id, input.Revision)
	if e != nil {
		return Cluster{}, e
	}
	old := row
	if e = s.credentials(ctx, &row, input.CredentialInput); e != nil {
		return Cluster{}, e
	}
	resetProbe(&row)
	row.Revision++
	row.UpdatedAt = time.Now().UTC()
	if e = s.store.Save(ctx, row, input.Revision, "kafka.credentials.replace"); e != nil {
		s.cleanup(context.Background(), row, old)
		return Cluster{}, serviceStoreError(e)
	}
	s.cleanup(context.Background(), old, row)
	return s.dto(ctx, row), nil
}
func (s *Service) Delete(ctx context.Context, actor Actor, id string, revision int64) error {
	if e := s.authorize(ctx, actor, true, false, id); e != nil {
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	row, e := s.mutable(ctx, actor, id, revision)
	if e != nil {
		return e
	}
	now := time.Now().UTC()
	row.DeletedAt = &now
	row.Enabled = false
	row.Revision++
	row.UpdatedAt = now
	if e = s.store.Save(ctx, row, revision, "kafka.delete"); e != nil {
		return serviceStoreError(e)
	}
	s.cleanup(context.Background(), row, Registration{})
	delete(s.jobs, id)
	return nil
}

func (s *Service) StartProbe(ctx context.Context, actor Actor, id, scenario string) (Job, error) {
	if e := s.authorize(ctx, actor, false, true, id); e != nil {
		return Job{}, e
	}
	if !validScenario(scenario) {
		return Job{}, failure("INVALID_ARGUMENT")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	row, e := s.store.Get(ctx, actor.ID, id)
	if e != nil {
		return Job{}, serviceStoreError(e)
	}
	if !row.Enabled {
		return Job{}, failure("DISABLED")
	}
	for _, ref := range secretRefs(row) {
		if ref != "" {
			if _, e := s.secrets.Get(ctx, ref); e != nil {
				return Job{}, failure("SECRET_UNAVAILABLE")
			}
		}
	}
	if !s.dto(ctx, row).CredentialsConfigured {
		return Job{}, failure("SECRET_UNAVAILABLE")
	}
	if job, ok := s.jobs[id]; ok && (job.Status != "done" || time.Since(job.started) < 2*time.Second) {
		return Job{}, failure("RATE_LIMITED")
	}
	recent := s.probeStarts[actor.ID][:0]
	for _, started := range s.probeStarts[actor.ID] {
		if time.Since(started) < 10*time.Second {
			recent = append(recent, started)
		}
	}
	s.probeStarts[actor.ID] = recent
	if len(recent) >= 5 {
		return Job{}, failure("RATE_LIMITED")
	}
	if len(s.jobs) >= 256 {
		for key, job := range s.jobs {
			if job.Status == "done" && time.Since(job.started) > 5*time.Minute {
				delete(s.jobs, key)
			}
		}
		if len(s.jobs) >= 256 {
			return Job{}, failure("RATE_LIMITED")
		}
	}
	select {
	case s.slots <- struct{}{}:
	default:
		return Job{}, failure("RATE_LIMITED")
	}
	row.Status = "probing"
	oldRevision := row.Revision
	row.Revision++
	row.UpdatedAt = time.Now().UTC()
	if e = s.store.Save(ctx, row, oldRevision, "kafka.probe.start"); e != nil {
		<-s.slots
		return Job{}, serviceStoreError(e)
	}
	jobID := randomID()
	if jobID == "" {
		<-s.slots
		return Job{}, failure("INTERNAL_ERROR")
	}
	job := Job{ID: jobID, Status: "pending", started: time.Now()}
	s.jobs[id] = job
	s.probeStarts[actor.ID] = append(recent, job.started)
	gateway := s.GatewayFactory(row, scenario)
	s.workers.Add(1)
	go s.probe(actor, row, gateway)
	return Job{ID: jobID, Status: "pending"}, nil
}
func (s *Service) probe(actor Actor, row Registration, gateway KafkaAdminGateway) {
	defer s.workers.Done()
	defer func() { <-s.slots }()
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	s.mu.Lock()
	job := s.jobs[row.ID]
	job.Status = "running"
	s.jobs[row.ID] = job
	s.mu.Unlock()
	result, e := gateway.Probe(ctx, row.ID)
	code := ErrorCode(e)
	if ctx.Err() != nil {
		code = "Timeout"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job = s.jobs[row.ID]
	job.Status = "done"
	job.ErrorCode = code
	persistCtx, persistCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer persistCancel()
	current, err := s.store.Get(persistCtx, actor.ID, row.ID)
	if err == nil && current.Revision == row.Revision {
		now := time.Now().UTC()
		current.LastProbeAt = &now
		current.LastProbeErrorCode = code
		current.Status = "failed"
		current.Capabilities = nil
		current.Brokers = nil
		current.DiscoveredClusterID = ""
		if code == "" {
			current.Status = "mock_connected"
			if s.readOnlyNative {
				current.Status = "connected"
			}
			current.DiscoveredClusterID = result.ClusterID
			current.Capabilities = result.Capabilities
			current.Brokers = result.Brokers
		}
		current.Revision++
		current.UpdatedAt = now
		// Persist final failures even if the probe exhausted its own context budget.
		if err = s.store.Save(persistCtx, current, row.Revision, "kafka.probe.finish"); err != nil {
			job.ErrorCode = "STORAGE_UNAVAILABLE"
		}
	} else {
		job.ErrorCode = "CONFLICT"
	}
	s.jobs[row.ID] = job
}
func (s *Service) ProbeStatus(ctx context.Context, actor Actor, id string) (Job, error) {
	if e := s.authorize(ctx, actor, false, false, id); e != nil {
		return Job{}, e
	}
	if _, e := s.store.Get(ctx, actor.ID, id); e != nil {
		return Job{}, serviceStoreError(e)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return Job{}, failure("NOT_FOUND")
	}
	return Job{ID: job.ID, Status: job.Status, ErrorCode: job.ErrorCode}, nil
}

func validScenario(value string) bool {
	switch value {
	case "", "success", "authentication_failed", "tls_failed", "broker_unreachable", "advertised_unreachable", "authorization_denied", "unsupported_api", "timeout", "kafka37", "kafka392":
		return true
	}
	return false
}

type mockGateway struct {
	row      Registration
	scenario string
}

func (g mockGateway) Probe(ctx context.Context, id string) (ProbeResult, error) {
	if id != g.row.ID {
		return ProbeResult{}, failure("NOT_FOUND")
	}
	select {
	case <-ctx.Done():
		return ProbeResult{}, failure("Timeout")
	default:
	}
	codes := map[string]string{"authentication_failed": "AuthenticationFailed", "tls_failed": "TLSHandshakeFailed", "broker_unreachable": "BrokerUnreachable", "advertised_unreachable": "AdvertisedBrokerUnreachable", "authorization_denied": "AuthorizationDenied", "unsupported_api": "UnsupportedAPI", "timeout": "Timeout"}
	if code := codes[g.scenario]; code != "" {
		return ProbeResult{}, failure(code)
	}
	caps := []Capability{{Feature: "METADATA", Supported: true, Reason: "synthetic mock response"}, {Feature: "ACL_READ", Supported: false, Reason: "not probed"}}
	if g.scenario == "kafka37" {
		caps = append(caps, Capability{Feature: "DESCRIBE_QUORUM", Supported: false, Reason: "synthetic unsupported response fixture"})
	}
	if g.scenario == "kafka392" {
		caps = append(caps, Capability{Feature: "DESCRIBE_QUORUM", Supported: true, Reason: "synthetic supported response fixture"})
	}
	return ProbeResult{ClusterID: "synthetic-" + id, Capabilities: caps, Brokers: []Broker{{ID: 1, Host: "broker.synthetic.test", Port: 9092}}}, nil
}
