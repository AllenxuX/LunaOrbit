package workspace

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"github.com/google/uuid"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"sort"
	"strings"
	"time"
)

type AccountIdentity struct {
	Username  string `json:"username"`
	Mechanism string `json:"mechanism"`
}
type AccountGateway interface {
	List(context.Context, registry.Actor, string) ([]AccountIdentity, error)
	Create(context.Context, registry.Actor, string, string, string) error
	Verify(context.Context, registry.Actor, string, string, string) error
}
type ProvisionOperation struct {
	RequestID          string             `json:"request_id"`
	RequestFingerprint string             `json:"request_fingerprint"`
	Rules              []kafkaops.ACLRule `json:"rules"`
	Results            []Result           `json:"results"`
	Fingerprint        string             `json:"fingerprint"`
	Status             string             `json:"status"`
	Revision           int64              `json:"revision"`
	CreatedBy          string             `json:"created_by"`
	ApprovedBy         string             `json:"approved_by"`
	ExecutedBy         string             `json:"executed_by"`
	Code               string             `json:"code,omitempty"`
	CreatedAt          time.Time          `json:"created_at"`
}
type Account struct {
	ID              string              `json:"id"`
	Username        string              `json:"username"`
	Principal       string              `json:"principal"`
	Mechanism       string              `json:"mechanism"`
	Source          string              `json:"source"`
	CredentialState string              `json:"credential_state"`
	Observation     string              `json:"observation"`
	Revision        int64               `json:"revision"`
	ApplicationIDs  []string            `json:"application_ids"`
	Operation       *ProvisionOperation `json:"operation,omitempty"`
}
type AccountSecret struct {
	Ciphertext string `json:"ciphertext"`
	KeyVersion int    `json:"key_version"`
}
type AccountInput struct {
	RequestID     string     `json:"request_id,omitempty"`
	AccessMode    string     `json:"access_mode,omitempty"`
	Topics        []Resource `json:"topics,omitempty"`
	Group         *Resource  `json:"group,omitempty"`
	Mode          string     `json:"mode"`
	ApplicationID string     `json:"application_id"`
	Username      string     `json:"username"`
	Password      string     `json:"password"`
}

func (s *Service) accountAuth(ctx context.Context, a registry.Actor, id, action string) error {
	if a.ID == "" {
		return errors.New("FORBIDDEN")
	}
	return s.Registry.AuthorizeUnscopedResource(ctx, a, id, action)
}
func accountAAD(id string, a Account) []byte { return []byte(id + "\x00" + a.ID + "\x00" + a.Username) }
func (s *Service) accountCrypt(id string, a Account, value string, open bool) (string, error) {
	if len(s.AccountKey) != 32 || strings.Trim(string(s.AccountKey), "\x00") == "" {
		return "", errors.New("DISABLED")
	}
	b, e := aes.NewCipher(s.AccountKey)
	if e != nil {
		return "", errors.New("DISABLED")
	}
	g, e := cipher.NewGCM(b)
	if e != nil {
		return "", e
	}
	if open {
		raw, e := base64.RawStdEncoding.DecodeString(value)
		if e != nil || len(raw) < g.NonceSize() {
			return "", errors.New("DISABLED")
		}
		out, e := g.Open(nil, raw[:g.NonceSize()], raw[g.NonceSize():], accountAAD(id, a))
		if e != nil {
			return "", errors.New("DISABLED")
		}
		return string(out), nil
	}
	nonce := make([]byte, g.NonceSize())
	if _, e = rand.Read(nonce); e != nil {
		return "", e
	}
	return base64.RawStdEncoding.EncodeToString(g.Seal(nonce, nonce, []byte(value), accountAAD(id, a))), nil
}
func (s *Service) ListAccounts(ctx context.Context, a registry.Actor, id string) ([]Account, error) {
	if e := s.accountAuth(ctx, a, id, "account.manage"); e != nil {
		return nil, e
	}
	st, e := s.Store.View(ctx, id)
	if e != nil {
		return nil, e
	}
	out := []Account{}
	seen := map[string]bool{}
	for _, v := range st.Accounts {
		out = append(out, v)
		seen[v.Username] = true
	}
	if s.AccountGateway != nil {
		identities, e := s.AccountGateway.List(ctx, a, id)
		if e != nil {
			return nil, errors.New("ACCOUNT_DISCOVERY_FAILED")
		}
		byUsername := map[string]AccountIdentity{}
		for _, v := range identities {
			_, ok := byUsername[v.Username]
			if !ok || v.Mechanism == "SCRAM-SHA-256" {
				byUsername[v.Username] = v
			}
		}
		for _, v := range byUsername {
			if !seen[v.Username] {
				out = append(out, Account{ID: "external:" + v.Username, Username: v.Username, Principal: "User:" + v.Username, Mechanism: v.Mechanism, Source: "discovered", CredentialState: "unmanaged", Observation: "observed", ApplicationIDs: []string{}})
				seen[v.Username] = true
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Username < out[j].Username })
	return out, nil
}
func (s *Service) SaveAccount(ctx context.Context, a registry.Actor, id string, in AccountInput) (Account, error) {
	var v Account
	if e := s.accountAuth(ctx, a, id, "account.manage"); e != nil {
		return v, e
	}
	if s.AccountGateway == nil {
		return v, errors.New("DISABLED")
	}
	if in.Mode != "create" && in.Mode != "import" && in.Mode != "reference" {
		return v, errInvalid
	}
	if in.Mode != "create" && (in.AccessMode != "" || len(in.Topics) > 0 || in.Group != nil) {
		return v, errInvalid
	}
	requestFingerprint := ""
	if in.Mode == "create" {
		if in.Password != "" || in.Username != "" {
			return v, errInvalid
		}
		parsed, e := uuid.Parse(in.RequestID)
		if e != nil || parsed.String() != in.RequestID {
			return v, errInvalid
		}
		requestFingerprint = fingerprint(struct {
			Mode, ApplicationID, AccessMode string
			Topics                          []Resource
			Group                           *Resource
		}{in.Mode, in.ApplicationID, in.AccessMode, in.Topics, in.Group})
		st, e := s.Store.View(ctx, id)
		if e != nil {
			return v, e
		}
		for _, old := range st.Accounts {
			if old.Operation != nil && old.Operation.RequestID == in.RequestID {
				if old.Operation.RequestFingerprint != requestFingerprint || old.Operation.CreatedBy != a.ID {
					return v, errConflict
				}
				return old, nil
			}
		}
		in.Username = "lo_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		b := make([]byte, 32)
		if _, e := rand.Read(b); e != nil {
			return v, e
		}
		in.Password = base64.RawURLEncoding.EncodeToString(b)
	}
	if !nameRE.MatchString(in.Username) || len(in.Password) > 4096 || (in.Mode == "reference" && in.Password != "") {
		return v, errInvalid
	}
	if in.Mode == "import" && in.Password == "" {
		return v, errInvalid
	}
	identities, e := s.AccountGateway.List(ctx, a, id)
	if e != nil {
		return v, errors.New("ACCOUNT_DISCOVERY_FAILED")
	}
	exists := false
	supported := false
	for _, x := range identities {
		if x.Username == in.Username {
			exists = true
			if x.Mechanism == "SCRAM-SHA-256" {
				supported = true
			}
		}
	}
	if in.Mode == "create" && exists {
		return v, errConflict
	}
	if in.Mode != "create" && !exists {
		return v, errors.New("NOT_FOUND")
	}
	if in.Mode != "create" && !supported {
		return v, errors.New("UNSUPPORTED_API")
	}
	if in.Mode == "import" {
		if e = s.AccountGateway.Verify(ctx, a, id, in.Username, in.Password); e != nil {
			return v, errors.New("AUTHENTICATION_FAILED")
		}
	}
	v = Account{ID: uuid.NewString(), Username: in.Username, Principal: "User:" + in.Username, Mechanism: "SCRAM-SHA-256", Source: in.Mode, CredentialState: "unmanaged", Observation: "observed", Revision: 1, ApplicationIDs: []string{}}
	if in.Mode == "import" {
		v.Observation = "verified"
	}
	if in.Mode == "create" {
		v.Observation = "pending"
		v.Operation = &ProvisionOperation{RequestID: in.RequestID, RequestFingerprint: requestFingerprint, Status: "pending_approval", Revision: 1, CreatedBy: a.ID, CreatedAt: time.Now().UTC()}
		v.Operation.Rules, e = s.accountRules(ctx, a, id, v.Principal, in)
		if e != nil {
			return Account{}, e
		}
		v.Operation.Fingerprint = fingerprint(v.Operation.Rules)
		v.Operation.Results = []Result{}
		if s.LabAutoApprove && s.WriteEnabled != nil && s.WriteEnabled(ctx, a, id) == nil {
			v.Operation.Status = "approved"
			v.Operation.ApprovedBy = a.ID
		}
	}
	var secret string
	if in.Password != "" {
		secret, e = s.accountCrypt(id, v, in.Password, false)
		if e != nil {
			return Account{}, e
		}
		v.CredentialState = "managed"
	}
	e = s.Store.Update(ctx, id, func(st *State) error {
		if in.Mode == "create" {
			for _, old := range st.Accounts {
				if old.Operation != nil && old.Operation.RequestID == in.RequestID {
					if old.Operation.RequestFingerprint != requestFingerprint || old.Operation.CreatedBy != a.ID {
						return errConflict
					}
					v = old
					return nil
				}
			}
		}
		if len(st.Audit) >= maxAdmissionAudit || len(st.Accounts) >= 128 {
			return errCapacity
		}
		if st.Accounts == nil {
			st.Accounts = map[string]Account{}
		}
		if st.AccountSecrets == nil {
			st.AccountSecrets = map[string]AccountSecret{}
		}
		for _, old := range st.Accounts {
			if old.Username == v.Username {
				if in.Mode == "reference" {
					v = old
					v.Revision++
					break
				}
				if in.Mode == "import" && old.CredentialState == "unmanaged" {
					v = old
					v.Revision++
					v.Source = "import"
					v.CredentialState = "managed"
					v.Observation = "verified"
					secret, e = s.accountCrypt(id, v, in.Password, false)
					if e != nil {
						return e
					}
					break
				}
				return errConflict
			}
		}
		if in.ApplicationID != "" {
			if _, ok := st.Applications[in.ApplicationID]; !ok {
				return errors.New("NOT_FOUND")
			}
			found := false
			for _, x := range v.ApplicationIDs {
				if x == in.ApplicationID {
					found = true
				}
			}
			if !found {
				v.ApplicationIDs = append(v.ApplicationIDs, in.ApplicationID)
			}
		}
		st.Accounts[v.ID] = v
		if secret != "" {
			st.AccountSecrets[v.ID] = AccountSecret{secret, 1}
		}
		audit(st, a, "account."+in.Mode, v.ID)
		return nil
	})
	return v, e
}
func (s *Service) RevealAccount(ctx context.Context, a registry.Actor, id, key string, revision int64) (string, error) {
	if e := s.accountAuth(ctx, a, id, "credentials.read"); e != nil {
		return "", e
	}
	var value string
	e := s.Store.Update(ctx, id, func(st *State) error {
		v, ok := st.Accounts[key]
		if !ok {
			return errors.New("NOT_FOUND")
		}
		if v.Revision != revision {
			return errConflict
		}
		if len(st.Audit) >= maxAdmissionAudit {
			return errCapacity
		}
		var e error
		value, e = s.accountCrypt(id, v, st.AccountSecrets[key].Ciphertext, true)
		if e != nil {
			return e
		}
		audit(st, a, "credentials.reveal", key)
		return nil
	})
	return value, e
}
func (s *Service) ApproveAccount(ctx context.Context, a registry.Actor, id, key string, revision int64) (Account, error) {
	var v Account
	if e := s.accountAuth(ctx, a, id, "account.manage"); e != nil {
		return v, e
	}
	snapshot, e := s.Store.View(ctx, id)
	if e != nil {
		return v, e
	}
	observed, ok := snapshot.Accounts[key]
	if !ok {
		return v, errors.New("NOT_FOUND")
	}
	if observed.Revision != revision || observed.Operation == nil || observed.Operation.Status != "pending_approval" {
		return v, errConflict
	}
	if observed.Operation.CreatedBy == a.ID && !s.LabAutoApprove {
		return v, errors.New("SELF_APPROVAL_DENIED")
	}
	if observed.Operation.CreatedBy == a.ID && (s.WriteEnabled == nil || s.WriteEnabled(ctx, a, id) != nil) {
		return v, errors.New("DISABLED")
	}
	if e = s.accountRulesAuth(ctx, a, id, observed.Operation.Rules); e != nil {
		return v, e
	}
	if observed.Operation.Fingerprint != fingerprint(observed.Operation.Rules) {
		return v, errConflict
	}
	e = s.Store.Update(ctx, id, func(st *State) error {
		var ok bool
		v, ok = st.Accounts[key]
		if !ok {
			return errors.New("NOT_FOUND")
		}
		if v.Revision != revision || v.Operation == nil || v.Operation.Status != "pending_approval" {
			return errConflict
		}
		if v.Operation.CreatedBy == a.ID && !s.LabAutoApprove {
			return errors.New("SELF_APPROVAL_DENIED")
		}
		if v.Operation.Fingerprint != observed.Operation.Fingerprint || v.Operation.CreatedBy != observed.Operation.CreatedBy {
			return errConflict
		}
		if len(st.Audit) >= maxAdmissionAudit {
			return errCapacity
		}
		v.Operation.Status = "approved"
		v.Operation.ApprovedBy = a.ID
		v.Operation.Revision++
		v.Revision++
		st.Accounts[key] = v
		audit(st, a, "account.approve", key)
		return nil
	})
	return v, e
}
func (s *Service) ExecuteAccount(ctx context.Context, a registry.Actor, id, accountID string, revision int64) (Account, error) {
	var v Account
	if e := s.accountAuth(ctx, a, id, "account.manage"); e != nil {
		return v, e
	}
	if s.AccountGateway == nil || s.WriteEnabled == nil {
		return v, errors.New("DISABLED")
	}
	if e := s.WriteEnabled(ctx, a, id); e != nil {
		return v, e
	}
	var password string
	snapshot, e := s.Store.View(ctx, id)
	if e != nil {
		return v, e
	}
	observed, ok := snapshot.Accounts[accountID]
	if !ok {
		return v, errors.New("NOT_FOUND")
	}
	if observed.Revision != revision || observed.Operation == nil || observed.Operation.Status != "approved" {
		return v, errConflict
	}
	if e = s.accountRulesAuth(ctx, a, id, observed.Operation.Rules); e != nil {
		return v, e
	}
	if observed.Operation.Fingerprint != fingerprint(observed.Operation.Rules) {
		return v, errConflict
	}
	e = s.Store.Update(ctx, id, func(st *State) error {
		var ok bool
		v, ok = st.Accounts[accountID]
		if !ok {
			return errors.New("NOT_FOUND")
		}
		if v.Revision != revision || v.Operation == nil || v.Operation.Status != "approved" || v.Operation.ApprovedBy == "" {
			return errConflict
		}
		if v.Operation.Fingerprint != observed.Operation.Fingerprint || v.Operation.ApprovedBy != observed.Operation.ApprovedBy {
			return errConflict
		}
		if len(st.Audit) >= maxAdmissionAudit-160 {
			return errCapacity
		}
		var e error
		password, e = s.accountCrypt(id, v, st.AccountSecrets[accountID].Ciphertext, true)
		if e != nil {
			return e
		}
		v.Operation.Status = "executing"
		v.Operation.ExecutedBy = a.ID
		v.Revision++
		v.Operation.Revision++
		st.Accounts[accountID] = v
		audit(st, a, "account.execute.start", accountID)
		return nil
	})
	if e != nil {
		return v, e
	}
	status, code := "review_required", "WRITE_UNCONFIRMED"
	identities, e := s.AccountGateway.List(ctx, a, id)
	exists := false
	for _, x := range identities {
		if x.Username == v.Username {
			exists = true
		}
	}
	if e == nil && exists {
		code = "IDENTITY_CONFLICT"
	}
	if e == nil && !exists {
		e = s.accountAuth(ctx, a, id, "account.manage")
		if e == nil {
			e = s.WriteEnabled(ctx, a, id)
		}
		if e == nil {
			e = s.AccountGateway.Create(ctx, a, id, v.Username, password)
		}
		if e == nil {
			v.Operation.Results = append(v.Operation.Results, Result{State: "scram_created"})
			if e = s.AccountGateway.Verify(ctx, a, id, v.Username, password); e == nil {
				v.Operation.Results = append(v.Operation.Results, Result{State: "authentication_verified"})
				status, code = "executed", ""
			} else {
				code = "AUTHENTICATION_FAILED"
			}
		}
	}
	if status == "executed" && len(v.Operation.Rules) > 0 {
		for _, rule := range v.Operation.Rules {
			result := Result{Rule: &rule, State: "applied"}
			e = s.accountAuth(ctx, a, id, "account.manage")
			if e == nil {
				e = authorizeACL(ctx, s.Registry, a, id, "acl.manage", rule)
			}
			if e == nil {
				e = s.WriteEnabled(ctx, a, id)
			}
			if e == nil {
				e = s.Gateway.CreateACL(ctx, a, id, rule)
			}
			if e == nil {
				var snap kafkaops.ACLSnapshot
				snap, e = s.Gateway.ACLs(ctx, a, id)
				if e == nil {
					found := false
					for _, r := range snap.Items {
						if key(r) == key(rule) {
							found = true
						}
					}
					if !found {
						e = errors.New("READBACK_FAILED")
					}
				}
			}
			if e != nil {
				result.State = "unknown"
				result.Code = "ACL_WRITE_UNCONFIRMED"
				status, code = "partial_failure", "ACL_WRITE_UNCONFIRMED"
			}
			v.Operation.Results = append(v.Operation.Results, result)
			// Persist each completed step before proceeding; crashes never replay SCRAM creation.
			safe, done := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			saveErr := s.Store.Update(safe, id, func(st *State) error {
				if st.Accounts[accountID].Revision != v.Revision {
					return errConflict
				}
				v.Revision++
				v.Operation.Revision++
				st.Accounts[accountID] = v
				return nil
			})
			done()
			if saveErr != nil {
				return v, saveErr
			}
			if e != nil {
				break
			}
		}
	}
	safe, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	e = s.Store.Update(safe, id, func(st *State) error {
		if st.Accounts[accountID].Revision != v.Revision {
			return errConflict
		}
		v.Operation.Status = status
		v.Operation.Code = code
		v.Operation.Revision++
		v.Revision++
		if status == "executed" {
			v.Observation = "verified"
		} else {
			v.Observation = "unknown"
		}
		st.Accounts[accountID] = v
		audit(st, a, "account.execute."+status, accountID)
		return nil
	})
	return v, e
}

// Legacy application principals become identity records only; migration never writes Kafka.
func normalizeAccounts(st *State) {
	if st.Accounts == nil {
		st.Accounts = map[string]Account{}
	}
	if st.AccountSecrets == nil {
		st.AccountSecrets = map[string]AccountSecret{}
	}
	for _, app := range st.Applications {
		if app.Principal == "" {
			continue
		}
		var v Account
		found := false
		for _, old := range st.Accounts {
			if old.Principal == app.Principal {
				v = old
				found = true
				break
			}
		}
		if !found {
			username := strings.TrimPrefix(app.Principal, "User:")
			v = Account{ID: uuid.NewSHA1(uuid.NameSpaceOID, []byte(app.Principal)).String(), Username: username, Principal: app.Principal, Mechanism: "SCRAM-SHA-256", Source: "legacy", CredentialState: "unmanaged", Observation: "identity_only", Revision: 1, ApplicationIDs: []string{}}
		}
		linked := false
		for _, id := range v.ApplicationIDs {
			if id == app.ID {
				linked = true
			}
		}
		if !linked {
			v.ApplicationIDs = append(v.ApplicationIDs, app.ID)
		}
		st.Accounts[v.ID] = v
	}
}

func (s *Service) accountRulesAuth(ctx context.Context, a registry.Actor, id string, rules []kafkaops.ACLRule) error {
	if len(rules) > 0 && s.Gateway == nil {
		return errors.New("DISABLED")
	}
	for _, r := range rules {
		if e := authorizeACL(ctx, s.Registry, a, id, "acl.manage", r); e != nil {
			return e
		}
	}
	return nil
}
func (s *Service) accountRules(ctx context.Context, a registry.Actor, id, principal string, in AccountInput) ([]kafkaops.ACLRule, error) {
	rules := []kafkaops.ACLRule{}
	if in.AccessMode == "" {
		if len(in.Topics) > 0 || in.Group != nil {
			return nil, errInvalid
		}
		return rules, nil
	}
	if (in.AccessMode != "produce" && in.AccessMode != "consume" && in.AccessMode != "produce_consume") || len(in.Topics) < 1 || len(in.Topics) > 50 {
		return nil, errInvalid
	}
	consume := in.AccessMode == "consume" || in.AccessMode == "produce_consume"
	if consume && (in.Group == nil || !validResource(*in.Group)) {
		return nil, errInvalid
	}
	if !consume && in.Group != nil {
		return nil, errInvalid
	}
	seen := map[string]bool{}
	add := func(resource Resource, typ, operation string) {
		r := kafkaops.ACLRule{Principal: principal, Host: "*", ResourceType: typ, ResourceName: resource.Name, PatternType: resource.PatternType, Operation: operation, Permission: "ALLOW"}
		if !seen[key(r)] {
			rules = append(rules, r)
			seen[key(r)] = true
		}
	}
	for _, topic := range in.Topics {
		if !validResource(topic) {
			return nil, errInvalid
		}
		if in.AccessMode != "consume" {
			add(topic, "TOPIC", "WRITE")
		}
		if consume {
			add(topic, "TOPIC", "READ")
		}
		add(topic, "TOPIC", "DESCRIBE")
	}
	if consume {
		add(*in.Group, "GROUP", "READ")
	}
	return rules, s.accountRulesAuth(ctx, a, id, rules)
}
