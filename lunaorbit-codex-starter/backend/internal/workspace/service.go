package workspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"regexp"
	"sort"
	"strings"
	"time"
)

var errInvalid = errors.New("INVALID_ARGUMENT")
var errConflict = errors.New("CONFLICT")
var errCapacity = errors.New("CAPACITY_EXCEEDED")

// Reserve audit space for every saved request to finish all 100 rule writes.
const maxApplications = 128
const maxRequests = 128
const maxAdmissionAudit = 16000

var nameRE = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)

type Service struct {
	Resources      ResourceGateway
	AccountKey     []byte
	AccountGateway AccountGateway
	Registry       *registry.Service
	Store          Store
	Gateway        Gateway
	WriteEnabled   func(context.Context, registry.Actor, string) error
	LabAutoApprove bool
}

func NewService(r *registry.Service, st Store, g Gateway) *Service {
	return &Service{Registry: r, Store: st, Gateway: g}
}
func (s *Service) auth(ctx context.Context, a registry.Actor, id, action, res string) error {
	if a.ID == "" {
		return errors.New("FORBIDDEN")
	}
	return s.Registry.AuthorizeResource(ctx, a, id, action, res)
}
func audit(st *State, a registry.Actor, action, target string) {
	st.Audit = append(st.Audit, Audit{a.ID, action, target, time.Now().UTC()})
}
func validApp(v Application) bool {
	return strings.TrimSpace(v.Name) != "" && len(v.Name) <= 128 && (v.Principal == "" || strings.HasPrefix(v.Principal, "User:") && len(v.Principal) > 5) && len(v.Principal) <= 256 && len(v.Owner) <= 128 && len(v.Description) <= 2048 && !strings.ContainsAny(v.Principal, "\n\r\x00")
}
func (s *Service) SaveApplication(ctx context.Context, a registry.Actor, id string, v Application) (Application, error) {
	if e := s.Registry.AuthorizeUnscopedResource(ctx, a, id, "applications.manage"); e != nil {
		return v, e
	}
	if !validApp(v) {
		return v, errInvalid
	}
	if v.RequestID != "" {
		parsed, e := uuid.Parse(v.RequestID)
		if e != nil || parsed.String() != v.RequestID {
			return v, errInvalid
		}
	}
	e := s.Store.Update(ctx, id, func(st *State) error {
		if v.ID == "" && v.RequestID != "" {
			for _, old := range st.Applications {
				if old.RequestID == v.RequestID {
					if old.Name != v.Name || old.Principal != v.Principal || old.Owner != v.Owner || old.Description != v.Description {
						return errConflict
					}
					v = old
					return nil
				}
			}
		}
		if len(st.Audit) >= maxAdmissionAudit {
			return errCapacity
		}
		if v.ID == "" {
			if len(st.Applications) >= maxApplications {
				return errCapacity
			}
			v.ID = uuid.NewString()
			v.Revision = 1
		} else {
			old, ok := st.Applications[v.ID]
			if !ok {
				return errors.New("NOT_FOUND")
			}
			if old.Revision != v.Revision {
				return errConflict
			}
			if v.RequestID != "" && v.RequestID != old.RequestID {
				return errConflict
			}
			v.RequestID = old.RequestID
			if old.Principal != v.Principal {
				return errConflict
			}
			for _, r := range st.Requests {
				if r.Plan.Input.ApplicationID == v.ID && (r.Status == "pending_approval" || r.Status == "approved" || r.Status == "executing") {
					return errConflict
				}
			}
			v.Revision++
		}
		st.Applications[v.ID] = v
		audit(st, a, "application.save", v.ID)
		return nil
	})
	return v, e
}
func (s *Service) DeleteApplication(ctx context.Context, a registry.Actor, id, key string, rev int64) error {
	if e := s.Registry.AuthorizeUnscopedResource(ctx, a, id, "applications.manage"); e != nil {
		return e
	}
	return s.Store.Update(ctx, id, func(st *State) error {
		if len(st.Audit) >= maxAdmissionAudit {
			return errCapacity
		}
		v, ok := st.Applications[key]
		if !ok {
			return errors.New("NOT_FOUND")
		}
		if v.Revision != rev {
			return errConflict
		}
		for _, r := range st.Requests {
			if r.Plan.Input.ApplicationID == key && r.Status != "execution_failed" {
				return errConflict
			}
		}
		delete(st.Applications, key)
		for accountID, account := range st.Accounts {
			linked := false
			remaining := []string{}
			for _, appID := range account.ApplicationIDs {
				if appID == key {
					linked = true
				} else {
					remaining = append(remaining, appID)
				}
			}
			if linked {
				account.ApplicationIDs = remaining
				account.Revision++
				st.Accounts[accountID] = account
			}
		}
		audit(st, a, "application.delete", key)
		return nil
	})
}
func key(r kafkaops.ACLRule) string {
	r.SourceRegistrationID = ""
	b, _ := json.Marshal(r)
	return string(b)
}
func fingerprint(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func validResource(r Resource) bool {
	return nameRE.MatchString(r.Name) && (r.PatternType == "LITERAL" || r.PatternType == "PREFIXED")
}
func (s *Service) Preview(ctx context.Context, a registry.Actor, id string, in Input) (Plan, error) {
	p := Plan{Input: in, Additions: []kafkaops.ACLRule{}, Existing: []kafkaops.ACLRule{}, Removals: []kafkaops.ACLRule{}, Before: map[string]string{}, After: map[string]string{}}
	p.Input.Fingerprint = ""
	action := "acl.manage"
	if strings.HasPrefix(in.Kind, "topic_") {
		action = "topic.manage"
	}
	if e := s.auth(ctx, a, id, action, ""); e != nil {
		return p, e
	}
	if s.Gateway == nil {
		return p, errors.New("DISABLED")
	}
	if action == "topic.manage" {
		if !nameRE.MatchString(in.Topic) {
			return p, errInvalid
		}
		if e := s.auth(ctx, a, id, action, "topic:"+in.Topic); e != nil {
			return p, e
		}
		before, e := s.Gateway.TopicConfigs(ctx, a, id, in.Topic)
		if e != nil && in.Kind != "topic_create" {
			return p, e
		}
		if in.Kind == "topic_create" && e == nil {
			return p, errConflict
		}
		if in.Kind == "topic_create" && e != nil && e.Error() != "NOT_FOUND" {
			var op *kafkaops.Error
			if !errors.As(e, &op) || op.Code != "NOT_FOUND" {
				return p, e
			}
		}
		if before != nil {
			p.Before = before
		}
		for k, v := range p.Before {
			p.After[k] = v
		}
		switch in.Kind {
		case "topic_create":
			if in.Partitions < 1 || in.Partitions > 10000 || in.ReplicationFactor < 1 || in.ReplicationFactor > 20 {
				return p, errInvalid
			}
			p.After["partitions"] = fmt.Sprint(in.Partitions)
			p.After["replication_factor"] = fmt.Sprint(in.ReplicationFactor)
		case "topic_expand":
			var n int
			fmt.Sscan(p.Before["partitions"], &n)
			if n < 1 || in.Partitions <= n || in.Partitions > 10000 {
				return p, errInvalid
			}
			p.After["partitions"] = fmt.Sprint(in.Partitions)
		case "topic_retention":
		default:
			return p, errInvalid
		}
		if in.Kind == "topic_retention" || in.RetentionMS != nil {
			if in.RetentionMS == nil || *in.RetentionMS < 60000 || *in.RetentionMS > 31536000000 {
				return p, errInvalid
			}
			p.After["retention.ms"] = fmt.Sprint(*in.RetentionMS)
		}
		p.Fingerprint = fingerprint(struct {
			Input  Input
			Before map[string]string
		}{p.Input, p.Before})
		return p, nil
	}
	if in.Kind == "acl_custom" {
		return s.previewCustomACL(ctx, a, id, p)
	}
	st, e := s.Store.View(ctx, id)
	if e != nil {
		return p, e
	}
	app, ok := st.Applications[in.ApplicationID]
	if !ok {
		return p, errors.New("NOT_FOUND")
	}
	if in.AccountID != "" {
		account, exists := st.Accounts[in.AccountID]
		linked := false
		for _, appID := range account.ApplicationIDs {
			if appID == app.ID {
				linked = true
			}
		}
		if !exists || !linked {
			return p, errors.New("NOT_FOUND")
		}
		app.Principal = account.Principal
	}
	if app.Principal == "" {
		return p, errInvalid
	}
	p.ApplicationRevision = app.Revision
	snap, e := s.Gateway.ACLs(ctx, a, id)
	if e != nil {
		return p, e
	}
	if snap.State != "available" && snap.State != "observed" && snap.State != "pending" && snap.State != "" {
		return p, errors.New("AUTHORIZER_UNVERIFIED")
	}
	rules := map[string]bool{}
	for _, r := range snap.Items {
		rules[key(r)] = true
	}
	desired := []kafkaops.ACLRule{}
	if in.Kind == "revoke" {
		if len(in.SelectedRules) < 1 || len(in.SelectedRules) > 100 {
			return p, errInvalid
		}
		for _, r := range in.SelectedRules {
			if r.Principal != app.Principal || !kafkaops.ValidACLRule(r) {
				return p, errInvalid
			}
			if e = authorizeACL(ctx, s.Registry, a, id, action, r); e != nil {
				return p, e
			}
			if !rules[key(r)] {
				return p, errConflict
			}
			p.Removals = append(p.Removals, r)
		}
	} else if in.Kind == "grant" {
		if len(in.Topics) < 1 || len(in.Topics) > 50 || (in.Mode != "produce" && in.Mode != "consume") {
			return p, errInvalid
		}
		add := func(r Resource, typ, op string) {
			desired = append(desired, kafkaops.ACLRule{Principal: app.Principal, Host: "*", ResourceType: typ, ResourceName: r.Name, PatternType: r.PatternType, Operation: op, Permission: "ALLOW"})
		}
		for _, t := range in.Topics {
			if !validResource(t) {
				return p, errInvalid
			}
			if e = s.auth(ctx, a, id, action, "topic:"+t.Name); e != nil {
				return p, e
			}
			op := "WRITE"
			if in.Mode == "consume" {
				op = "READ"
			}
			add(t, "TOPIC", op)
			add(t, "TOPIC", "DESCRIBE")
		}
		if in.Mode == "consume" {
			if in.Group == nil || !validResource(*in.Group) {
				return p, errInvalid
			}
			if e = s.auth(ctx, a, id, action, "group:"+in.Group.Name); e != nil {
				return p, e
			}
			add(*in.Group, "GROUP", "READ")
		}
		seen := map[string]bool{}
		for _, r := range desired {
			if seen[key(r)] {
				continue
			}
			seen[key(r)] = true
			if rules[key(r)] {
				p.Existing = append(p.Existing, r)
			} else {
				p.Additions = append(p.Additions, r)
			}
		}
	} else {
		return p, errInvalid
	}
	keys := []string{}
	for k := range rules {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	p.Fingerprint = fingerprint(struct {
		Input     Input
		Revision  int64
		Principal string
		Rules     []string
	}{p.Input, app.Revision, app.Principal, keys})
	return p, nil
}
func (s *Service) CreateRequest(ctx context.Context, a registry.Actor, id string, in Input) (Request, error) {
	p, e := s.Preview(ctx, a, id, in)
	if e != nil {
		return Request{}, e
	}
	if in.Fingerprint == "" || p.Fingerprint != in.Fingerprint {
		return Request{}, errConflict
	}
	r := Request{ID: uuid.NewString(), Revision: 1, Status: "pending_approval", Plan: p, CreatedBy: a.ID, Results: []Result{}, CreatedAt: time.Now().UTC()}
	if s.LabAutoApprove && s.WriteEnabled != nil && s.WriteEnabled(ctx, a, id) == nil {
		r.Status = "approved"
		r.ApprovedBy = a.ID
	}
	e = s.Store.Update(ctx, id, func(st *State) error {
		if len(st.Requests) >= maxRequests || len(st.Audit) >= maxAdmissionAudit {
			return errCapacity
		}
		if p.ApplicationRevision != 0 && st.Applications[in.ApplicationID].Revision != p.ApplicationRevision {
			return errConflict
		}
		st.Requests[r.ID] = r
		audit(st, a, "request.create", r.ID)
		if r.Status == "approved" {
			audit(st, a, "request.approve", r.ID)
		}
		return nil
	})
	return r, e
}
func (s *Service) Approve(ctx context.Context, a registry.Actor, id, key string, rev int64) (Request, error) {
	var out Request
	snapshot, e := s.Store.View(ctx, id)
	if e != nil {
		return out, e
	}
	observed, ok := snapshot.Requests[key]
	if !ok {
		return out, errors.New("NOT_FOUND")
	}
	if e = s.authorizePlan(ctx, a, id, observed.Plan); e != nil {
		return out, e
	}
	if observed.Revision != rev || observed.Status != "pending_approval" {
		return out, errConflict
	}
	if observed.CreatedBy == a.ID && !s.LabAutoApprove {
		return out, errors.New("SELF_APPROVAL_DENIED")
	}
	if s.LabAutoApprove && (s.WriteEnabled == nil || s.WriteEnabled(ctx, a, id) != nil) {
		return out, errors.New("DISABLED")
	}
	e = s.Store.Update(ctx, id, func(st *State) error {
		r, ok := st.Requests[key]
		if !ok {
			return errors.New("NOT_FOUND")
		}
		if r.Revision != rev || r.Status != "pending_approval" || r.Plan.Fingerprint != observed.Plan.Fingerprint || r.CreatedBy != observed.CreatedBy {
			return errConflict
		}
		r.ApprovedBy = a.ID
		r.Revision++
		r.Status = "approved"
		st.Requests[key] = r
		audit(st, a, "request.approve", key)
		out = r
		return nil
	})
	return out, e
}
func (s *Service) authorizePlan(ctx context.Context, a registry.Actor, id string, p Plan) error {
	action := "acl.manage"
	if strings.HasPrefix(p.Input.Kind, "topic_") {
		action = "topic.manage"
	}
	return s.authorizePlanAction(ctx, a, id, p, action)
}
func (s *Service) authorizePlanAction(ctx context.Context, a registry.Actor, id string, p Plan, action string) error {
	if e := s.auth(ctx, a, id, action, ""); e != nil {
		return e
	}
	if p.Input.Topic != "" {
		if e := s.auth(ctx, a, id, action, "topic:"+p.Input.Topic); e != nil {
			return e
		}
	}
	for _, t := range p.Input.Topics {
		if e := s.auth(ctx, a, id, action, "topic:"+t.Name); e != nil {
			return e
		}
	}
	if p.Input.Group != nil {
		if e := s.auth(ctx, a, id, action, "group:"+p.Input.Group.Name); e != nil {
			return e
		}
	}
	rules := append([]kafkaops.ACLRule{}, p.Additions...)
	rules = append(rules, p.Existing...)
	rules = append(rules, p.Removals...)
	rules = append(rules, p.Retained...)
	rules = append(rules, p.Input.SelectedRules...)
	rules = append(rules, p.Input.DesiredRules...)
	for _, r := range rules {
		if e := authorizeACL(ctx, s.Registry, a, id, action, r); e != nil {
			return e
		}
	}
	return nil
}
