package workspace

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"lunaorbit/backend/internal/kafkaadmin"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ConfigSnapshot = kafkaadmin.ConfigSnapshot
type ResourceTarget = kafkaadmin.ConfigResource
type ResourceGateway interface {
	DescribeConfigs(context.Context, registry.Actor, string, kafkaadmin.ConfigResource) (ConfigSnapshot, error)
	ValidateConfigs(context.Context, registry.Actor, string, kafkaadmin.ConfigResource, []kafkaadmin.ConfigChange) error
	AlterConfigs(context.Context, registry.Actor, string, kafkaadmin.ConfigResource, []kafkaadmin.ConfigChange) error
	DeleteTopic(context.Context, registry.Actor, string, string) error
	Reassign(context.Context, registry.Actor, string, string, []kafkaadmin.ReplicaAssignment) error
	CancelReassignments(context.Context, registry.Actor, string, string, []int32) error
	Reassignments(context.Context, registry.Actor, string, string) ([]kafkaadmin.PartitionReassignment, error)
	TopicState(context.Context, registry.Actor, string, string) (*kafkaops.Topic, error)
}
type ResourceIntent struct {
	Kind             string                         `json:"kind"`
	Resource         ResourceTarget                 `json:"resource"`
	Changes          []kafkaadmin.ConfigChange      `json:"changes,omitempty"`
	Assignments      []kafkaadmin.ReplicaAssignment `json:"assignments,omitempty"`
	Partitions       []int32                        `json:"partitions,omitempty"`
	ConfirmationName string                         `json:"confirmation_name,omitempty"`
}
type ResourcePlan struct {
	Input       ResourceIntent                     `json:"input"`
	Before      *ConfigSnapshot                    `json:"before,omitempty"`
	After       *ConfigSnapshot                    `json:"after,omitempty"`
	Topic       *kafkaops.Topic                    `json:"topic,omitempty"`
	Assignments []kafkaadmin.PartitionReassignment `json:"assignments,omitempty"`
	Fingerprint string                             `json:"fingerprint"`
}
type ResourceOperation struct {
	ID         string       `json:"id"`
	RequestID  string       `json:"request_id"`
	Revision   int64        `json:"revision"`
	Status     string       `json:"status"`
	Plan       ResourcePlan `json:"plan"`
	CreatedBy  string       `json:"created_by"`
	ApprovedBy string       `json:"approved_by"`
	ExecutedBy string       `json:"executed_by"`
	Code       string       `json:"code,omitempty"`
	CreatedAt  time.Time    `json:"created_at"`
}

func secretResourceConfig(name string) bool {
	n := strings.ToLower(name)
	for _, v := range []string{"password", "secret", "credential", "jaas", "token", "private.key", "private_key"} {
		if strings.Contains(n, v) {
			return true
		}
	}
	return false
}
func validateResourceTarget(r ResourceTarget) error {
	if r.Type == "TOPIC" && nameRE.MatchString(r.Name) {
		return nil
	}
	if r.Type == "BROKER" {
		if r.Name == "" {
			return nil
		}
		n, e := strconv.ParseInt(r.Name, 10, 32)
		if e == nil && n >= 0 && strconv.FormatInt(n, 10) == r.Name {
			return nil
		}
	}
	return errInvalid
}
func validateResourceIntent(in ResourceIntent) error {
	if e := validateResourceTarget(in.Resource); e != nil {
		return e
	}
	if in.Resource.Type == "TOPIC" && strings.HasPrefix(in.Resource.Name, "__") {
		return errInvalid
	}
	if in.Kind != "configs" && in.Resource.Type != "TOPIC" {
		return errInvalid
	}
	switch in.Kind {
	case "configs":
		if len(in.Changes) == 0 || len(in.Changes) > 128 || len(in.Assignments) > 0 || len(in.Partitions) > 0 || in.ConfirmationName != "" {
			return errInvalid
		}
		seen := map[string]bool{}
		for _, c := range in.Changes {
			if c.Name == "" || len(c.Name) > 512 || seen[c.Name] || secretResourceConfig(c.Name) || (c.Operation != "SET" && c.Operation != "DELETE") || (c.Operation == "SET" && (c.Value == nil || len(*c.Value) > 8192)) || (c.Operation == "DELETE" && c.Value != nil) {
				return errInvalid
			}
			seen[c.Name] = true
		}
	case "topic_delete":
		if in.ConfirmationName != in.Resource.Name || len(in.Changes) > 0 || len(in.Assignments) > 0 || len(in.Partitions) > 0 {
			return errInvalid
		}
	case "reassign":
		if len(in.Assignments) == 0 || len(in.Assignments) > 128 || len(in.Changes) > 0 || len(in.Partitions) > 0 || in.ConfirmationName != "" {
			return errInvalid
		}
	case "reassign_cancel":
		if len(in.Partitions) == 0 || len(in.Partitions) > 128 || len(in.Changes) > 0 || len(in.Assignments) > 0 || in.ConfirmationName != "" {
			return errInvalid
		}
		seen := map[int32]bool{}
		for _, p := range in.Partitions {
			if p < 0 || seen[p] {
				return errInvalid
			}
			seen[p] = true
		}
	default:
		return errInvalid
	}
	return nil
}
func (s *Service) resourceAuth(ctx context.Context, a registry.Actor, id string, r ResourceTarget, write bool) error {
	if e := validateResourceTarget(r); e != nil {
		return e
	}
	action := "read"
	if write {
		action = "topic.manage"
	}
	if r.Type == "BROKER" {
		if write || a.Role != "information" {
			action = "topic.manage"
		}
		if e := s.Registry.AuthorizeUnscopedResource(ctx, a, id, action); e != nil {
			return e
		}
	} else if e := s.auth(ctx, a, id, action, "topic:"+r.Name); e != nil {
		return e
	}
	if s.Resources == nil {
		return errors.New("DISABLED")
	}
	return nil
}
func (s *Service) resourceWrite(ctx context.Context, a registry.Actor, id string, r ResourceTarget) error {
	if e := s.resourceAuth(ctx, a, id, r, true); e != nil {
		return e
	}
	if s.WriteEnabled == nil {
		return errors.New("DISABLED")
	}
	return s.WriteEnabled(ctx, a, id)
}
func cleanResourceSnapshot(in ConfigSnapshot) ConfigSnapshot {
	out := in
	out.Entries = append([]kafkaadmin.ConfigEntry{}, in.Entries...)
	for i := range out.Entries {
		c := &out.Entries[i]
		c.Synonyms = append([]kafkaadmin.ConfigSynonym{}, c.Synonyms...)
		if c.Sensitive || secretResourceConfig(c.Name) || c.Type == "PASSWORD" {
			c.Sensitive = true
			c.Value = nil
			c.DefaultValue = nil
			c.Documentation = ""
			for j := range c.Synonyms {
				c.Synonyms[j].Value = nil
			}
		}
		for j := range c.Synonyms {
			if secretResourceConfig(c.Synonyms[j].Name) {
				c.Synonyms[j].Value = nil
			}
		}
	}
	sort.Slice(out.Entries, func(i, j int) bool { return out.Entries[i].Name < out.Entries[j].Name })
	return out
}
func (s *Service) ResourceConfigs(ctx context.Context, a registry.Actor, id string, r ResourceTarget) (ConfigSnapshot, error) {
	if e := s.resourceAuth(ctx, a, id, r, false); e != nil {
		return ConfigSnapshot{}, e
	}
	v, e := s.Resources.DescribeConfigs(ctx, a, id, r)
	return cleanResourceSnapshot(v), e
}
func (s *Service) PreviewResource(ctx context.Context, a registry.Actor, id string, in ResourceIntent) (ResourcePlan, error) {
	p := ResourcePlan{Input: in}
	if e := validateResourceIntent(in); e != nil {
		return p, e
	}
	if e := s.resourceAuth(ctx, a, id, in.Resource, true); e != nil {
		return p, e
	}
	if in.Kind == "configs" {
		if in.Resource.Type == "TOPIC" {
			topic, e := s.Resources.TopicState(ctx, a, id, in.Resource.Name)
			if e != nil {
				return p, e
			}
			if topic == nil {
				return p, errors.New("NOT_FOUND")
			}
			if topic.Internal || topic.ErrorCode != "" {
				return p, errInvalid
			}
			p.Topic = topic
		}
		before, e := s.Resources.DescribeConfigs(ctx, a, id, in.Resource)
		if e != nil {
			return p, e
		}
		before = cleanResourceSnapshot(before)
		after := cleanResourceSnapshot(before)
		for _, change := range in.Changes {
			found := false
			for i, c := range after.Entries {
				if c.Name != change.Name {
					continue
				}
				if c.Sensitive || c.ReadOnly {
					return p, errInvalid
				}
				found = true
				if change.Operation == "SET" {
					after.Entries[i].Value = change.Value
					after.Entries[i].Source = "DYNAMIC_OVERRIDE"
				} else {
					after.Entries[i].Value = nil
					for _, synonym := range c.Synonyms {
						if synonym.Source != resourceOverrideSource(in.Resource) && synonym.Source != "UNKNOWN_CONFIG" && synonym.Source != "" && synonym.Value != nil {
							after.Entries[i].Value = synonym.Value
							break
						}
					}
					after.Entries[i].Source = "INHERITED"
				}
			}
			if !found {
				return p, errInvalid
			}
		}
		if e = s.Resources.ValidateConfigs(ctx, a, id, in.Resource, in.Changes); e != nil {
			return p, e
		}
		p.Before = &before
		p.After = &after
	} else {
		topic, e := s.Resources.TopicState(ctx, a, id, in.Resource.Name)
		if e != nil {
			return p, e
		}
		if topic == nil {
			return p, errors.New("NOT_FOUND")
		}
		if topic.Internal || topic.ErrorCode != "" {
			return p, errInvalid
		}
		p.Topic = topic
		if in.Kind == "reassign" || in.Kind == "reassign_cancel" {
			p.Assignments, e = s.Resources.Reassignments(ctx, a, id, in.Resource.Name)
			if e != nil {
				return p, e
			}
			if e = validateResourceAssignments(in, topic, p.Assignments); e != nil {
				return p, e
			}
		}
	}
	p.Fingerprint = fingerprint(p)
	return p, nil
}
func (s *Service) CreateResourceOperation(ctx context.Context, a registry.Actor, id string, in ResourceIntent, fp, requestID string) (ResourceOperation, error) {
	var out ResourceOperation
	if e := s.resourceWrite(ctx, a, id, in.Resource); e != nil {
		return out, e
	}
	u, e := uuid.Parse(requestID)
	if e != nil || u.String() != requestID {
		return out, errInvalid
	}
	st, e := s.Store.View(ctx, id)
	if e != nil {
		return out, e
	}
	for _, op := range st.ResourceOperations {
		if op.RequestID == requestID {
			if !reflect.DeepEqual(op.Plan.Input, in) || op.Plan.Fingerprint != fp || op.CreatedBy != a.ID {
				return out, errConflict
			}
			return op, nil
		}
	}
	plan, e := s.PreviewResource(ctx, a, id, in)
	if e != nil {
		return out, e
	}
	if fp != plan.Fingerprint {
		return out, errConflict
	}
	out = ResourceOperation{ID: uuid.NewString(), RequestID: requestID, Revision: 1, Status: "pending_approval", Plan: plan, CreatedBy: a.ID, CreatedAt: time.Now().UTC()}
	if s.LabAutoApprove {
		out.Status = "approved"
		out.ApprovedBy = a.ID
	}
	e = s.Store.Update(ctx, id, func(st *State) error {
		if st.ResourceOperations == nil {
			st.ResourceOperations = map[string]ResourceOperation{}
		}
		for _, op := range st.ResourceOperations {
			if op.RequestID == requestID {
				if !reflect.DeepEqual(op.Plan.Input, in) || op.Plan.Fingerprint != fp || op.CreatedBy != a.ID {
					return errConflict
				}
				out = op
				return nil
			}
		}
		if len(st.ResourceOperations) >= 128 || len(st.Audit) >= maxAdmissionAudit {
			return errCapacity
		}
		st.ResourceOperations[out.ID] = out
		audit(st, a, "resource.request", out.ID)
		return nil
	})
	return out, e
}
func (s *Service) resourceOperation(ctx context.Context, a registry.Actor, id, key string, write bool) (ResourceOperation, error) {
	st, e := s.Store.View(ctx, id)
	if e != nil {
		return ResourceOperation{}, e
	}
	op, ok := st.ResourceOperations[key]
	if !ok {
		return op, errors.New("NOT_FOUND")
	}
	if write {
		e = s.resourceWrite(ctx, a, id, op.Plan.Input.Resource)
	} else {
		e = s.resourceAuth(ctx, a, id, op.Plan.Input.Resource, false)
	}
	return op, e
}
func (s *Service) ApproveResourceOperation(ctx context.Context, a registry.Actor, id, key string, rev int64) (ResourceOperation, error) {
	op, e := s.resourceOperation(ctx, a, id, key, true)
	if e != nil {
		return op, e
	}
	if op.CreatedBy == a.ID && !s.LabAutoApprove {
		return op, errors.New("SELF_APPROVAL_DENIED")
	}
	if op.Status != "pending_approval" || op.Revision != rev {
		return op, errConflict
	}
	return s.updateResourceOperation(ctx, a, id, op, "resource.approve", func(v *ResourceOperation) { v.Status = "approved"; v.ApprovedBy = a.ID })
}
func (s *Service) updateResourceOperation(ctx context.Context, a registry.Actor, id string, op ResourceOperation, action string, f func(*ResourceOperation)) (ResourceOperation, error) {
	out := op
	e := s.Store.Update(ctx, id, func(st *State) error {
		if len(st.Audit) >= maxAdmissionAudit+4096 {
			return errCapacity
		}
		old, ok := st.ResourceOperations[op.ID]
		if !ok {
			return errors.New("NOT_FOUND")
		}
		if old.Revision != op.Revision || old.Status != op.Status {
			return errConflict
		}
		out = old
		f(&out)
		out.Revision++
		st.ResourceOperations[out.ID] = out
		audit(st, a, action, out.ID)
		return nil
	})
	return out, e
}
func (s *Service) ExecuteResourceOperation(ctx context.Context, a registry.Actor, id, key string, rev int64) (ResourceOperation, error) {
	op, e := s.resourceOperation(ctx, a, id, key, true)
	if e != nil {
		return op, e
	}
	if op.Status != "approved" || op.Revision != rev {
		return op, errConflict
	}
	current, e := s.PreviewResource(ctx, a, id, op.Plan.Input)
	if e != nil {
		return op, e
	}
	if current.Fingerprint != op.Plan.Fingerprint {
		return op, errConflict
	}
	op, e = s.updateResourceOperation(ctx, a, id, op, "resource.submit", func(v *ResourceOperation) { v.Status = "submitting"; v.ExecutedBy = a.ID })
	if e != nil {
		return op, e
	}
	// The durable submitting reservation prevents duplicate Kafka writes after timeout or restart.
	submitCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	e = s.resourceWrite(submitCtx, a, id, op.Plan.Input.Resource)
	if e == nil {
		in := op.Plan.Input
		switch in.Kind {
		case "configs":
			e = s.Resources.AlterConfigs(submitCtx, a, id, in.Resource, in.Changes)
		case "topic_delete":
			if identified, ok := s.Resources.(interface {
				DeleteTopicIdentified(context.Context, registry.Actor, string, string, string) error
			}); ok && op.Plan.Topic != nil && op.Plan.Topic.TopicID != "" {
				e = identified.DeleteTopicIdentified(submitCtx, a, id, in.Resource.Name, op.Plan.Topic.TopicID)
			} else {
				e = s.Resources.DeleteTopic(submitCtx, a, id, in.Resource.Name)
			}
		case "reassign":
			e = s.Resources.Reassign(submitCtx, a, id, in.Resource.Name, in.Assignments)
		case "reassign_cancel":
			e = s.Resources.CancelReassignments(submitCtx, a, id, in.Resource.Name, in.Partitions)
		}
	}
	status, code := "observing", ""
	if e != nil {
		status = "unknown"
		code = resourceErrorCode(e)
		if definiteResourceFailure(code) {
			status = "failed"
		}
	}
	// Saving uses an independent short context so a canceled request still records an uncertain outcome.
	saveCtx, saveCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer saveCancel()
	return s.updateResourceOperation(saveCtx, a, id, op, "resource.acceptance", func(v *ResourceOperation) { v.Status = status; v.Code = code })
}
func resourceErrorCode(e error) string {
	var op *kafkaops.Error
	if errors.As(e, &op) {
		return op.Code
	}
	for _, code := range []string{"FORBIDDEN", "AUTHORIZATION_DENIED", "DISABLED", "INVALID_ARGUMENT", "UNSUPPORTED_API", "TIMEOUT", "NOT_FOUND"} {
		if e.Error() == code {
			return code
		}
	}
	return "OPERATION_UNCERTAIN"
}
func (s *Service) RefreshResourceOperation(ctx context.Context, a registry.Actor, id, key string, rev int64) (ResourceOperation, error) {
	op, e := s.resourceOperation(ctx, a, id, key, true)
	if e != nil {
		return op, e
	}
	if op.Revision != rev || (op.Status != "observing" && op.Status != "unknown" && op.Status != "submitting" && op.Status != "partial_failure") {
		return op, errConflict
	}
	status, code := s.observeResource(ctx, a, id, op)
	return s.updateResourceOperation(ctx, a, id, op, "resource.observe", func(v *ResourceOperation) { v.Status = status; v.Code = code })
}
func definiteResourceFailure(code string) bool {
	switch code {
	case "FORBIDDEN", "AUTHORIZATION_DENIED", "DISABLED", "INVALID_ARGUMENT", "UNSUPPORTED_API", "TOPIC_DELETION_DISABLED", "AUTHENTICATION_FAILED", "AUTHORIZER_UNVERIFIED":
		return true
	}
	return false
}
