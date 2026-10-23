package workspace

import (
	"context"
	"errors"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"sort"
	"strings"
	"time"
)

func (s *Service) Execute(ctx context.Context, a registry.Actor, id, keyID string, revision int64) (Request, error) {
	st, e := s.Store.View(ctx, id)
	if e != nil {
		return Request{}, e
	}
	r, ok := st.Requests[keyID]
	if !ok {
		return r, errors.New("NOT_FOUND")
	}
	if e = s.authorizePlan(ctx, a, id, r.Plan); e != nil {
		return r, e
	}
	if s.WriteEnabled == nil {
		return r, errors.New("DISABLED")
	}
	if e = s.WriteEnabled(ctx, a, id); e != nil {
		return r, e
	}
	if r.Revision != revision || r.Status != "approved" || r.ApprovedBy == "" || (r.CreatedBy == r.ApprovedBy && !s.LabAutoApprove) {
		return r, errConflict
	}
	fresh, e := s.Preview(ctx, a, id, r.Plan.Input)
	if e != nil {
		return r, e
	}
	if fresh.Fingerprint != r.Plan.Fingerprint {
		return r, errConflict
	}
	e = s.Store.Update(ctx, id, func(st *State) error {
		old := st.Requests[keyID]
		if old.Revision != revision || old.Status != "approved" {
			return errConflict
		}
		if r.Plan.ApplicationRevision != 0 && st.Applications[r.Plan.Input.ApplicationID].Revision != r.Plan.ApplicationRevision {
			return errConflict
		}
		r.Status = "executing"
		r.Revision++
		st.Requests[keyID] = r
		audit(st, a, "request.execute.start", keyID)
		return nil
	})
	if e != nil {
		return r, e
	}
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	save := func() error {
		safe, done := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer done()
		return s.Store.Update(safe, id, func(st *State) error {
			if st.Requests[keyID].Revision != r.Revision {
				return errConflict
			}
			r.Revision++
			st.Requests[keyID] = r
			audit(st, a, "request.execute."+r.Status, keyID)
			return nil
		})
	}
	apply := func(rule *kafkaops.ACLRule, write func() error, verify func() bool) error {
		err := s.authorizePlan(bounded, a, id, r.Plan)
		if err == nil {
			err = s.WriteEnabled(bounded, a, id)
		}
		if err == nil {
			err = write()
		}
		result := Result{Rule: rule, State: "applied"}
		if err != nil {
			result.State = "unknown"
			result.Code = "WRITE_UNCONFIRMED"
			if code := registry.ErrorCode(err); code == "FORBIDDEN" || code == "NOT_FOUND" {
				result.State = "failed"
				result.Code = code
			}
			var op *kafkaops.Error
			if errors.As(err, &op) {
				result.Code = op.Code
				switch op.Code {
				case "AUTHORIZATION_DENIED", "UNSUPPORTED_API", "AUTHORIZER_UNVERIFIED", "INVALID_ARGUMENT":
					result.State = "failed"
				}
			}
		}
		if !verify() {
			if result.State != "failed" {
				result.State = "unknown"
			}
			if result.Code == "" {
				result.Code = "VERIFICATION_FAILED"
			}
		} else if err != nil {
			result.State = "applied"
			result.Code = "VERIFIED_AFTER_ERROR"
		}
		r.Results = append(r.Results, result)
		return save()
	}
	if strings.HasPrefix(r.Plan.Input.Kind, "topic_") {
		in := r.Plan.Input
		write := func() error {
			switch in.Kind {
			case "topic_create":
				return s.Gateway.CreateTopic(bounded, a, id, in.Topic, in.Partitions, in.ReplicationFactor, in.RetentionMS)
			case "topic_expand":
				return s.Gateway.ExpandTopic(bounded, a, id, in.Topic, in.Partitions)
			default:
				return s.Gateway.AlterTopicRetention(bounded, a, id, in.Topic, *in.RetentionMS)
			}
		}
		e = apply(nil, write, func() bool {
			after, err := s.Gateway.TopicConfigs(bounded, a, id, in.Topic)
			if err != nil {
				return false
			}
			for k, v := range r.Plan.After {
				if after[k] != v {
					return false
				}
			}
			return true
		})
	} else {
		expectedFingerprint := r.Plan.Fingerprint
		for _, rule := range append(append([]kafkaops.ACLRule{}, r.Plan.Additions...), r.Plan.Removals...) {
			rule := rule
			remove := false
			for _, v := range r.Plan.Removals {
				if key(v) == key(rule) {
					remove = true
				}
			}
			e = apply(&rule, func() error {
				// Recheck external drift before each custom rule, accounting for
				// our own confirmed prior mutations in the expected fingerprint.
				if r.Plan.Input.Kind == "acl_custom" {
					snap, err := s.Gateway.ACLs(bounded, a, id)
					if err != nil {
						return err
					}
					current := map[string]bool{}
					for _, v := range snap.Items {
						current[key(v)] = true
					}
					compute := func() string {
						keys := []string{}
						for k := range current {
							keys = append(keys, k)
						}
						sort.Strings(keys)
						return fingerprint(struct {
							Input Input
							Rules []string
						}{r.Plan.Input, keys})
					}
					if compute() != expectedFingerprint {
						return errConflict
					}
					if remove {
						delete(current, key(rule))
					} else {
						current[key(rule)] = true
					}
					expectedFingerprint = compute()
				}
				if remove {
					return s.Gateway.DeleteACL(bounded, a, id, rule)
				}
				return s.Gateway.CreateACL(bounded, a, id, rule)
			}, func() bool {
				snap, err := s.Gateway.ACLs(bounded, a, id)
				if err != nil {
					return false
				}
				present := false
				for _, v := range snap.Items {
					if key(v) == key(rule) {
						present = true
					}
				}
				return present != remove
			})
			if e != nil {
				return r, e
			}
			if bounded.Err() != nil {
				break
			}
		}
	}
	if e != nil {
		return r, e
	}
	r.Status = "executed"
	expectedResults := len(r.Plan.Additions) + len(r.Plan.Removals)
	if strings.HasPrefix(r.Plan.Input.Kind, "topic_") {
		expectedResults = 1
	}
	if len(r.Results) != expectedResults {
		r.Status = "execution_unknown"
	}
	applied := 0
	for _, v := range r.Results {
		if v.State == "applied" {
			applied++
		} else {
			r.Status = "execution_unknown"
		}
	}
	if r.Status != "executed" && applied > 0 {
		r.Status = "partial_failure"
	}
	e = save()
	return r, e
}
