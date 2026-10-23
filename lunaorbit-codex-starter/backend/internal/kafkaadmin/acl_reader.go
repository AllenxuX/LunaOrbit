package kafkaadmin

import (
	"context"
	"errors"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"lunaorbit/backend/internal/kafkaops"
	"strings"
	"time"
)

func (f *Factory) ReadACLs(ctx context.Context, p Profile) (s kafkaops.ACLSnapshot, err error) {
	defer func() {
		if err != nil {
			var op *kafkaops.Error
			if !errors.As(err, &op) {
				err = &kafkaops.Error{Code: operationCode(err)}
			}
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, f.policy.timeout)
	defer cancel()
	if _, err = f.Probe(ctx, p); err != nil {
		return s, err
	}
	select {
	case f.policy.slots <- struct{}{}:
		defer func() { <-f.policy.slots }()
	case <-ctx.Done():
		return s, ctx.Err()
	}
	opts, e := f.clientOptions(ctx, p)
	if e != nil {
		return s, e
	}
	cl, e := kgo.NewClient(opts...)
	if e != nil {
		return s, safeError("invalid_configuration")
	}
	defer cl.Close()
	req := kmsg.NewPtrDescribeACLsRequest()
	req.ResourceType = 1
	req.ResourcePatternType = 1
	req.Operation = 1
	req.PermissionType = 1
	response, e := req.RequestWith(ctx, cl)
	if e != nil {
		return s, e
	}
	return aclSnapshot(response, p.RegistrationID)
}
func aclSnapshot(r *kmsg.DescribeACLsResponse, id string) (kafkaops.ACLSnapshot, error) {
	s := kafkaops.ACLSnapshot{Items: []kafkaops.ACLRule{}, ObservedAt: time.Now().UTC(), State: "pending", Reason: "Explicit Kafka principal mapping has not been verified"}
	if r.ErrorCode != 0 {
		if errors.Is(kerr.ErrorForCode(r.ErrorCode), kerr.SecurityDisabled) {
			return s, &kafkaops.Error{Code: "AUTHORIZER_UNVERIFIED"}
		}
		return s, &kafkaops.Error{Code: operationCode(kerr.ErrorForCode(r.ErrorCode))}
	}
	for _, r := range r.Resources {
		for _, a := range r.ACLs {
			if len(s.Items) >= 10000 {
				return kafkaops.ACLSnapshot{}, &kafkaops.Error{Code: "CAPACITY_EXCEEDED"}
			}
			s.Items = append(s.Items, kafkaops.ACLRule{Principal: a.Principal, Host: a.Host, ResourceType: strings.ToUpper(r.ResourceType.String()), ResourceName: r.ResourceName, PatternType: strings.ToUpper(r.ResourcePatternType.String()), Operation: strings.ToUpper(a.Operation.String()), Permission: strings.ToUpper(a.PermissionType.String()), SourceRegistrationID: id})
		}
	}
	return s, nil
}
