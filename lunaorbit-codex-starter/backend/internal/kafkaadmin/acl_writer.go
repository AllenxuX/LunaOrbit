package kafkaadmin

import (
	"context"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"lunaorbit/backend/internal/kafkaops"
	"strings"
)

func aclCreation(r kafkaops.ACLRule) (kmsg.CreateACLsRequestCreation, error) {
	c := kmsg.NewCreateACLsRequestCreation()
	if !kafkaops.ValidACLRule(r) {
		return c, &kafkaops.Error{Code: "INVALID_ARGUMENT"}
	}
	for i := int8(2); i <= 7; i++ {
		v := kmsg.ACLResourceType(i)
		if strings.ToUpper(v.String()) == r.ResourceType {
			c.ResourceType = v
		}
	}
	for i := int8(3); i <= 14; i++ {
		v := kmsg.ACLOperation(i)
		if strings.ToUpper(v.String()) == r.Operation {
			c.Operation = v
		}
	}
	if r.Operation == "ALL" {
		c.Operation = 2
	}
	switch r.PatternType {
	case "LITERAL":
		c.ResourcePatternType = 3
	case "PREFIXED":
		c.ResourcePatternType = 4
	}
	switch r.Permission {
	case "ALLOW":
		c.PermissionType = 3
	case "DENY":
		c.PermissionType = 2
	}
	if c.ResourceType < 2 || c.Operation < 2 || c.ResourcePatternType < 3 || c.PermissionType < 2 || r.ResourceName == "" || r.Host == "" || strings.ContainsAny(r.Principal+r.Host+r.ResourceName, "\x00\r\n") {
		return c, &kafkaops.Error{Code: "INVALID_ARGUMENT"}
	}
	c.ResourceName = r.ResourceName
	c.Principal = r.Principal
	c.Host = r.Host
	return c, nil
}

// CreateACL performs preflight authorizer discovery and creates exactly one rule.
func (f *Factory) CreateACL(ctx context.Context, p Profile, r kafkaops.ACLRule) error {
	c, e := aclCreation(r)
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, f.policy.timeout)
	defer cancel()
	if _, e = f.ReadACLs(ctx, p); e != nil {
		return e
	}
	select {
	case f.policy.slots <- struct{}{}:
		defer func() { <-f.policy.slots }()
	case <-ctx.Done():
		return &kafkaops.Error{Code: "TIMEOUT"}
	}
	opts, e := f.clientOptions(ctx, p)
	if e != nil {
		return &kafkaops.Error{Code: operationCode(e)}
	}
	cl, e := kgo.NewClient(opts...)
	if e != nil {
		return &kafkaops.Error{Code: "INVALID_ARGUMENT"}
	}
	defer cl.Close()
	req := kmsg.NewPtrCreateACLsRequest()
	req.Creations = []kmsg.CreateACLsRequestCreation{c}
	resp, e := req.RequestWith(ctx, cl)
	if e != nil {
		return &kafkaops.Error{Code: operationCode(e)}
	}
	if len(resp.Results) != 1 {
		return &kafkaops.Error{Code: "BROKER_UNREACHABLE"}
	}
	if resp.Results[0].ErrorCode == 54 {
		return &kafkaops.Error{Code: "AUTHORIZER_UNVERIFIED"}
	}
	if resp.Results[0].ErrorCode != 0 {
		return &kafkaops.Error{Code: operationCode(kerr.ErrorForCode(resp.Results[0].ErrorCode))}
	}
	return nil
}
