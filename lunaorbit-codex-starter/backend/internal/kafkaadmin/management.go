package kafkaadmin

import (
	"context"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"lunaorbit/backend/internal/kafkaops"
	"regexp"
	"strconv"
)

var topicName = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)

func validTopic(name string) bool { return topicName.MatchString(name) && name != "." && name != ".." }

// Retention is deliberately finite and bounded to one year; unlimited retention
// requires a separate policy and is not exposed through this adapter.
func validRetention(v int64) bool { return v > 0 && v <= 31536000000 }
func managementError(code int16) error {
	if code == 0 {
		return nil
	}
	if code == 3 {
		return &kafkaops.Error{Code: "NOT_FOUND"}
	}
	if code == 54 {
		return &kafkaops.Error{Code: "AUTHORIZER_UNVERIFIED"}
	}
	return &kafkaops.Error{Code: operationCode(kerr.ErrorForCode(code))}
}
func invalidManagement() error   { return &kafkaops.Error{Code: "INVALID_ARGUMENT"} }
func malformedManagement() error { return &kafkaops.Error{Code: "BROKER_UNREACHABLE"} }

// managementClient verifies every seed and discovered listener before creating
// an independently authenticated, policy-bound client. Mutations are never retried.
func (f *Factory) managementClient(ctx context.Context, p Profile, run func(context.Context, *kgo.Client) error) error {
	ctx, cancel := context.WithTimeout(ctx, f.policy.timeout)
	defer cancel()
	if _, err := f.Probe(ctx, p); err != nil {
		return &kafkaops.Error{Code: operationCode(err)}
	}
	select {
	case f.policy.slots <- struct{}{}:
		defer func() { <-f.policy.slots }()
	case <-ctx.Done():
		return &kafkaops.Error{Code: "TIMEOUT"}
	}
	opts, err := f.clientOptions(ctx, p)
	if err != nil {
		return &kafkaops.Error{Code: operationCode(err)}
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return invalidManagement()
	}
	defer cl.Close()
	return run(ctx, cl)
}

func (f *Factory) CreateTopic(ctx context.Context, p Profile, name string, partitions, rf int, retentionMS *int64) error {
	if !validTopic(name) || partitions < 1 || partitions > 100000 || rf < 1 || rf > 32767 || (retentionMS != nil && !validRetention(*retentionMS)) {
		return invalidManagement()
	}
	req := kmsg.NewPtrCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = name
	topic.NumPartitions = int32(partitions)
	topic.ReplicationFactor = int16(rf)
	if retentionMS != nil {
		v := strconv.FormatInt(*retentionMS, 10)
		topic.Configs = []kmsg.CreateTopicsRequestTopicConfig{{Name: "retention.ms", Value: &v}}
	}
	req.Topics = []kmsg.CreateTopicsRequestTopic{topic}
	return f.managementClient(ctx, p, func(ctx context.Context, cl *kgo.Client) error {
		r, e := req.RequestWith(ctx, cl)
		if e != nil {
			return &kafkaops.Error{Code: operationCode(e)}
		}
		if len(r.Topics) != 1 || r.Topics[0].Topic != name {
			return malformedManagement()
		}
		return managementError(r.Topics[0].ErrorCode)
	})
}
func (f *Factory) ExpandTopic(ctx context.Context, p Profile, name string, total int) error {
	if !validTopic(name) || total < 1 || total > 100000 {
		return invalidManagement()
	}
	req := kmsg.NewPtrCreatePartitionsRequest()
	req.Topics = []kmsg.CreatePartitionsRequestTopic{{Topic: name, Count: int32(total)}}
	return f.managementClient(ctx, p, func(ctx context.Context, cl *kgo.Client) error {
		r, e := req.RequestWith(ctx, cl)
		if e != nil {
			return &kafkaops.Error{Code: operationCode(e)}
		}
		if len(r.Topics) != 1 || r.Topics[0].Topic != name {
			return malformedManagement()
		}
		return managementError(r.Topics[0].ErrorCode)
	})
}
func (f *Factory) AlterTopicRetention(ctx context.Context, p Profile, name string, retentionMS int64) error {
	if !validTopic(name) || !validRetention(retentionMS) {
		return invalidManagement()
	}
	v := strconv.FormatInt(retentionMS, 10)
	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	req.Resources = []kmsg.IncrementalAlterConfigsRequestResource{{ResourceType: 2, ResourceName: name, Configs: []kmsg.IncrementalAlterConfigsRequestResourceConfig{{Name: "retention.ms", Op: 0, Value: &v}}}}
	return f.managementClient(ctx, p, func(ctx context.Context, cl *kgo.Client) error {
		r, e := req.RequestWith(ctx, cl)
		if e != nil {
			return &kafkaops.Error{Code: operationCode(e)}
		}
		if len(r.Resources) != 1 || r.Resources[0].ResourceName != name || r.Resources[0].ResourceType != 2 {
			return malformedManagement()
		}
		return managementError(r.Resources[0].ErrorCode)
	})
}

var topicConfigNames = []string{"retention.ms", "retention.bytes", "cleanup.policy", "min.insync.replicas"}

func (f *Factory) TopicConfigs(ctx context.Context, p Profile, name string) (map[string]string, error) {
	if !validTopic(name) {
		return nil, invalidManagement()
	}
	out := map[string]string{}
	req := kmsg.NewPtrDescribeConfigsRequest()
	req.Resources = []kmsg.DescribeConfigsRequestResource{{ResourceType: 2, ResourceName: name, ConfigNames: append([]string(nil), topicConfigNames...)}}
	err := f.managementClient(ctx, p, func(ctx context.Context, cl *kgo.Client) error {
		r, e := req.RequestWith(ctx, cl)
		if e != nil {
			return &kafkaops.Error{Code: operationCode(e)}
		}
		if len(r.Resources) != 1 || r.Resources[0].ResourceName != name || r.Resources[0].ResourceType != 2 {
			return malformedManagement()
		}
		resource := r.Resources[0]
		if e := managementError(resource.ErrorCode); e != nil {
			return e
		}
		for _, c := range resource.Configs {
			for _, allowed := range topicConfigNames {
				if c.Name == allowed && c.Value != nil && !c.IsSensitive {
					out[c.Name] = *c.Value
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (f *Factory) DeleteACL(ctx context.Context, p Profile, rule kafkaops.ACLRule) error {
	c, e := aclCreation(rule)
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, f.policy.timeout)
	defer cancel()
	if _, e = f.ReadACLs(ctx, p); e != nil {
		return e
	}
	req := kmsg.NewPtrDeleteACLsRequest()
	req.Filters = []kmsg.DeleteACLsRequestFilter{{ResourceType: c.ResourceType, ResourceName: &c.ResourceName, ResourcePatternType: c.ResourcePatternType, Principal: &c.Principal, Host: &c.Host, Operation: c.Operation, PermissionType: c.PermissionType}}
	return f.managementClient(ctx, p, func(ctx context.Context, cl *kgo.Client) error {
		r, e := req.RequestWith(ctx, cl)
		if e != nil {
			return &kafkaops.Error{Code: operationCode(e)}
		}
		if len(r.Results) != 1 {
			return malformedManagement()
		}
		if e := managementError(r.Results[0].ErrorCode); e != nil {
			return e
		}
		for _, m := range r.Results[0].MatchingACLs {
			if e := managementError(m.ErrorCode); e != nil {
				return e
			}
			if m.ResourceType != c.ResourceType || m.ResourceName != c.ResourceName || m.ResourcePatternType != c.ResourcePatternType || m.Principal != c.Principal || m.Host != c.Host || m.Operation != c.Operation || m.PermissionType != c.PermissionType {
				return malformedManagement()
			}
		}
		return nil
	})
}
