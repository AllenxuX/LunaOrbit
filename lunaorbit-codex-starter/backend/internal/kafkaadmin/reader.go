package kafkaadmin

import (
	"context"
	"encoding/hex"
	"errors"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"lunaorbit/backend/internal/kafkaops"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
)

type boundReader struct {
	f *Factory
	p Profile
}

func (f *Factory) BindReader(p Profile) kafkaops.Reader {
	p.BootstrapServers = append([]string(nil), p.BootstrapServers...)
	return boundReader{f, p}
}
func (r boundReader) Read(ctx context.Context, kind, resource string) (kafkaops.Snapshot, error) {
	return r.f.Read(ctx, r.p, kind, resource)
}
func (f *Factory) Read(ctx context.Context, p Profile, kind, resource string) (s kafkaops.Snapshot, err error) {
	defer func() {
		if err != nil {
			err = &kafkaops.Error{Code: operationCode(err)}
		}
	}()
	switch kind {
	case "brokers", "topics", "groups":
	case "offsets":
		if resource == "" {
			return s, safeError("invalid_configuration")
		}
	default:
		return s, safeError("unsupported_api")
	}
	ctx, outerCancel := context.WithTimeout(ctx, f.policy.timeout)
	defer outerCancel()
	// Probe validates every seed and discovered advertised listener before reads.
	if _, err = f.Probe(ctx, p); err != nil {
		return s, err
	}
	ctx, cancel := context.WithTimeout(ctx, f.policy.timeout)
	defer cancel()
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
	s = kafkaops.Snapshot{ObservedAt: time.Now().UTC(), State: "verified", Brokers: []kafkaops.Broker{}, Topics: []kafkaops.Topic{}, Groups: []kafkaops.Group{}, Offsets: []kafkaops.Offset{}, Capabilities: []kafkaops.Capability{}}
	switch kind {
	case "brokers", "topics":
		req := kmsg.NewPtrMetadataRequest()
		req.AllowAutoTopicCreation = false
		if kind == "brokers" {
			req.Topics = []kmsg.MetadataRequestTopic{}
		} else if resource != "" {
			req.Topics = []kmsg.MetadataRequestTopic{{Topic: &resource}}
		}
		r, e := req.RequestWith(ctx, cl)
		if e != nil {
			return s, e
		}
		if r.ErrorCode != 0 {
			return s, kerr.ErrorForCode(r.ErrorCode)
		}
		if len(r.Brokers) > f.policy.maxBrokers {
			return s, safeError("broker_limit")
		}
		for _, b := range r.Brokers {
			if _, e := f.policy.resolve(ctx, net.JoinHostPort(b.Host, strconv.Itoa(int(b.Port)))); e != nil {
				return s, advertisedError(e)
			}
			s.Brokers = append(s.Brokers, kafkaops.Broker{ID: b.NodeID, Host: b.Host, Port: b.Port})
		}
		for _, t := range r.Topics {
			if t.ErrorCode != 0 {
				return s, kerr.ErrorForCode(t.ErrorCode)
			}
			if t.Topic == nil {
				continue
			}
			topic := kafkaops.Topic{Name: *t.Topic, Internal: t.IsInternal, Partitions: []kafkaops.Partition{}}
			if t.TopicID != ([16]byte{}) {
				topic.TopicID = hex.EncodeToString(t.TopicID[:])
			}
			known := len(t.Partitions) > 0
			min, max := 0, 0
			for _, pt := range t.Partitions {
				part := kafkaops.Partition{Partition: pt.Partition, Leader: pt.Leader, Replicas: pt.Replicas, ISR: pt.ISR}
				if pt.ErrorCode != 0 {
					part.ErrorCode = operationCode(kerr.ErrorForCode(pt.ErrorCode))
					known = false
				}
				n := len(pt.Replicas)
				if n == 0 {
					known = false
				}
				if min == 0 || n < min {
					min = n
				}
				if n > max {
					max = n
				}
				topic.Partitions = append(topic.Partitions, part)
			}
			if known {
				topic.RFMin = &min
				topic.RFMax = &max
			}
			s.Topics = append(s.Topics, topic)
		}
	case "groups":
		if resource != "" {
			req := kmsg.NewPtrDescribeGroupsRequest()
			req.Groups = []string{resource}
			response, e := req.RequestWith(ctx, cl)
			if e != nil {
				return s, e
			}
			if len(response.Groups) != 1 || response.Groups[0].Group != resource {
				return s, safeError("invalid_response")
			}
			if response.Groups[0].ErrorCode != 0 {
				return s, kerr.ErrorForCode(response.Groups[0].ErrorCode)
			}
			s.Groups = append(s.Groups, groupEvidence(response.Groups[0]))
			break
		}
		seen := map[string]bool{}
		for _, sh := range cl.RequestSharded(ctx, kmsg.NewPtrListGroupsRequest()) {
			if sh.Err != nil {
				return s, sh.Err
			}
			r := sh.Resp.(*kmsg.ListGroupsResponse)
			if r.ErrorCode != 0 {
				return s, kerr.ErrorForCode(r.ErrorCode)
			}
			for _, g := range r.Groups {
				if !seen[g.Group] && (resource == "" || g.Group == resource) {
					seen[g.Group] = true
					s.Groups = append(s.Groups, kafkaops.Group{ID: g.Group, State: g.GroupState, ProtocolType: g.ProtocolType})
				}
			}
		}
		sort.Slice(s.Groups, func(i, j int) bool { return s.Groups[i].ID < s.Groups[j].ID })
	case "offsets":
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = resource
		req.Groups = []kmsg.OffsetFetchRequestGroup{{Group: resource}}
		r, e := req.RequestWith(ctx, cl)
		if e != nil {
			return s, e
		}
		if r.ErrorCode != 0 {
			return s, kerr.ErrorForCode(r.ErrorCode)
		}
		add := func(topic string, partition int32, offset int64, code int16) {
			o := kafkaops.Offset{Group: resource, Topic: topic, Partition: partition}
			if code != 0 {
				o.ErrorCode = operationCode(kerr.ErrorForCode(code))
			} else if offset >= 0 {
				v := offset
				o.Committed = &v
			}
			s.Offsets = append(s.Offsets, o)
		}
		if len(r.Groups) == 0 {
			for _, t := range r.Topics {
				for _, pt := range t.Partitions {
					add(t.Topic, pt.Partition, pt.Offset, pt.ErrorCode)
				}
			}
		}
		for _, g := range r.Groups {
			if g.ErrorCode != 0 {
				return s, kerr.ErrorForCode(g.ErrorCode)
			}
			for _, t := range g.Topics {
				for _, pt := range t.Partitions {
					add(t.Topic, pt.Partition, pt.Offset, pt.ErrorCode)
				}
			}
		}
		ends := kmsg.NewPtrListOffsetsRequest()
		topicIndexes := map[string]int{}
		for _, o := range s.Offsets {
			index, exists := topicIndexes[o.Topic]
			if !exists {
				index = len(ends.Topics)
				topicIndexes[o.Topic] = index
				t := kmsg.NewListOffsetsRequestTopic()
				t.Topic = o.Topic
				ends.Topics = append(ends.Topics, t)
			}
			pt := kmsg.NewListOffsetsRequestTopicPartition()
			pt.Partition = o.Partition
			pt.Timestamp = -1
			ends.Topics[index].Partitions = append(ends.Topics[index].Partitions, pt)
		}
		if len(ends.Topics) > 0 {
			for _, sh := range cl.RequestSharded(ctx, ends) {
				if sh.Err != nil {
					return s, sh.Err
				}
				r := sh.Resp.(*kmsg.ListOffsetsResponse)
				for _, t := range r.Topics {
					for _, pt := range t.Partitions {
						for i := range s.Offsets {
							o := &s.Offsets[i]
							if o.Topic != t.Topic || o.Partition != pt.Partition {
								continue
							}
							if pt.ErrorCode != 0 {
								o.ErrorCode = operationCode(kerr.ErrorForCode(pt.ErrorCode))
								continue
							}
							if pt.Offset >= 0 {
								v := pt.Offset
								o.LogEnd = &v
								if o.Committed != nil && v >= *o.Committed {
									lag := v - *o.Committed
									o.Lag = &lag
								}
							}
						}
					}
				}
			}
		}
	}
	return s, nil
}
func operationCode(err error) string {
	if errors.Is(err, kerr.UnknownTopicOrPartition) {
		return "NOT_FOUND"
	}
	if err != nil && err.Error() == "request key is unknown" {
		return "UNSUPPORTED_API"
	}
	code := CodeOf(classify(err))
	switch code {
	case "invalid_configuration":
		return "INVALID_ARGUMENT"
	case "tls_failed":
		return "TLS_HANDSHAKE_FAILED"
	}
	return strings.ToUpper(code)
}
