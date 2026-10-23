package kafkaadmin

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/twmb/franz-go/pkg/kmsg"
	"io"
	"lunaorbit/backend/internal/kafkaops"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
)

func managementBroker(t *testing.T, code int16) (*Factory, Profile, *atomic.Int32) {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { l.Close() })
	host, ps, _ := net.SplitHostPort(l.Addr().String())
	port, _ := strconv.Atoi(ps)
	count := new(atomic.Int32)
	go func() {
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			go func() {
				defer c.Close()
				for {
					var sz [4]byte
					if _, e := io.ReadFull(c, sz[:]); e != nil {
						return
					}
					b := make([]byte, binary.BigEndian.Uint32(sz[:]))
					if _, e := io.ReadFull(c, b); e != nil {
						return
					}
					key := int16(binary.BigEndian.Uint16(b))
					version := int16(binary.BigEndian.Uint16(b[2:]))
					var r kmsg.Response
					switch key {
					case 18:
						v := kmsg.NewPtrApiVersionsResponse()
						v.ApiKeys = []kmsg.ApiVersionsResponseApiKey{{ApiKey: 18, MaxVersion: 3}, {ApiKey: 3, MaxVersion: 9}, {ApiKey: 19, MaxVersion: 3}, {ApiKey: 32, MaxVersion: 2}, {ApiKey: 37, MaxVersion: 1}, {ApiKey: 44, MaxVersion: 0}, {ApiKey: 29, MaxVersion: 1}, {ApiKey: 31, MaxVersion: 1}}
						r = v
					case 3:
						v := kmsg.NewPtrMetadataResponse()
						v.ControllerID = 1
						v.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 1, Host: host, Port: int32(port)}}
						r = v
					case 19:
						count.Add(1)
						v := kmsg.NewPtrCreateTopicsResponse()
						v.Topics = []kmsg.CreateTopicsResponseTopic{{Topic: "synthetic", ErrorCode: code}}
						r = v
					case 37:
						count.Add(1)
						v := kmsg.NewPtrCreatePartitionsResponse()
						v.Topics = []kmsg.CreatePartitionsResponseTopic{{Topic: "synthetic", ErrorCode: code}}
						r = v
					case 44:
						q := kmsg.NewPtrIncrementalAlterConfigsRequest()
						q.SetVersion(version)
						if e := q.ReadFrom(b[10+int(binary.BigEndian.Uint16(b[8:])):]); e != nil || len(q.Resources) != 1 || len(q.Resources[0].Configs) != 1 || q.Resources[0].Configs[0].Name != "retention.ms" || q.Resources[0].Configs[0].Op != 0 {
							t.Error("unsafe retention mutation", q, e)
						}
						count.Add(1)
						v := kmsg.NewPtrIncrementalAlterConfigsResponse()
						v.Resources = []kmsg.IncrementalAlterConfigsResponseResource{{ResourceName: "synthetic", ResourceType: 2, ErrorCode: code}}
						r = v
					case 32:
						v := kmsg.NewPtrDescribeConfigsResponse()
						value := "123"
						v.Resources = []kmsg.DescribeConfigsResponseResource{{ResourceType: 2, ResourceName: "synthetic", ErrorCode: code, Configs: []kmsg.DescribeConfigsResponseResourceConfig{{Name: "retention.ms", Value: &value}, {Name: "secret", Value: &value}, {Name: "cleanup.policy", Value: &value, IsSensitive: true}}}}
						r = v
					case 29:
						v := kmsg.NewPtrDescribeACLsResponse()
						if code == 54 {
							v.ErrorCode = 54
						}
						r = v
					case 31:
						q := kmsg.NewPtrDeleteACLsRequest()
						q.SetVersion(version)
						if e := q.ReadFrom(b[10+int(binary.BigEndian.Uint16(b[8:])):]); e != nil || len(q.Filters) != 1 {
							t.Error("invalid deletion", q, e)
							return
						}
						filter := q.Filters[0]
						if filter.ResourceType != 2 || filter.ResourceName == nil || *filter.ResourceName != "synthetic" || filter.ResourcePatternType != 3 || filter.Principal == nil || *filter.Principal != "User:synthetic" || filter.Host == nil || *filter.Host != "*" || filter.Operation != 3 || filter.PermissionType != 3 {
							t.Error("non-exact deletion", q)
						}
						count.Add(1)
						v := kmsg.NewPtrDeleteACLsResponse()
						v.Results = []kmsg.DeleteACLsResponseResult{{MatchingACLs: []kmsg.DeleteACLsResponseResultMatchingACL{{ErrorCode: code, ResourceType: 2, ResourceName: "synthetic", ResourcePatternType: 3, Principal: "User:synthetic", Host: "*", Operation: 3, PermissionType: 3}}}}
						r = v
					default:
						return
					}
					r.SetVersion(version)
					out := append([]byte{}, b[4:8]...)
					if r.IsFlexible() && key != 18 {
						out = append(out, 0)
					}
					out = r.AppendTo(out)
					binary.BigEndian.PutUint32(sz[:], uint32(len(out)))
					if _, e := c.Write(append(sz[:], out...)); e != nil {
						return
					}
				}
			}()
		}
	}()
	policy, _ := NewNetworkPolicy(NetworkPolicyConfig{AllowedCIDRs: []string{"127.0.0.0/8"}, AllowedPorts: []int{port}})
	f, _ := NewFactory(policy, nil)
	return f, Profile{BootstrapServers: []string{l.Addr().String()}, SecurityProtocol: "PLAINTEXT"}, count
}
func TestManagementSocketResults(t *testing.T) {
	for _, code := range []int16{0, 29, 35} {
		t.Run(strconv.Itoa(int(code)), func(t *testing.T) {
			f, p, count := managementBroker(t, code)
			ctx := context.Background()
			for _, run := range []func() error{func() error { return f.CreateTopic(ctx, p, "synthetic", 1, 1, nil) }, func() error { return f.ExpandTopic(ctx, p, "synthetic", 2) }, func() error { return f.AlterTopicRetention(ctx, p, "synthetic", 1000) }} {
				e := run()
				if (e == nil) != (code == 0) {
					t.Fatal(code, e)
				}
			}
			if count.Load() != 3 {
				t.Fatal("mutation retry", count.Load())
			}
			configs, e := f.TopicConfigs(ctx, p, "synthetic")
			if code == 0 && (e != nil || len(configs) != 1 || configs["retention.ms"] != "123") {
				t.Fatal(configs, e)
			}
			if code != 0 && e == nil {
				t.Fatal("config resource failure ignored")
			}
		})
	}
}
func TestDeleteACLSocketPerItemFailure(t *testing.T) {
	for _, code := range []int16{0, 29, 54} {
		f, p, count := managementBroker(t, code)
		e := f.DeleteACL(context.Background(), p, kafkaops.ACLRule{ResourceType: "TOPIC", ResourceName: "synthetic", PatternType: "LITERAL", Principal: "User:synthetic", Host: "*", Operation: "READ", Permission: "ALLOW"})
		if (e == nil) != (code == 0) {
			t.Fatal(code, e)
		}
		if code == 54 && count.Load() != 0 {
			t.Fatal("wrote with disabled authorizer")
		}
		if code != 54 && count.Load() != 1 {
			t.Fatal("delete retries", count.Load())
		}
	}
}
func TestManagementMissingTopicSafeError(t *testing.T) {
	f, p, _ := managementBroker(t, 3)
	_, err := f.TopicConfigs(context.Background(), p, "synthetic")
	var safe *kafkaops.Error
	if !errors.As(err, &safe) || safe.Code != "NOT_FOUND" {
		t.Fatal(err)
	}
}
