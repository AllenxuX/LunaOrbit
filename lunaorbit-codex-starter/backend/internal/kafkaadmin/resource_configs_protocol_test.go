package kafkaadmin

import (
	"context"
	"encoding/binary"
	"github.com/twmb/franz-go/pkg/kmsg"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Two independently listening brokers expose different values. This catches
// accidental cluster-wide sharding / response merging for broker config reads.
func TestConfigBrokerRoutingAndPreflightOverProtocol(t *testing.T) {
	listeners := make([]net.Listener, 2)
	ports := make([]int, 2)
	for i := range listeners {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		listeners[i] = l
		t.Cleanup(func() { l.Close() })
		_, p, _ := net.SplitHostPort(l.Addr().String())
		ports[i], _ = strconv.Atoi(p)
	}
	var mu sync.Mutex
	writes := []bool{}
	for index, l := range listeners {
		go func(index int, l net.Listener) {
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
						var resp kmsg.Response
						body := b[10+int(binary.BigEndian.Uint16(b[8:])):]
						switch key {
						case 18:
							r := kmsg.NewPtrApiVersionsResponse()
							r.ApiKeys = []kmsg.ApiVersionsResponseApiKey{{ApiKey: 18, MaxVersion: 3}, {ApiKey: 3, MaxVersion: 9}, {ApiKey: 32, MaxVersion: 2}, {ApiKey: 44, MaxVersion: 0}}
							resp = r
						case 3:
							r := kmsg.NewPtrMetadataResponse()
							r.ControllerID = 1
							for i, p := range ports {
								r.Brokers = append(r.Brokers, kmsg.MetadataResponseBroker{NodeID: int32(i + 1), Host: "127.0.0.1", Port: int32(p)})
							}
							resp = r
						case 32:
							q := kmsg.NewPtrDescribeConfigsRequest()
							q.SetVersion(version)
							if e := q.ReadFrom(body); e != nil {
								t.Error(e)
								return
							}
							if !q.IncludeSynonyms || q.Resources[0].ConfigNames != nil {
								t.Error("partial config metadata")
							}
							r := kmsg.NewPtrDescribeConfigsResponse()
							v := strconv.Itoa(index + 1)
							r.Resources = []kmsg.DescribeConfigsResponseResource{{ResourceType: 4, ResourceName: q.Resources[0].ResourceName, Configs: []kmsg.DescribeConfigsResponseResourceConfig{{Name: "log.retention.ms", Value: &v, Source: 2}}}}
							resp = r
						case 44:
							// A legitimate broker may need more than one second to
							// validate a dynamic configuration in a multi-node cluster.
							time.Sleep(1200 * time.Millisecond)
							q := kmsg.NewPtrIncrementalAlterConfigsRequest()
							q.SetVersion(version)
							if e := q.ReadFrom(body); e != nil {
								t.Error(e)
								return
							}
							if index != 1 || q.Resources[0].ResourceName != "2" || q.Resources[0].ResourceType != 4 {
								t.Error("cross broker mutation")
							}
							mu.Lock()
							writes = append(writes, q.ValidateOnly)
							mu.Unlock()
							r := kmsg.NewPtrIncrementalAlterConfigsResponse()
							r.Resources = []kmsg.IncrementalAlterConfigsResponseResource{{ResourceType: 4, ResourceName: "2"}}
							resp = r
						default:
							return
						}
						resp.SetVersion(version)
						out := append([]byte{}, b[4:8]...)
						if resp.IsFlexible() && key != 18 {
							out = append(out, 0)
						}
						out = resp.AppendTo(out)
						binary.BigEndian.PutUint32(sz[:], uint32(len(out)))
						if _, e := c.Write(append(sz[:], out...)); e != nil {
							return
						}
					}
				}()
			}
		}(index, l)
	}
	policy, _ := NewNetworkPolicy(NetworkPolicyConfig{AllowedCIDRs: []string{"127.0.0.0/8"}, AllowedPorts: ports})
	f, _ := NewFactory(policy, nil)
	p := Profile{BootstrapServers: []string{listeners[0].Addr().String()}, SecurityProtocol: "PLAINTEXT"}
	snapshot, e := f.DescribeResourceConfigs(context.Background(), p, ConfigResource{"BROKER", "2"})
	if e != nil || len(snapshot.Entries) != 1 || snapshot.Entries[0].Value == nil || *snapshot.Entries[0].Value != "2" {
		t.Fatal(snapshot, e)
	}
	v := "1000"
	if e = f.AlterResourceConfigs(context.Background(), p, ConfigResource{"BROKER", "2"}, []ConfigChange{{Name: "log.retention.ms", Operation: "SET", Value: &v}}); e != nil {
		t.Fatal(e)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(writes) != 2 || !writes[0] || writes[1] {
		t.Fatal("preflight must precede exactly one mutation", writes)
	}
}
