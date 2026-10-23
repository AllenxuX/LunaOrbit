package kafkaadmin

import (
	"context"
	"encoding/binary"
	"github.com/twmb/franz-go/pkg/kmsg"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// Local protocol fixture implements only ApiVersions and empty-topic Metadata.
func mockBroker(t *testing.T, advertised string) (string, func()) {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	host, ps, _ := net.SplitHostPort(l.Addr().String())
	port, _ := strconv.Atoi(ps)
	if advertised != "" {
		host = advertised
	}
	go func() {
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			go func() {
				defer c.Close()
				for {
					var size [4]byte
					if _, e := io.ReadFull(c, size[:]); e != nil {
						return
					}
					b := make([]byte, binary.BigEndian.Uint32(size[:]))
					if _, e := io.ReadFull(c, b); e != nil {
						return
					}
					key := int16(binary.BigEndian.Uint16(b))
					version := int16(binary.BigEndian.Uint16(b[2:]))
					var response kmsg.Response
					switch key {
					case 18:
						r := kmsg.NewPtrApiVersionsResponse()
						r.SetVersion(version)
						r.ApiKeys = []kmsg.ApiVersionsResponseApiKey{{ApiKey: 18, MinVersion: 0, MaxVersion: 3}, {ApiKey: 3, MinVersion: 0, MaxVersion: 9}}
						response = r
					case 3:
						r := kmsg.NewPtrMetadataResponse()
						r.SetVersion(version)
						id := "synthetic-cluster"
						r.ClusterID = &id
						r.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 1, Host: host, Port: int32(port)}}
						response = r
					default:
						return
					}
					out := append([]byte{}, b[4:8]...)
					if response.IsFlexible() && key != 18 {
						out = append(out, 0)
					}
					out = response.AppendTo(out)
					binary.BigEndian.PutUint32(size[:], uint32(len(out)))
					if _, e = c.Write(append(size[:], out...)); e != nil {
						return
					}
				}
			}()
		}
	}()
	return l.Addr().String(), func() { l.Close() }
}
func TestLocalKafkaMetadataAndAdvertisedPolicy(t *testing.T) {
	for _, advertised := range []string{"", "10.0.0.2", "127.0.0.2"} {
		t.Run(advertised, func(t *testing.T) {
			address, close := mockBroker(t, advertised)
			defer close()
			_, port, _ := net.SplitHostPort(address)
			n, _ := strconv.Atoi(port)
			p, _ := NewNetworkPolicy(NetworkPolicyConfig{AllowedCIDRs: []string{"127.0.0.0/8"}, AllowedPorts: []int{n}})
			f, _ := NewFactory(p, nil)
			r, e := f.Probe(context.Background(), Profile{BootstrapServers: []string{address}, SecurityProtocol: "PLAINTEXT"})
			if advertised != "" {
				if CodeOf(e) != "advertised_unreachable" {
					t.Fatal(e)
				}
				return
			}
			if e != nil || r.ClusterID != "synthetic-cluster" || len(r.Brokers) != 1 {
				t.Fatal(r, e)
			}
		})
	}
}
func TestRebindingRejectedBeforeSecondConnection(t *testing.T) {
	address, close := mockBroker(t, "")
	defer close()
	_, port, _ := net.SplitHostPort(address)
	n, _ := strconv.Atoi(port)
	calls := 0
	p, _ := NewNetworkPolicy(NetworkPolicyConfig{AllowedCIDRs: []string{"127.0.0.0/8"}, AllowedPorts: []int{n}, Resolver: resolverFunc(func(context.Context, string) ([]net.IPAddr, error) {
		calls++
		ip := "127.0.0.1"
		if calls > 1 {
			ip = "10.0.0.1"
		}
		return []net.IPAddr{{IP: net.ParseIP(ip)}}, nil
	})})
	f, _ := NewFactory(p, nil)
	_, e := f.Probe(context.Background(), Profile{BootstrapServers: []string{net.JoinHostPort("broker.test", port)}, SecurityProtocol: "PLAINTEXT"})
	if CodeOf(e) != "network_denied" {
		t.Fatal(e)
	}
}
func TestUntrustedTLSFailsWithoutLeakingTarget(t *testing.T) {
	s := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer s.Close()
	address := s.Listener.Addr().String()
	_, port, _ := net.SplitHostPort(address)
	n, _ := strconv.Atoi(port)
	p, _ := NewNetworkPolicy(NetworkPolicyConfig{AllowedCIDRs: []string{"127.0.0.0/8"}, AllowedPorts: []int{n}})
	f, _ := NewFactory(p, nil)
	_, e := f.Probe(context.Background(), Profile{SecurityProtocol: "SSL", BootstrapServers: []string{address}})
	if CodeOf(e) != "tls_failed" || e.Error() != "tls_failed" {
		t.Fatal(e)
	}
}
