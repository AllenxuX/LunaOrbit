package kafkaadmin

import (
	"encoding/binary"
	"github.com/twmb/franz-go/pkg/kmsg"
	"io"
	"net"
	"strconv"
	"testing"
)

func mockACLBroker(t *testing.T, advertised string) (string, func()) {
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
						r.ApiKeys = []kmsg.ApiVersionsResponseApiKey{{ApiKey: 18, MinVersion: 0, MaxVersion: 3}, {ApiKey: 29, MinVersion: 0, MaxVersion: 2}, {ApiKey: 3, MinVersion: 0, MaxVersion: 9}, {ApiKey: 16, MinVersion: 0, MaxVersion: 4}, {ApiKey: 10, MinVersion: 0, MaxVersion: 3}, {ApiKey: 9, MinVersion: 2, MaxVersion: 7}, {ApiKey: 2, MinVersion: 1, MaxVersion: 5}}
						response = r
					case 3:
						r := kmsg.NewPtrMetadataResponse()
						r.SetVersion(version)
						id := "synthetic-cluster"
						r.ClusterID = &id
						r.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 1, Host: host, Port: int32(port)}}
						name := "synthetic-topic"
						r.Topics = []kmsg.MetadataResponseTopic{{Topic: &name, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}, {Partition: 1, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}, {Partition: 2, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}}}}
						response = r
					case 29:
						r := kmsg.NewPtrDescribeACLsResponse()
						r.SetVersion(version)
						r.ErrorCode = 54
						response = r
					case 16:
						r := kmsg.NewPtrListGroupsResponse()
						r.SetVersion(version)
						r.Groups = []kmsg.ListGroupsResponseGroup{{Group: "synthetic-group", ProtocolType: "consumer", GroupState: "Stable"}}
						response = r
					case 10:
						r := kmsg.NewPtrFindCoordinatorResponse()
						r.SetVersion(version)
						r.NodeID = 1
						r.Host = host
						r.Port = int32(port)
						response = r
					case 9:
						r := kmsg.NewPtrOffsetFetchResponse()
						r.SetVersion(version)
						r.Topics = []kmsg.OffsetFetchResponseTopic{{Topic: "synthetic-topic", Partitions: []kmsg.OffsetFetchResponseTopicPartition{{Partition: 0, Offset: 10}, {Partition: 1, Offset: -1}, {Partition: 2, Offset: 30}}}}
						response = r
					case 2:
						r := kmsg.NewPtrListOffsetsResponse()
						r.SetVersion(version)
						r.Topics = []kmsg.ListOffsetsResponseTopic{{Topic: "synthetic-topic", Partitions: []kmsg.ListOffsetsResponseTopicPartition{{Partition: 0, Offset: 20}, {Partition: 1, Offset: 20}, {Partition: 2, Offset: 20}}}}
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
