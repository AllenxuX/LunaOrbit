package kafkaadmin

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type seenKeys struct {
	mu   sync.Mutex
	keys map[int16]int
}

func (s *seenKeys) count(k kmsg.Key) int { s.mu.Lock(); defer s.mu.Unlock(); return s.keys[int16(k)] }

func recordingCluster(t *testing.T, opts ...kfake.Opt) (*kfake.Cluster, *seenKeys) {
	t.Helper()
	c, err := kfake.NewCluster(append([]kfake.Opt{kfake.NumBrokers(1), kfake.SeedTopics(1, "orders")}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	seen := &seenKeys{keys: map[int16]int{}}
	c.Control(func(req kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		seen.mu.Lock()
		seen.keys[req.Key()]++
		seen.mu.Unlock()
		return nil, nil, false
	})
	return c, seen
}

func plainDial(ctx context.Context, network, address string) (net.Conn, error) {
	return (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, network, address)
}

func TestReadOnlyClientReadsMetadataButNeverSendsWrites(t *testing.T) {
	c, seen := recordingCluster(t)
	cl, err := NewReadOnlyClient(c.ListenAddrs(), nil, nil, plainDial)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	meta := kmsg.NewPtrMetadataRequest()
	resp, err := meta.RequestWith(ctx, cl)
	if err != nil || len(resp.Topics) != 1 || *resp.Topics[0].Topic != "orders" {
		t.Fatalf("read path broken: %v", err)
	}
	if _, err := kmsg.NewPtrListGroupsRequest().RequestWith(ctx, cl); err != nil {
		t.Fatalf("list groups: %v", err)
	}
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("x")}).FirstErr(); err == nil {
		t.Fatal("produce must be refused")
	}
	create := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic, ct.NumPartitions, ct.ReplicationFactor = "new-topic", 1, 1
	create.Topics = append(create.Topics, ct)
	alter := kmsg.NewPtrAlterConfigsRequest()
	ar := kmsg.NewAlterConfigsRequestResource()
	ar.ResourceType, ar.ResourceName = kmsg.ConfigResourceTypeTopic, "orders"
	alter.Resources = append(alter.Resources, ar)
	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Group = "g1"
	del := kmsg.NewPtrDeleteTopicsRequest()
	del.TopicNames = []string{"orders"}
	for _, req := range []kmsg.Request{create, alter, commit, del} {
		if _, err := cl.Request(ctx, req); err == nil {
			t.Fatalf("%s must be refused", kmsg.Key(req.Key()).Name())
		}
	}
	for _, k := range []kmsg.Key{kmsg.Produce, kmsg.CreateTopics, kmsg.AlterConfigs, kmsg.IncrementalAlterConfigs, kmsg.OffsetCommit, kmsg.DeleteTopics, kmsg.InitProducerID} {
		if n := seen.count(k); n != 0 {
			t.Fatalf("broker received %s %d times", k.Name(), n)
		}
	}
	if seen.count(kmsg.Metadata) == 0 {
		t.Fatal("expected metadata traffic")
	}
}

func TestReadOnlyClientAuthenticatesWithSCRAM(t *testing.T) {
	c, seen := recordingCluster(t, kfake.EnableSASL(), kfake.Superuser("SCRAM-SHA-256", "reader", "synthetic-pw"))
	cl, err := NewReadOnlyClient(c.ListenAddrs(), scramMechanism("reader", "synthetic-pw"), nil, plainDial)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl); err != nil {
		t.Fatalf("scram read: %v", err)
	}
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("x")}).FirstErr(); err == nil || seen.count(kmsg.Produce) != 0 {
		t.Fatal("produce must be refused before the network")
	}
}

func TestAllowlistIsExactlyTheApprovedReadSet(t *testing.T) {
	want := []kmsg.Key{kmsg.Metadata, kmsg.ApiVersions, kmsg.DescribeCluster, kmsg.DescribeConfigs, kmsg.ListOffsets, kmsg.OffsetFetch, kmsg.DescribeGroups, kmsg.ListGroups, kmsg.FindCoordinator, kmsg.DescribeLogDirs, kmsg.SASLHandshake, kmsg.SASLAuthenticate}
	if len(readOnlyKeys) != len(want) {
		t.Fatalf("allowlist size %d", len(readOnlyKeys))
	}
	for _, k := range want {
		if !readOnlyKeys[int16(k)] {
			t.Fatalf("%s missing", k.Name())
		}
	}
}
