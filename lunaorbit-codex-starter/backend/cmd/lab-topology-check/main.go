// lab-topology-check writes unique synthetic records only after explicit --lab.
// It is intended to run inside the isolated console web container.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const sourceSeed = "kafka-source.lunaorbit-integration.svc.cluster.local:9092"
const targetSeed = "kafka-target.lunaorbit-integration.svc.cluster.local:9092"

type input struct {
	RemoteHost string `json:"remote_host"`
	SameTopic  bool   `json:"same_topic,omitempty"`
}

var invalid = errors.New("INVALID_LAB_INPUT")

func readInput(reader io.Reader, lab bool) (input, error) {
	var in input
	if !lab {
		return in, invalid
	}
	raw, err := io.ReadAll(io.LimitReader(reader, 2049))
	if err != nil || len(raw) > 2048 {
		return in, invalid
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&in) != nil || dec.Decode(new(any)) != io.EOF {
		return input{}, invalid
	}
	ip, err := netip.ParseAddr(in.RemoteHost)
	if err != nil || !ip.Is4() || !ip.IsPrivate() || ip.IsLoopback() || ip.IsMulticast() || ip.String() != in.RemoteHost {
		return input{}, invalid
	}
	return in, nil
}
func allowedEndpoint(in input, address string) bool {
	if address == sourceSeed || address == targetSeed || address == "kafka-source-extra.lunaorbit-integration.svc.cluster.local:9092" {
		return true
	}
	return address == net.JoinHostPort(in.RemoteHost, "19092") || address == net.JoinHostPort(in.RemoteHost, "29092")
}
func client(in input, seed string, extra ...kgo.Opt) (*kgo.Client, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(seed), kgo.ClientID("lunaorbit-lab-topology-check"), kgo.Dialer(func(ctx context.Context, network, address string) (net.Conn, error) {
		if !allowedEndpoint(in, address) {
			return nil, errors.New("LAB_ENDPOINT_DENIED")
		}
		return (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, network, address)
	}), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.RequestTimeoutOverhead(3 * time.Second), kgo.RetryTimeout(5 * time.Second), kgo.ProduceRequestTimeout(5 * time.Second), kgo.RecordDeliveryTimeout(10 * time.Second), kgo.FetchMaxBytes(1 << 20), kgo.FetchMaxPartitionBytes(1 << 20), kgo.FetchMaxWait(time.Second), kgo.MaxBufferedRecords(10), kgo.MaxBufferedBytes(1 << 20)}
	return kgo.NewClient(append(opts, extra...)...)
}
func clusterID(ctx context.Context, cl *kgo.Client) (string, error) {
	req := kmsg.NewPtrMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || resp.ClusterID == nil || *resp.ClusterID == "" || len(resp.Brokers) == 0 {
		return "", errors.New("METADATA_UNAVAILABLE")
	}
	return *resp.ClusterID, nil
}
func ensureTopic(ctx context.Context, cl *kgo.Client, name string) error {
	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 5000
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = name
	topic.NumPartitions = 1
	topic.ReplicationFactor = 1
	req.Topics = []kmsg.CreateTopicsRequestTopic{topic}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Topics) != 1 || (resp.Topics[0].ErrorCode != 0 && resp.Topics[0].ErrorCode != 36) {
		return errors.New("LAB_TOPIC_UNAVAILABLE")
	}
	return nil
}
func readStart(earliest, latest int64) int64 {
	if latest-100 > earliest {
		return latest - 100
	}
	return earliest
}
func offset(ctx context.Context, cl *kgo.Client, topic string, timestamp int64) (int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	p := kmsg.NewListOffsetsRequestTopicPartition()
	p.Partition = 0
	p.Timestamp = timestamp
	req.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{p}}}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 || resp.Topics[0].Partitions[0].ErrorCode != 0 {
		return 0, errors.New("OFFSET_UNAVAILABLE")
	}
	value := resp.Topics[0].Partitions[0].Offset
	if value < 0 {
		return 0, errors.New("OFFSET_UNAVAILABLE")
	}
	return value, nil
}
func wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(500 * time.Millisecond):
		return nil
	}
}
func verify(ctx context.Context, in input, seed, topic string, marker []byte) error {
	query, err := client(in, seed)
	if err != nil {
		return err
	}
	defer query.Close()
	var start int64
	for {
		earliest, e := offset(ctx, query, topic, -2)
		latest, l := offset(ctx, query, topic, -1)
		if e == nil && l == nil {
			start = readStart(earliest, latest)
			break
		}
		if wait(ctx) != nil {
			return errors.New("REPLICATION_TIMEOUT")
		}
	}
	consumer, err := client(in, seed, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().At(start)}}))
	if err != nil {
		return err
	}
	defer consumer.Close()
	scanned := 0
	for ctx.Err() == nil {
		if scanned == 100 {
			if wait(ctx) != nil {
				break
			}
			earliest, e := offset(ctx, query, topic, -2)
			latest, l := offset(ctx, query, topic, -1)
			if e != nil || l != nil {
				continue
			}
			consumer.SetOffsets(map[string]map[int32]kgo.EpochOffset{topic: {0: {Epoch: -1, Offset: readStart(earliest, latest)}}})
			scanned = 0
		}
		fetches := consumer.PollRecords(ctx, 100-scanned)
		for _, failure := range fetches.Errors() {
			if failure.Err != nil {
				return errors.New("CONSUME_FAILED")
			}
		}
		records := fetches.Records()
		scanned += len(records)
		for _, record := range records {
			if bytes.Equal(record.Value, marker) {
				return nil
			}
		}
	}
	return errors.New("MARKER_NOT_FOUND")
}
func run(ctx context.Context, in input) error {
	seeds := []string{sourceSeed, targetSeed, net.JoinHostPort(in.RemoteHost, "19092"), net.JoinHostPort(in.RemoteHost, "29092")}
	clients := make([]*kgo.Client, 0, 4)
	defer func() {
		for _, cl := range clients {
			cl.Close()
		}
	}()
	ids := map[string]bool{}
	for _, seed := range seeds {
		cl, err := client(in, seed)
		if err != nil {
			return errors.New("CLIENT_FAILED")
		}
		clients = append(clients, cl)
		id, err := clusterID(ctx, cl)
		if err != nil || ids[id] {
			return errors.New("FOUR_DISTINCT_CLUSTERS_REQUIRED")
		}
		ids[id] = true
	}
	fmt.Println("PASS distinct_clusters count=4")
	if err := ensureTopic(ctx, clients[3], "lunaorbit-mesh-events"); err != nil {
		return err
	}
	count := 2
	if in.SameTopic {
		count = 3
	}
	markers := make([][]byte, count)
	for i := range markers {
		random := make([]byte, 24)
		if _, err := rand.Read(random); err != nil {
			return errors.New("MARKER_GENERATION_FAILED")
		}
		markers[i] = []byte("lunaorbit-lab-" + hex.EncodeToString(random))
	}
	for i, pair := range []struct {
		cl    *kgo.Client
		topic string
	}{{clients[0], "lunaorbit-lab-events"}, {clients[3], "lunaorbit-mesh-events"}} {
		if err := pair.cl.ProduceSync(ctx, &kgo.Record{Topic: pair.topic, Partition: 0, Value: markers[i]}).FirstErr(); err != nil {
			return errors.New("LAB_PRODUCE_FAILED")
		}
	}
	if in.SameTopic {
		if err := clients[1].ProduceSync(ctx, &kgo.Record{Topic: "lunaorbit-lab-events", Partition: 0, Value: markers[2]}).FirstErr(); err != nil {
			return errors.New("LAB_PRODUCE_B_FAILED")
		}
	}
	fmt.Printf("PASS produced_markers count=%d\n", len(markers))
	checks := []struct {
		seed, topic, label string
		marker             []byte
	}{{seeds[1], "source.lunaorbit-lab-events", "A_to_B", markers[0]}, {seeds[2], "source.lunaorbit-lab-events", "A_to_C", markers[0]}, {seeds[1], "upstream.lunaorbit-mesh-events", "D_to_B", markers[1]}, {seeds[2], "target.source.lunaorbit-lab-events", "A_to_B_to_C", markers[0]}, {seeds[2], "target.upstream.lunaorbit-mesh-events", "D_to_B_to_C", markers[1]}}
	if in.SameTopic {
		for i := range checks {
			if i == 2 || i == 4 {
				checks[i].topic = "lunaorbit-mesh-events"
			} else {
				checks[i].topic = "lunaorbit-lab-events"
			}
		}
	}
	if in.SameTopic {
		checks = append(checks, struct {
			seed, topic, label string
			marker             []byte
		}{seeds[2], "lunaorbit-lab-events", "B_to_C_unique", markers[2]})
	}
	verifyCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	results := make([]error, len(checks))
	var wg sync.WaitGroup
	for i, check := range checks {
		wg.Add(1)
		go func() { defer wg.Done(); results[i] = verify(verifyCtx, in, check.seed, check.topic, check.marker) }()
	}
	wg.Wait()
	passed := 0
	for i, err := range results {
		if err == nil {
			fmt.Printf("PASS %s count=1\n", checks[i].label)
			passed++
		}
	}
	if passed != len(checks) {
		return errors.New("REPLICATION_CHECK_FAILED")
	}
	fmt.Printf("PASS replication_paths count=%d\n", len(checks))
	return nil
}
func main() {
	lab := flag.Bool("lab", false, "explicitly allow synthetic lab marker writes")
	flag.Parse()
	in, err := readInput(os.Stdin, *lab)
	if err != nil || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "FAIL INVALID_LAB_INPUT")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if err = run(ctx, in); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL", err)
		os.Exit(1)
	}
}
