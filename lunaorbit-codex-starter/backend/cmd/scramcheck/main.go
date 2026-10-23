// scramcheck performs opt-in checks on the named synthetic Kafka lab only.
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
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type input struct {
	BootstrapServers []string `json:"bootstrap_servers"`
	Username         string   `json:"username"`
	Password         string   `json:"password"`
	Topic            string   `json:"topic"`
	DeniedTopic      string   `json:"denied_topic"`
	Group            string   `json:"group"`
	DeniedGroup      string   `json:"denied_group"`
}

var invalid = errors.New("INVALID_INPUT")
var resourceName = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)

func allowedEndpoint(address string) bool {
	host, port, err := net.SplitHostPort(address)
	return err == nil && port == "9094" && (host == "kafka-source.lunaorbit-integration.svc.cluster.local" || host == "kafka-target.lunaorbit-integration.svc.cluster.local")
}
func readInput(reader io.Reader, lab bool) (input, error) {
	var in input
	if !lab {
		return in, invalid
	}
	raw, err := io.ReadAll(io.LimitReader(reader, 16385))
	if err != nil || len(raw) > 16384 {
		return in, invalid
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&in) != nil {
		return input{}, invalid
	}
	var trailing any
	if dec.Decode(&trailing) != io.EOF {
		return input{}, invalid
	}
	if len(in.BootstrapServers) < 1 || len(in.BootstrapServers) > 2 || len(in.Username) < 1 || len(in.Username) > 128 || len(in.Password) < 1 || len(in.Password) > 4096 || strings.ContainsRune(in.Password, 0) {
		return input{}, invalid
	}
	if !resourceName.MatchString(in.Username) {
		return input{}, invalid
	}
	for _, address := range in.BootstrapServers {
		if !allowedEndpoint(address) {
			return input{}, invalid
		}
	}
	for _, name := range []string{in.Topic, in.DeniedTopic, in.Group, in.DeniedGroup} {
		if !resourceName.MatchString(name) || name == "." || name == ".." {
			return input{}, invalid
		}
	}
	if in.Topic == in.DeniedTopic || in.Group == in.DeniedGroup {
		return input{}, invalid
	}
	return in, nil
}
func client(in input, password string, extra ...kgo.Opt) (*kgo.Client, error) {
	options := []kgo.Opt{kgo.SeedBrokers(in.BootstrapServers...), kgo.SASL(scram.Auth{User: in.Username, Pass: password}.AsSha256Mechanism()),
		kgo.Dialer(func(ctx context.Context, network, address string) (net.Conn, error) {
			if !allowedEndpoint(address) {
				return nil, invalid
			}
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, address)
		}), kgo.DialTimeout(5 * time.Second), kgo.RequestRetries(0), kgo.RecordRetries(0), kgo.DisableIdempotentWrite(), kgo.RecordDeliveryTimeout(10 * time.Second),
		kgo.RequestTimeoutOverhead(time.Second), kgo.RetryTimeout(10 * time.Second), kgo.ProducerBatchMaxBytes(16384)}
	return kgo.NewClient(append(options, extra...)...)
}
func apiVersions(cl *kgo.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	r, err := kmsg.NewPtrApiVersionsRequest().RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	return kerr.ErrorForCode(r.ErrorCode)
}
func groupCheck(cl *kgo.Client, group string, denied bool) (bool, string) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	req := kmsg.NewPtrDescribeGroupsRequest()
	req.Groups = []string{group}
	r, err := req.RequestWith(ctx, cl)
	return groupResult(group, denied, r, err)
}

// Coordinator discovery can return authorization failure before DescribeGroups
// is sent. Both layers must report the exact GROUP_AUTHORIZATION_FAILED code.
func groupResult(group string, denied bool, r *kmsg.DescribeGroupsResponse, err error) (bool, string) {
	if err != nil {
		if denied && errors.Is(err, kerr.GroupAuthorizationFailed) {
			return true, ""
		}
		var kafkaError *kerr.Error
		if errors.As(err, &kafkaError) {
			return false, "GROUP_REQUEST_CODE_" + strconv.Itoa(int(kafkaError.Code))
		}
		return false, "GROUP_REQUEST_ERROR"
	}
	if r == nil || len(r.Groups) != 1 || r.Groups[0].Group != group {
		return false, "GROUP_RESPONSE_MALFORMED"
	}
	code := r.Groups[0].ErrorCode
	if denied && code == kerr.GroupAuthorizationFailed.Code {
		return true, ""
	}
	if !denied && (code == 0 || code == kerr.GroupIDNotFound.Code) {
		return true, ""
	}
	return false, "GROUP_RESPONSE_CODE_" + strconv.Itoa(int(code))
}

type groupConsumer interface {
	PollFetches(context.Context) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
}

func consumeAndCommit(ctx context.Context, consumer groupConsumer, topic string, marker []byte, output io.Writer) string {
	var found *kgo.Record
	for found == nil && ctx.Err() == nil {
		fetches := consumer.PollFetches(ctx)
		if len(fetches.Errors()) > 0 {
			return "GROUP_CONSUME"
		}
		fetches.EachRecord(func(record *kgo.Record) {
			if record.Topic == topic && bytes.Equal(record.Value, marker) {
				found = record
			}
		})
	}
	if found == nil {
		return "GROUP_CONSUME"
	}
	fmt.Fprintln(output, "PASS GROUP_CONSUME")
	if consumer.CommitRecords(ctx, found) != nil {
		return "GROUP_COMMIT"
	}
	fmt.Fprintln(output, "PASS GROUP_COMMIT")
	return ""
}
func run(in input, output io.Writer) string {
	cl, err := client(in, in.Password)
	if err != nil {
		return "CLIENT"
	}
	defer cl.Close()
	if apiVersions(cl) != nil {
		return "AUTH_GOOD"
	}
	fmt.Fprintln(output, "PASS AUTH_GOOD")
	wrong, err := client(in, in.Password+"-wrong-synthetic")
	if err != nil {
		return "AUTH_BAD"
	}
	err = apiVersions(wrong)
	wrong.Close()
	if !errors.Is(err, kerr.SaslAuthenticationFailed) {
		return "AUTH_BAD"
	}
	fmt.Fprintln(output, "PASS AUTH_BAD")
	marker := make([]byte, 24)
	if _, err = rand.Read(marker); err != nil {
		return "MARKER"
	}
	value := []byte("lunaorbit-scram-check-" + hex.EncodeToString(marker))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	record, err := cl.ProduceSync(ctx, &kgo.Record{Topic: in.Topic, Value: value}).First()
	cancel()
	if err != nil {
		return "TOPIC_PRODUCE"
	}
	fmt.Fprintln(output, "PASS TOPIC_PRODUCE")
	cl.AddConsumePartitions(map[string]map[int32]kgo.Offset{in.Topic: {record.Partition: kgo.NewOffset().At(record.Offset)}})
	ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
	found := false
	for !found && ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		if len(fetches.Errors()) > 0 {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if r.Topic == in.Topic && r.Partition == record.Partition && r.Offset == record.Offset && bytes.Equal(r.Value, value) {
				found = true
			}
		})
	}
	cancel()
	cl.PauseFetchPartitions(map[string][]int32{in.Topic: {record.Partition}})
	if !found {
		return "TOPIC_FETCH"
	}
	fmt.Fprintln(output, "PASS TOPIC_FETCH")
	ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
	_, err = cl.ProduceSync(ctx, &kgo.Record{Topic: in.DeniedTopic, Value: value}).First()
	cancel()
	if !errors.Is(err, kerr.TopicAuthorizationFailed) {
		return "TOPIC_DENIED"
	}
	fmt.Fprintln(output, "PASS TOPIC_DENIED")
	if ok, diagnostic := groupCheck(cl, in.Group, false); !ok {
		return "GROUP_ALLOW_" + diagnostic
	}
	fmt.Fprintln(output, "PASS GROUP_ALLOW")
	if ok, diagnostic := groupCheck(cl, in.DeniedGroup, true); !ok {
		return "GROUP_DENIED_" + diagnostic
	}
	fmt.Fprintln(output, "PASS GROUP_DENIED")
	// Release the direct-partition client before joining a real consumer group.
	cl.Close()
	grouped, err := client(in, in.Password, kgo.ConsumerGroup(in.Group), kgo.ConsumeTopics(in.Topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())
	if err != nil {
		return "GROUP_CONSUME"
	}
	defer grouped.Close()
	groupCtx, groupCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer groupCancel()
	return consumeAndCommit(groupCtx, grouped, in.Topic, value, output)
}
func main() {
	lab := flag.Bool("lab", false, "allow only the named synthetic lab targets")
	flag.CommandLine.SetOutput(io.Discard)
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stdout, "FAIL INVALID_INPUT")
		os.Exit(1)
	}
	in, err := readInput(os.Stdin, *lab)
	if err != nil {
		fmt.Fprintln(os.Stdout, "FAIL INVALID_INPUT")
		os.Exit(1)
	}
	if stage := run(in, os.Stdout); stage != "" {
		fmt.Fprintln(os.Stdout, "FAIL "+stage)
		os.Exit(1)
	}
}
