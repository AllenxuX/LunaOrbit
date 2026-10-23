package main

import (
	"context"
	"errors"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"strings"
	"testing"
)

func TestInputGuard(t *testing.T) {
	good := `{"bootstrap_servers":["kafka-source.lunaorbit-integration.svc.cluster.local:9094"],"username":"synthetic","password":"synthetic-password","topic":"lunaorbit-allowed","denied_topic":"lunaorbit-denied","group":"lunaorbit-group","denied_group":"lunaorbit-denied-group"}`
	if _, err := readInput(strings.NewReader(good), true); err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{good, strings.Replace(good, "lunaorbit-integration.svc.cluster.local", "production.svc.cluster.local", 1), strings.Replace(good, ":9094", ":9092", 1), strings.Replace(good, "lunaorbit-denied\"", "lunaorbit-allowed\"", 1), strings.Replace(good, "synthetic-password", "", 1), strings.Replace(good, "\"username\"", "\"unknown\"", 1), good + `{}`, strings.Replace(good, "kafka-source.", "127.0.0.1.", 1)} {
		lab := input != good
		if _, err := readInput(strings.NewReader(input), lab); err == nil {
			t.Fatal("unsafe input accepted")
		}
	}
}
func TestDialGuardAlsoRejectsAdvertisedTargets(t *testing.T) {
	for _, address := range []string{"127.0.0.1:9094", "kafka-source.lunaorbit-integration.svc.cluster.local:9092", "kafka-source.production.svc.cluster.local:9094", "kafka-source.lunaorbit-integration.svc.cluster.local.attacker:9094"} {
		if allowedEndpoint(address) {
			t.Fatal("unexpected endpoint allowed")
		}
	}
	if !allowedEndpoint("kafka-target.lunaorbit-integration.svc.cluster.local:9094") {
		t.Fatal("lab target rejected")
	}
}
func TestGroupAuthorizationDenialAtBothProtocolLayers(t *testing.T) {
	const group = "synthetic-group"
	body := func(code int16) *kmsg.DescribeGroupsResponse {
		return &kmsg.DescribeGroupsResponse{Groups: []kmsg.DescribeGroupsResponseGroup{{Group: group, ErrorCode: code}}}
	}
	for _, tc := range []struct {
		response *kmsg.DescribeGroupsResponse
		err      error
		denied   bool
		want     bool
	}{
		{nil, kerr.GroupAuthorizationFailed, true, true},
		{body(30), nil, true, true},
		{nil, kerr.GroupAuthorizationFailed, false, false},
		{body(30), nil, false, false},
		{body(0), nil, true, false},
		{nil, kerr.CoordinatorNotAvailable, true, false},
		{body(69), nil, false, true},
	} {
		got, _ := groupResult(group, tc.denied, tc.response, tc.err)
		if got != tc.want {
			t.Fatal("incorrect authorization result")
		}
	}
	if ok, diagnostic := groupResult(group, true, nil, kerr.CoordinatorNotAvailable); ok || diagnostic != "GROUP_REQUEST_CODE_15" {
		t.Fatal(ok, diagnostic)
	}
}

type fixtureGroupConsumer struct {
	record    *kgo.Record
	commitErr error
	commits   int
	cancel    context.CancelFunc
}

func (f *fixtureGroupConsumer) PollFetches(context.Context) kgo.Fetches {
	f.cancel()
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: f.record.Topic, Partitions: []kgo.FetchPartition{{Partition: 0, Records: []*kgo.Record{f.record}}}}}}}
}
func (f *fixtureGroupConsumer) CommitRecords(_ context.Context, records ...*kgo.Record) error {
	f.commits++
	return f.commitErr
}
func TestGroupConsumerCommitsOnlyItsOwnTopicMarker(t *testing.T) {
	for _, tc := range []struct {
		topic     string
		value     string
		commitErr error
		stage     string
		commits   int
	}{
		{"other-topic", "synthetic-marker", nil, "GROUP_CONSUME", 0},
		{"synthetic-topic", "other-marker", nil, "GROUP_CONSUME", 0},
		{"synthetic-topic", "synthetic-marker", nil, "", 1},
		{"synthetic-topic", "synthetic-marker", errors.New("private untrusted detail"), "GROUP_COMMIT", 1},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		consumer := &fixtureGroupConsumer{record: &kgo.Record{Topic: tc.topic, Value: []byte(tc.value)}, commitErr: tc.commitErr, cancel: cancel}
		var output strings.Builder
		stage := consumeAndCommit(ctx, consumer, "synthetic-topic", []byte("synthetic-marker"), &output)
		if stage != tc.stage || consumer.commits != tc.commits {
			t.Fatal(stage, consumer.commits)
		}
		if strings.Contains(output.String(), "private") {
			t.Fatal("error detail leaked")
		}
		if tc.commitErr != nil && strings.Contains(output.String(), "PASS GROUP_COMMIT") {
			t.Fatal("failed commit reported success")
		}
	}
}
