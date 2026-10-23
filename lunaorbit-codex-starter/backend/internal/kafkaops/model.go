package kafkaops

import (
	"context"
	"time"
)

type Broker struct {
	ID   int32  `json:"id"`
	Host string `json:"host"`
	Port int32  `json:"port"`
}
type Partition struct {
	Partition int32   `json:"partition"`
	Leader    int32   `json:"leader"`
	Replicas  []int32 `json:"replicas"`
	ISR       []int32 `json:"isr"`
	ErrorCode string  `json:"error_code,omitempty"`
}
type Topic struct {
	TopicID    string      `json:"topic_id,omitempty"`
	Name       string      `json:"name"`
	Internal   bool        `json:"internal"`
	Partitions []Partition `json:"partitions"`
	RFMin      *int        `json:"rf_min"`
	RFMax      *int        `json:"rf_max"`
	ErrorCode  string      `json:"error_code,omitempty"`
}
type Group struct {
	AssignedTopics     []string `json:"assigned_topics"`
	SubscribedTopics   []string `json:"subscribed_topics"`
	AssignmentsKnown   bool     `json:"assignments_known"`
	SubscriptionsKnown bool     `json:"subscriptions_known"`
	EvidenceIssue      string   `json:"evidence_issue,omitempty"`
	ID                 string   `json:"id"`
	State              string   `json:"state"`
	ProtocolType       string   `json:"protocol_type"`
	ErrorCode          string   `json:"error_code,omitempty"`
}
type Offset struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Committed *int64 `json:"committed"`
	LogEnd    *int64 `json:"log_end"`
	Lag       *int64 `json:"lag"`
	ErrorCode string `json:"error_code,omitempty"`
}
type Capability struct {
	Feature   string `json:"feature"`
	Supported bool   `json:"supported"`
	Reason    string `json:"reason"`
}
type Snapshot struct {
	Synthetic    bool         `json:"synthetic"`
	ObservedAt   time.Time    `json:"observed_at"`
	Stale        bool         `json:"stale"`
	State        string       `json:"state"`
	Reason       string       `json:"reason,omitempty"`
	Brokers      []Broker     `json:"brokers"`
	Topics       []Topic      `json:"topics"`
	Groups       []Group      `json:"groups"`
	Offsets      []Offset     `json:"offsets"`
	Capabilities []Capability `json:"capabilities"`
}
type Reader interface {
	Read(context.Context, string, string) (Snapshot, error)
}
type Error struct{ Code string }

func (e *Error) Error() string { return "kafka operations: " + e.Code }
