package audit

import (
	"context"
	"testing"
)

func TestSQLSinkRejectsInvalidEvent(t *testing.T) {
	sink := SQLSink{}
	for _, event := range []Event{{}, {ActorID: "synthetic-user", Action: "read", TargetID: "synthetic-target", Outcome: "unknown"}} {
		if err := sink.Record(context.Background(), event); err == nil || err.Error() != "invalid audit event" {
			t.Fatalf("expected validation failure, got %v", err)
		}
	}
}

func TestSQLSinkFailsClosedWithoutStorage(t *testing.T) {
	err := (SQLSink{}).Record(context.Background(), Event{ActorID: "synthetic-user", Action: "read", TargetID: "synthetic-target", Outcome: "success"})
	if err == nil || err.Error() != "audit storage unavailable" {
		t.Fatalf("expected storage failure, got %v", err)
	}
}
