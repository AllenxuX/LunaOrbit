// Package audit defines a metadata-only sink. It never accepts credentials or raw configs.
package audit

import (
	"context"
	"database/sql"
	"errors"
)

type Event struct{ ActorID, Action, TargetID, Outcome string }
type Sink interface {
	Record(context.Context, Event) error
}
type SQLSink struct{ DB *sql.DB }

func (s SQLSink) Record(ctx context.Context, e Event) error {
	if e.ActorID == "" || e.Action == "" || e.TargetID == "" || (e.Outcome != "success" && e.Outcome != "failure" && e.Outcome != "denied") {
		return errors.New("invalid audit event")
	}
	if s.DB == nil {
		return errors.New("audit storage unavailable")
	}
	_, err := s.DB.ExecContext(ctx, `INSERT INTO audit_events (actor_id, action, target_id, outcome) VALUES ($1,$2,$3,$4)`, e.ActorID, e.Action, e.TargetID, e.Outcome)
	if err != nil {
		return errors.New("audit persistence failed")
	}
	return nil
}
