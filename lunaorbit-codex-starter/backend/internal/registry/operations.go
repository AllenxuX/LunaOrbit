package registry

import (
	"context"
	"errors"
	"lunaorbit/backend/internal/kafkaops"
	"strings"
	"time"
)

type OperationsFactory func(Registration, string) kafkaops.Reader

func (s *Service) ReadOperations(ctx context.Context, actor Actor, id, kind, resource, scenario string) (kafkaops.Snapshot, error) {
	empty := kafkaops.Snapshot{}
	if e := s.authorize(ctx, actor, false, false, id); e != nil {
		return empty, e
	}
	if kind != "brokers" && kind != "topics" && kind != "groups" && kind != "offsets" {
		return empty, failure("INVALID_ARGUMENT")
	}
	if len(resource) > 249 || strings.ContainsAny(resource, "\x00\r\n") {
		return empty, failure("INVALID_ARGUMENT")
	}
	row, _, e := s.resource(ctx, actor, id)
	if e != nil {
		return empty, serviceStoreError(e)
	}
	if resource != "" {
		name := "topic:" + resource
		if kind == "groups" || kind == "offsets" {
			name = "group:" + resource
		}
		if e = s.AuthorizeResource(ctx, actor, id, "read", name); e != nil {
			return empty, e
		}
	}
	if !row.Enabled {
		return empty, failure("DISABLED")
	}
	for _, ref := range secretRefs(row) {
		if ref != "" {
			if _, e = s.secrets.Get(ctx, ref); e != nil {
				return empty, failure("SECRET_UNAVAILABLE")
			}
		}
	}
	if !s.dto(ctx, row).CredentialsConfigured {
		return empty, failure("SECRET_UNAVAILABLE")
	}
	s.mu.Lock()
	if s.operationStarts == nil {
		s.operationStarts = map[string][]time.Time{}
	}
	now := time.Now()
	for key, times := range s.operationStarts {
		if len(times) == 0 || now.Sub(times[len(times)-1]) >= time.Minute {
			delete(s.operationStarts, key)
		}
	}
	key := actor.ID + ":" + id
	recent := []time.Time{}
	for _, t := range s.operationStarts[key] {
		if now.Sub(t) < time.Minute {
			recent = append(recent, t)
		}
	}
	if len(recent) >= 30 || len(s.operationStarts) >= 256 && len(recent) == 0 {
		s.mu.Unlock()
		return empty, failure("RATE_LIMITED")
	}
	s.operationStarts[key] = append(recent, now)
	factory := s.OperationsFactory
	s.mu.Unlock()
	if factory == nil {
		return empty, failure("UNSUPPORTED_API")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	select {
	case s.slots <- struct{}{}:
	case <-ctx.Done():
		return empty, failure("TIMEOUT")
	}
	defer func() { <-s.slots }()
	snapshot, e := factory(row, scenario).Read(ctx, kind, resource)
	if ctx.Err() != nil {
		return empty, failure("TIMEOUT")
	}
	if e != nil {
		var op *kafkaops.Error
		if errors.As(e, &op) {
			switch op.Code {
			case "NETWORK_DENIED", "ADVERTISED_UNREACHABLE", "BROKER_LIMIT", "CLUSTER_MISMATCH", "AUTHORIZATION_DENIED", "BROKER_UNREACHABLE", "UNSUPPORTED_API", "TIMEOUT", "INVALID_ARGUMENT", "AUTHENTICATION_FAILED", "TLS_HANDSHAKE_FAILED":
				return empty, failure(op.Code)
			}
		}
		return empty, failure("BROKER_UNREACHABLE")
	}
	snapshot.Topics = filterTopics(snapshot.Topics, func(n string) bool { return s.AuthorizeResource(ctx, actor, id, "read", "topic:"+n) == nil })
	snapshot.Groups = filterGroups(snapshot.Groups, func(n string) bool { return s.AuthorizeResource(ctx, actor, id, "read", "group:"+n) == nil })
	offsets := snapshot.Offsets[:0]
	for _, o := range snapshot.Offsets {
		if s.AuthorizeResource(ctx, actor, id, "read", "group:"+o.Group) == nil && s.AuthorizeResource(ctx, actor, id, "read", "topic:"+o.Topic) == nil {
			offsets = append(offsets, o)
		}
	}
	snapshot.Offsets = offsets
	return snapshot, nil
}
