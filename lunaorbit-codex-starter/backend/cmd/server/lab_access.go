package main

import (
	"errors"
	"lunaorbit/backend/internal/access"
	"lunaorbit/backend/internal/registry"
)

func configureLabAccess(s *access.Service, r *registry.Service, env func(string) string) error {
	if env("LUNAORBIT_RUNTIME_MODE") != "admin-lab" {
		return errors.New("admin lab required for ACL adapter")
	}
	f := r.NativeKafkaFactory()
	if f == nil {
		return errors.New("bounded native Kafka factory required")
	}
	n := &access.Native{Registry: r, Factory: f}
	s.Native = n
	s.LabWrites = env("ENABLE_KAFKA_WRITES") == "true"
	s.LabSelfApproval = s.LabWrites && env("KAFKA_LAB_ACL_SELF_APPROVAL") == "true"
	if s.LabWrites {
		s.Writer = n
	}
	return nil
}
