package registry

import (
	"context"
	"lunaorbit/backend/internal/kafkaadmin"
	"slices"
)

// NativeKafkaFactory reuses the factory configured by UseReadOnlyKafka.
func (s *Service) NativeKafkaFactory() *kafkaadmin.Factory {
	if !s.readOnlyNative || s.GatewayFactory == nil {
		return nil
	}
	gateway, ok := s.GatewayFactory(Registration{}, "").(FranzGateway)
	if !ok {
		return nil
	}
	f, _ := gateway.Factory.(*kafkaadmin.Factory)
	return f
}
func (s *Service) AuthorizeLabACL(ctx context.Context, a Actor, id string) error {
	if a.Role != "admin" || a.ID == "" {
		return failure("FORBIDDEN")
	}
	r, _, e := s.resource(ctx, a, id)
	if e != nil {
		return serviceStoreError(e)
	}
	if !r.Enabled {
		return failure("DISABLED")
	}
	s.labMu.Lock()
	defer s.labMu.Unlock()
	if !slices.Contains(s.labCatalogIDs[a.ID], id) {
		return failure("NOT_FOUND")
	}
	for _, entry := range s.labCatalog {
		if r.SecurityProtocol == entry.SecurityProtocol && sameLabSeeds(r.BootstrapServers, entry.BootstrapServers) {
			return nil
		}
	}
	return failure("NOT_FOUND")
}

// AuthorizedProfile is backend-only and never resolves or returns secret values.
// Factory's network policy validates seeds and every advertised endpoint.
func (s *Service) AuthorizedProfile(ctx context.Context, a Actor, id string) (kafkaadmin.Profile, error) {
	if e := s.authorize(ctx, a, false, false, id); e != nil {
		return kafkaadmin.Profile{}, e
	}
	r, _, e := s.resource(ctx, a, id)
	if e != nil {
		return kafkaadmin.Profile{}, serviceStoreError(e)
	}
	if !r.Enabled {
		return kafkaadmin.Profile{}, failure("DISABLED")
	}
	return kafkaadmin.Profile{RegistrationID: r.ID, BootstrapServers: append([]string(nil), r.BootstrapServers...), SecurityProtocol: r.SecurityProtocol, SASLMechanism: r.SASLMechanism, UsernameSecretRef: r.UsernameRef, PasswordSecretRef: r.PasswordRef, TLSCASecretRef: r.TLSCARef, TLSCertSecretRef: r.TLSCertRef, TLSKeySecretRef: r.TLSKeyRef, TLSServerName: r.TLSServerName}, nil
}

// AuthorizeConfiguredLabTarget checks the current registration against the
// startup-owned lab allowlist. It deliberately does not depend on browser
// seeding or the process-local seed ledger. Mutation permission and write
// enablement must still be checked independently by the calling workspace.
func (s *Service) AuthorizeConfiguredLabTarget(ctx context.Context, a Actor, id string) error {
	if e := s.AuthorizeResource(ctx, a, id, "read", ""); e != nil {
		return e
	}
	r, _, e := s.resource(ctx, a, id)
	if e != nil {
		return e
	}
	if !r.Enabled {
		return failure("DISABLED")
	}
	s.labMu.Lock()
	defer s.labMu.Unlock()
	if !s.readOnlyNative || !validLabCatalogSize(len(s.labCatalog)) {
		return failure("NOT_FOUND")
	}
	for _, entry := range s.labCatalog {
		if r.SecurityProtocol != entry.SecurityProtocol || r.SASLMechanism != entry.SASLMechanism || r.TLSServerName != entry.TLSServerName || !sameLabSeeds(r.BootstrapServers, entry.BootstrapServers) {
			continue
		}
		for _, ref := range secretRefs(r) {
			if ref != "" {
				return failure("NOT_FOUND")
			}
		}
		return nil
	}
	return failure("NOT_FOUND")
}
