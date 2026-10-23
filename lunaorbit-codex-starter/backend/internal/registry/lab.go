package registry

import (
	"lunaorbit/backend/internal/kafkaadmin"
	"lunaorbit/backend/internal/kafkaops"
)

// UseReadOnlyKafka enables native probes and reads only. It does not install
// any Kafka mutation implementation or expose the secret store to callers.
func (s *Service) UseReadOnlyKafka(policy *kafkaadmin.NetworkPolicy) error {
	factory, err := kafkaadmin.NewFactory(policy, s.secrets)
	if err != nil {
		return err
	}
	s.readOnlyNative = true
	s.GatewayFactory = func(r Registration, _ string) KafkaAdminGateway {
		return FranzGateway{Factory: factory, Registration: r}
	}
	s.OperationsFactory = func(r Registration, _ string) kafkaops.Reader {
		return factory.BindReader(kafkaadmin.Profile{RegistrationID: r.ID, BootstrapServers: r.BootstrapServers, SecurityProtocol: r.SecurityProtocol, SASLMechanism: r.SASLMechanism, UsernameSecretRef: r.UsernameRef, PasswordSecretRef: r.PasswordRef, TLSCASecretRef: r.TLSCARef, TLSCertSecretRef: r.TLSCertRef, TLSKeySecretRef: r.TLSKeyRef, TLSServerName: r.TLSServerName})
	}
	return nil
}
