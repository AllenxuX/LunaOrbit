package main

import (
	"context"
	"errors"
	"lunaorbit/backend/internal/registry"
	"lunaorbit/backend/internal/workspace"
	"sync"
)

type nativeAccounts struct {
	registry      *registry.Service
	saslEndpoints map[string]string
}

func (n *nativeAccounts) List(ctx context.Context, a registry.Actor, id string) ([]workspace.AccountIdentity, error) {
	p, e := n.registry.AuthorizedProfile(ctx, a, id)
	if e != nil {
		return nil, e
	}
	rows, e := n.registry.NativeKafkaFactory().ListSCRAM(ctx, p)
	if e != nil {
		return nil, e
	}
	out := []workspace.AccountIdentity{}
	for _, r := range rows {
		out = append(out, workspace.AccountIdentity{Username: r.Username, Mechanism: r.Mechanism})
	}
	return out, nil
}
func (n *nativeAccounts) Create(ctx context.Context, a registry.Actor, id, user, password string) error {
	p, e := n.registry.AuthorizedProfile(ctx, a, id)
	if e != nil {
		return e
	}
	return n.registry.NativeKafkaFactory().CreateSCRAM(ctx, p, user, password)
}
func (n *nativeAccounts) Verify(ctx context.Context, a registry.Actor, id, user, password string) error {
	p, e := n.registry.AuthorizedProfile(ctx, a, id)
	if e != nil {
		return e
	}
	listener := p
	if p.SecurityProtocol == "PLAINTEXT" {
		listener.BootstrapServers = nil
		for _, seed := range p.BootstrapServers {
			target, ok := n.saslEndpoints[seed]
			if !ok {
				return errors.New("UNSUPPORTED_API")
			}
			listener.BootstrapServers = append(listener.BootstrapServers, target)
		}
		listener.SecurityProtocol = "SASL_PLAINTEXT"
		listener.SASLMechanism = "SCRAM-SHA-256"
	}
	return n.registry.NativeKafkaFactory().VerifySCRAMListener(ctx, p, listener, user, password)
}

type mockAccounts struct {
	mu    sync.Mutex
	users map[string]map[string]string
}

func (m *mockAccounts) List(_ context.Context, _ registry.Actor, id string) ([]workspace.AccountIdentity, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []workspace.AccountIdentity{}
	for user := range m.users[id] {
		out = append(out, workspace.AccountIdentity{Username: user, Mechanism: "SCRAM-SHA-256"})
	}
	return out, nil
}
func (m *mockAccounts) Create(_ context.Context, _ registry.Actor, id, user, password string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.users[id] == nil {
		m.users[id] = map[string]string{}
	}
	if _, ok := m.users[id][user]; ok {
		return errors.New("CONFLICT")
	}
	m.users[id][user] = password
	return nil
}
func (m *mockAccounts) Verify(_ context.Context, _ registry.Actor, id, user, password string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.users[id][user] != password || password == "" {
		return errors.New("AUTHENTICATION_FAILED")
	}
	return nil
}
