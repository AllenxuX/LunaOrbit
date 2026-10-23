package changes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"lunaorbit/backend/internal/registry"
	"strings"
	"sync"
	"testing"
	"time"
)

func setup(t *testing.T) (*Service, registry.Actor, registry.Actor) {
	t.Helper()
	v, _ := NewVault(bytes.Repeat([]byte{7}, 32))
	s := NewService(NewMemoryStore(), v, DefaultCatalog(), NewMockAdapter())
	owner := registry.Actor{ID: "owner", Role: "admin"}
	approver := registry.Actor{ID: "approver", Role: "admin"}
	s.SourceAuthorizer = func(_ context.Context, a registry.Actor, t Target) error {
		if a.ID != "owner" {
			return ErrNotFound
		}
		return nil
	}
	s.ApprovalAuthorizer = StaticApprovals{"approver": []string{DefaultCatalog()[0].ID}, "owner": []string{DefaultCatalog()[0].ID}}
	return s, owner, approver
}
func submit(t *testing.T, s *Service, a registry.Actor) Request {
	t.Helper()
	targets, e := s.Targets(context.Background(), a)
	if e != nil || len(targets) != 1 {
		t.Fatal(e, targets)
	}
	r, e := s.Submit(context.Background(), a, Input{TargetID: targets[0].ID, TopicRule: "orders.*|events.*", ExpectedResourceVersion: targets[0].ResourceVersion})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func approve(t *testing.T, s *Service, owner, approver registry.Actor) Request {
	t.Helper()
	r := submit(t, s, owner)
	r, e := s.Approve(context.Background(), approver, r.ID, r.Revision)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestSyntheticExecutionAndEncryptedSnapshot(t *testing.T) {
	s, owner, ap := setup(t)
	r := approve(t, s, owner, ap)
	r, e := s.Execute(context.Background(), owner, r.ID, r.Revision, "success")
	if e != nil || r.Status != "succeeded" || r.Evidence.ReplicationHealthy == nil || !*r.Evidence.ReplicationHealthy {
		t.Fatal(r, e)
	}
	b, e := s.Store.Backup(context.Background(), r.ID)
	if e != nil || bytes.Contains(b, []byte("DO_NOT_EXPOSE")) {
		t.Fatal("plaintext backup")
	}
	raw, e := s.Vault.Open(b, aad(r))
	if e != nil || !bytes.Contains(raw, []byte("DO_NOT_EXPOSE")) {
		t.Fatal("full raw backup missing", e)
	}
	r.OwnerID = "other"
	if _, e = s.Vault.Open(b, aad(r)); e == nil {
		t.Fatal("AAD owner binding")
	}
	payload, _ := json.Marshal(r)
	if strings.Contains(string(payload), "DO_NOT_EXPOSE") || strings.Contains(string(payload), "approval_hash") {
		t.Fatal("secret response")
	}
	doc := ConfigDocument{Raw: raw}
	encoded, _ := json.Marshal(doc)
	if strings.Contains(string(encoded)+fmt.Sprintf("%+v %#v", doc, doc), "DO_NOT_EXPOSE") {
		t.Fatal("document leaked")
	}
	m := s.Adapter.(*MockAdapter)
	if m.Updates != 1 || m.Restarts != 1 {
		t.Fatal("mutation count")
	}
	targets, _ := s.Targets(context.Background(), owner)
	if targets[0].ResourceVersion != "2" {
		t.Fatal("target RV not refreshed")
	}
}
func TestApprovalScopesAndConcurrentCAS(t *testing.T) {
	s, owner, ap := setup(t)
	r := submit(t, s, owner)
	if _, e := s.Approve(context.Background(), owner, r.ID, r.Revision); e == nil || e.Error() != "SELF_APPROVAL_DENIED" {
		t.Fatal(e)
	}
	if _, e := s.Approve(context.Background(), registry.Actor{ID: "outside", Role: "admin"}, r.ID, r.Revision); !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
	if _, e := s.Get(context.Background(), registry.Actor{ID: "outside", Role: "admin"}, r.ID); !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
	r, e := s.Approve(context.Background(), ap, r.ID, r.Revision)
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	out := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, e := s.Execute(context.Background(), owner, r.ID, r.Revision, "success")
			out <- e
		}()
	}
	wg.Wait()
	close(out)
	success := 0
	for e := range out {
		if e == nil {
			success++
		}
	}
	if success != 1 || s.Adapter.(*MockAdapter).Updates != 1 {
		t.Fatal("double execute")
	}
}
func TestExecutionFailuresAndBackupAuditGate(t *testing.T) {
	for _, scenario := range []string{"conflict", "rollout_failed", "metrics_missing", "timeout"} {
		t.Run(scenario, func(t *testing.T) {
			s, o, a := setup(t)
			r := approve(t, s, o, a)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			r, e := s.Execute(ctx, o, r.ID, r.Revision, scenario)
			if e != nil {
				t.Fatal(e)
			}
			expected := "manual_review"
			if scenario == "conflict" {
				expected = "conflict"
			}
			if r.Status != expected {
				t.Fatal(r.Status)
			}
			if scenario == "timeout" && s.Adapter.(*MockAdapter).Updates != 0 {
				t.Fatal("timeout wrote")
			}
		})
	}
	s, o, a := setup(t)
	r := approve(t, s, o, a)
	s.Store.(*MemoryStore).FailAudit = true
	if _, e := s.Execute(context.Background(), o, r.ID, r.Revision, "success"); e == nil {
		t.Fatal("audit should prevent execute")
	}
	if s.Adapter.(*MockAdapter).Updates != 0 {
		t.Fatal("write without audit")
	}
}
func TestDefaultDenyAndPreview(t *testing.T) {
	s, o, _ := setup(t)
	s.SourceAuthorizer = nil
	if items, _ := s.Targets(context.Background(), o); len(items) != 0 {
		t.Fatal("default scope open")
	}
	s, o, _ = setup(t)
	in := Input{TargetID: DefaultCatalog()[0].ID, TopicRule: "orders.*", ExpectedResourceVersion: "1", Preview: true}
	r, e := s.Submit(context.Background(), o, in)
	if e != nil || r.Status != "preview" {
		t.Fatal(e)
	}
	rows, _ := s.Store.List(context.Background())
	if len(rows) != 0 {
		t.Fatal("preview persisted")
	}
	in.Preview = false
	in.ExpectedResourceVersion = "stale"
	if _, e = s.Submit(context.Background(), o, in); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	in.ExpectedResourceVersion = "1"
	in.TopicRule = "password=DO_NOT_EXPOSE"
	if _, e = s.Submit(context.Background(), o, in); e == nil {
		t.Fatal("untyped content accepted")
	}
}
func TestSnapshotAADIncludesDeploymentUID(t *testing.T) {
	s, o, a := setup(t)
	r := approve(t, s, o, a)
	r, e := s.Execute(context.Background(), o, r.ID, r.Revision, "success")
	if e != nil {
		t.Fatal(e)
	}
	cipher, e := s.Store.Backup(context.Background(), r.ID)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Vault.Open(cipher, aad(r)); e != nil {
		t.Fatal(e)
	}
	r.WorkloadUID = "replacement-deployment-uid"
	if _, e = s.Vault.Open(cipher, aad(r)); e == nil {
		t.Fatal("snapshot accepted a different deployment UID")
	}
}

func TestLabSelfApprovalRequiresAllExplicitFlags(t *testing.T) {
	for _, tc := range []struct {
		name              string
		lab, writes, self bool
		allowed           bool
	}{
		{"production remains independent", false, true, true, false},
		{"lab default", true, false, false, false},
		{"lab writes only", true, true, false, false},
		{"lab self only", true, false, true, false},
		{"explicit lab admin", true, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, owner, _ := setup(t)
			r := submit(t, s, owner)
			if tc.lab {
				s.Adapter = &LabAdapter{}
			}
			s.EnableWrites = tc.writes
			s.LabSelfApproval = tc.self
			r, e := s.Approve(context.Background(), owner, r.ID, r.Revision)
			if tc.allowed {
				if e != nil || !r.CanExecute || r.Approver != owner.ID {
					t.Fatal(r, e)
				}
			} else if e == nil {
				t.Fatal("self approval escaped flags")
			}
		})
	}
}
