package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"lunaorbit/backend/internal/kafkaadmin"
	"lunaorbit/backend/internal/kafkaops"
	"lunaorbit/backend/internal/registry"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestResourceIntentRejectsUnsafeDelete(t *testing.T) {
	for _, name := range []string{"__consumer_offsets", "", "bad/name"} {
		if validateResourceIntent(ResourceIntent{Kind: "topic_delete", Resource: ResourceTarget{Type: "TOPIC", Name: name}, ConfirmationName: name}) == nil {
			t.Fatalf("accepted %q", name)
		}
	}
	if validateResourceIntent(ResourceIntent{Kind: "topic_delete", Resource: ResourceTarget{Type: "TOPIC", Name: "events"}, ConfirmationName: "other"}) == nil {
		t.Fatal("unconfirmed delete")
	}
}

// resourceFake keeps broker acceptance separate from observed metadata/configuration.
type resourceFake struct {
	snapshot ConfigSnapshot
	topic    *kafkaops.Topic
	active   []kafkaadmin.PartitionReassignment
	writes   int
	err      error
}

func (f *resourceFake) DescribeConfigs(context.Context, registry.Actor, string, kafkaadmin.ConfigResource) (ConfigSnapshot, error) {
	return f.snapshot, nil
}
func (f *resourceFake) ValidateConfigs(context.Context, registry.Actor, string, kafkaadmin.ConfigResource, []kafkaadmin.ConfigChange) error {
	return nil
}
func (f *resourceFake) AlterConfigs(context.Context, registry.Actor, string, kafkaadmin.ConfigResource, []kafkaadmin.ConfigChange) error {
	f.writes++
	return f.err
}
func (f *resourceFake) DeleteTopic(context.Context, registry.Actor, string, string) error {
	f.writes++
	return f.err
}
func (f *resourceFake) Reassign(context.Context, registry.Actor, string, string, []kafkaadmin.ReplicaAssignment) error {
	f.writes++
	return f.err
}
func (f *resourceFake) CancelReassignments(context.Context, registry.Actor, string, string, []int32) error {
	f.writes++
	return f.err
}
func (f *resourceFake) Reassignments(context.Context, registry.Actor, string, string) ([]kafkaadmin.PartitionReassignment, error) {
	return f.active, nil
}
func (f *resourceFake) TopicState(context.Context, registry.Actor, string, string) (*kafkaops.Topic, error) {
	return f.topic, nil
}
func configValue(s string) *string { return &s }
func resourceSetup(t *testing.T) (*Service, registry.Actor, string, *resourceFake) {
	s, a, id, _ := setup(t)
	f := &resourceFake{topic: &kafkaops.Topic{Name: "events", Partitions: []kafkaops.Partition{{Partition: 0, Replicas: []int32{0}, ISR: []int32{0}}}}, snapshot: ConfigSnapshot{Resource: ResourceTarget{Type: "TOPIC", Name: "events"}, Entries: []kafkaadmin.ConfigEntry{{Name: "retention.ms", Value: configValue("1000"), DefaultValue: configValue("7000"), Source: "DYNAMIC_TOPIC_CONFIG"}, {Name: "cleanup.policy", Value: configValue("delete"), Source: "DEFAULT_CONFIG"}}}}
	s.Resources = f
	return s, a, id, f
}
func createResourceTest(t *testing.T, s *Service, a registry.Actor, id string, in ResourceIntent) ResourceOperation {
	t.Helper()
	p, e := s.PreviewResource(context.Background(), a, id, in)
	if e != nil {
		t.Fatal(e)
	}
	op, e := s.CreateResourceOperation(context.Background(), a, id, in, p.Fingerprint, uuid.NewString())
	if e != nil {
		t.Fatal(e)
	}
	return op
}
func TestResourceConfigStaleAndPartialObservation(t *testing.T) {
	s, a, id, f := resourceSetup(t)
	s.LabAutoApprove = true
	ctx := context.Background()
	in := ResourceIntent{Kind: "configs", Resource: ResourceTarget{Type: "TOPIC", Name: "events"}, Changes: []kafkaadmin.ConfigChange{{Name: "retention.ms", Operation: "SET", Value: configValue("2000")}, {Name: "cleanup.policy", Operation: "SET", Value: configValue("compact")}}}
	op := createResourceTest(t, s, a, id, in)
	f.snapshot.Entries[0].Value = configValue("9999")
	if _, e := s.ExecuteResourceOperation(ctx, a, id, op.ID, op.Revision); e != errConflict || f.writes != 0 {
		t.Fatal("stale config submitted", e, f.writes)
	}
	f.snapshot.Entries[0].Value = configValue("1000")
	op, e := s.ExecuteResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil || op.Status != "observing" || f.writes != 1 {
		t.Fatal(op, e)
	}
	if _, e = s.ExecuteResourceOperation(ctx, a, id, op.ID, op.Revision); e != errConflict || f.writes != 1 {
		t.Fatal("replayed", e)
	}
	f.snapshot.Entries[0].Value = configValue("2000")
	op, e = s.RefreshResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil || op.Status != "partial_failure" {
		t.Fatal(op, e)
	}
	f.snapshot.Entries[1].Value = configValue("compact")
	f.snapshot.Entries[1].Source = "DYNAMIC_TOPIC_CONFIG"
	op, e = s.RefreshResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil || op.Status != "succeeded" {
		t.Fatal(op, e)
	}
}
func TestResourceRestoreDefaultRequiresInheritance(t *testing.T) {
	s, a, id, f := resourceSetup(t)
	s.LabAutoApprove = true
	ctx := context.Background()
	op := createResourceTest(t, s, a, id, ResourceIntent{Kind: "configs", Resource: ResourceTarget{Type: "TOPIC", Name: "events"}, Changes: []kafkaadmin.ConfigChange{{Name: "retention.ms", Operation: "DELETE"}}})
	if op.Plan.After.Entries[1].Name == "retention.ms" && op.Plan.After.Entries[1].Value != nil {
		t.Fatal("preview default")
	}
	op, e := s.ExecuteResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil {
		t.Fatal(e)
	}
	f.snapshot.Entries[0].Value = configValue("7000")
	op, e = s.RefreshResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil || op.Status == "succeeded" {
		t.Fatal("matching override is not inheritance", op, e)
	}
	f.snapshot.Entries[0].Source = "DEFAULT_CONFIG"
	op, e = s.RefreshResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil || op.Status != "succeeded" {
		t.Fatal(op, e)
	}
}
func TestResourceDeleteAcceptanceIndependentApprovalAndIdempotency(t *testing.T) {
	s, a, id, f := resourceSetup(t)
	ctx := context.Background()
	in := ResourceIntent{Kind: "topic_delete", Resource: ResourceTarget{Type: "TOPIC", Name: "events"}, ConfirmationName: "events"}
	op := createResourceTest(t, s, a, id, in)
	if _, e := s.ApproveResourceOperation(ctx, a, id, op.ID, op.Revision); e == nil {
		t.Fatal("self approved")
	}
	s.LabAutoApprove = true
	op, e := s.ApproveResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil {
		t.Fatal(e)
	}
	op, e = s.ExecuteResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil || op.Status != "observing" {
		t.Fatal(op, e)
	}
	op, e = s.RefreshResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil || op.Status != "observing" {
		t.Fatal("accepted != deleted", op, e)
	}
	f.topic = nil
	op, e = s.RefreshResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil || op.Status != "succeeded" {
		t.Fatal(op, e)
	}
	replay, e := s.CreateResourceOperation(ctx, a, id, in, op.Plan.Fingerprint, op.RequestID)
	if e != nil || replay.ID != op.ID || f.writes != 1 {
		t.Fatal(replay, e)
	}
	in.ConfirmationName = "different"
	if _, e = s.CreateResourceOperation(ctx, a, id, in, op.Plan.Fingerprint, op.RequestID); e != errConflict {
		t.Fatal("request id reused", e)
	}
}
func TestResourceCancelWaitsForOriginalReplicas(t *testing.T) {
	s, a, id, f := resourceSetup(t)
	s.LabAutoApprove = true
	ctx := context.Background()
	f.active = []kafkaadmin.PartitionReassignment{{Partition: 0, Replicas: []int32{0, 1}, AddingReplicas: []int32{1}, RemovingReplicas: []int32{0}}}
	op := createResourceTest(t, s, a, id, ResourceIntent{Kind: "reassign_cancel", Resource: ResourceTarget{Type: "TOPIC", Name: "events"}, Partitions: []int32{0}})
	op, e := s.ExecuteResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil {
		t.Fatal(e)
	}
	f.active = nil
	f.topic.Partitions[0].Replicas = []int32{1}
	f.topic.Partitions[0].ISR = []int32{1}
	op, e = s.RefreshResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil || op.Status == "succeeded" {
		t.Fatal("list absence cannot confirm cancellation", op, e)
	}
	f.topic.Partitions[0].Replicas = []int32{0}
	f.topic.Partitions[0].ISR = []int32{0}
	op, e = s.RefreshResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil || op.Status != "succeeded" {
		t.Fatal(op, e)
	}
}
func TestResourceMetadataInternalAndSensitiveSnapshot(t *testing.T) {
	s, a, id, f := resourceSetup(t)
	ctx := context.Background()
	f.topic.Internal = true
	in := ResourceIntent{Kind: "configs", Resource: ResourceTarget{Type: "TOPIC", Name: "events"}, Changes: []kafkaadmin.ConfigChange{{Name: "retention.ms", Operation: "DELETE"}}}
	if _, e := s.PreviewResource(ctx, a, id, in); e == nil {
		t.Fatal("metadata internal config mutation")
	}
	f.snapshot.Entries = append(f.snapshot.Entries, kafkaadmin.ConfigEntry{Name: "ssl.keystore.password", Value: configValue("synthetic-secret"), Documentation: "synthetic-secret", Synonyms: []kafkaadmin.ConfigSynonym{{Name: "x", Value: configValue("synthetic-secret")}}})
	snap, e := s.ResourceConfigs(ctx, a, id, in.Resource)
	if e != nil {
		t.Fatal(e)
	}
	b, _ := json.Marshal(snap)
	if strings.Contains(string(b), "synthetic-secret") {
		t.Fatal("leaked sensitive config")
	}
}
func TestResourceIndependentApprovalScopeAndRevokedWrite(t *testing.T) {
	s, owner, id, f := resourceSetup(t)
	ctx := context.Background()
	approver := registry.Actor{ID: "reviewer", Role: "viewer"}
	_, e := s.Registry.CreateGrant(ctx, owner, id, registry.GrantInput{SubjectType: "user", SubjectID: approver.ID, Role: "admin", TopicPrefix: "events", GroupPrefix: "group-"})
	if e != nil {
		t.Fatal(e)
	}
	op := createResourceTest(t, s, owner, id, ResourceIntent{Kind: "topic_delete", Resource: ResourceTarget{Type: "TOPIC", Name: "events"}, ConfirmationName: "events"})
	op, e = s.ApproveResourceOperation(ctx, approver, id, op.ID, op.Revision)
	if e != nil || op.ApprovedBy != approver.ID {
		t.Fatal(op, e)
	}
	s.WriteEnabled = func(context.Context, registry.Actor, string) error { return errors.New("DISABLED") }
	if _, e = s.ExecuteResourceOperation(ctx, owner, id, op.ID, op.Revision); e == nil || f.writes != 0 {
		t.Fatal("write disabled ignored", e)
	}
	if _, e = s.ResourceConfigs(ctx, approver, id, ResourceTarget{Type: "BROKER", Name: ""}); e == nil {
		t.Fatal("scoped broker defaults read")
	}
	if _, e = s.ResourceConfigs(ctx, approver, id, ResourceTarget{Type: "TOPIC", Name: "unrelated"}); e == nil {
		t.Fatal("scope read leaked")
	}
}
func TestResourceUnknownSubmissionPersistsAndCannotReplay(t *testing.T) {
	s, a, id, f := resourceSetup(t)
	s.LabAutoApprove = true
	ctx := context.Background()
	op := createResourceTest(t, s, a, id, ResourceIntent{Kind: "topic_delete", Resource: ResourceTarget{Type: "TOPIC", Name: "events"}, ConfirmationName: "events"})
	f.err = context.DeadlineExceeded
	op, e := s.ExecuteResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil || op.Status != "unknown" {
		t.Fatal(op, e)
	}
	restored := NewService(s.Registry, s.Store, s.Gateway)
	restored.Resources = f
	restored.WriteEnabled = s.WriteEnabled
	if _, e = restored.ExecuteResourceOperation(ctx, a, id, op.ID, op.Revision); e != errConflict || f.writes != 1 {
		t.Fatal("restart replay", e)
	}
	f.topic = nil
	op, e = restored.RefreshResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil || op.Status != "succeeded" {
		t.Fatal(op, e)
	}
}
func TestResourceSetRequiresObservedOverrideSource(t *testing.T) {
	s, a, id, f := resourceSetup(t)
	s.LabAutoApprove = true
	ctx := context.Background()
	f.snapshot.Resource = ResourceTarget{Type: "BROKER", Name: ""}
	f.snapshot.Entries[0].Source = "DEFAULT_CONFIG"
	op := createResourceTest(t, s, a, id, ResourceIntent{Kind: "configs", Resource: ResourceTarget{Type: "BROKER", Name: ""}, Changes: []kafkaadmin.ConfigChange{{Name: "retention.ms", Operation: "SET", Value: configValue("1000")}}})
	op, e := s.ExecuteResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil {
		t.Fatal(e)
	}
	op, e = s.RefreshResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil || op.Status == "succeeded" {
		t.Fatal("inherited equal value cannot prove override set", op, e)
	}
	f.snapshot.Entries[0].Source = "DYNAMIC_DEFAULT_BROKER_CONFIG"
	op, e = s.RefreshResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil || op.Status != "succeeded" {
		t.Fatal(op, e)
	}
}
func TestResourceDefiniteRejectedSubmissionFailed(t *testing.T) {
	s, a, id, f := resourceSetup(t)
	s.LabAutoApprove = true
	ctx := context.Background()
	op := createResourceTest(t, s, a, id, ResourceIntent{Kind: "topic_delete", Resource: ResourceTarget{Type: "TOPIC", Name: "events"}, ConfirmationName: "events"})
	f.err = &kafkaops.Error{Code: "TOPIC_DELETION_DISABLED"}
	op, e := s.ExecuteResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil || op.Status != "failed" || op.Code != "TOPIC_DELETION_DISABLED" {
		t.Fatal(op, e)
	}
}
func TestResourceObservationRejectsSameNameRecreation(t *testing.T) {
	for _, kind := range []string{"configs", "topic_delete"} {
		t.Run(kind, func(t *testing.T) {
			s, a, id, f := resourceSetup(t)
			s.LabAutoApprove = true
			ctx := context.Background()
			f.topic.TopicID = "original-id"
			in := ResourceIntent{Kind: kind, Resource: ResourceTarget{Type: "TOPIC", Name: "events"}, ConfirmationName: "events"}
			if kind == "configs" {
				in.ConfirmationName = ""
				in.Changes = []kafkaadmin.ConfigChange{{Name: "retention.ms", Operation: "SET", Value: configValue("1000")}}
			}
			op := createResourceTest(t, s, a, id, in)
			op, e := s.ExecuteResourceOperation(ctx, a, id, op.ID, op.Revision)
			if e != nil {
				t.Fatal(e)
			}
			f.topic.TopicID = "new-id"
			op, e = s.RefreshResourceOperation(ctx, a, id, op.ID, op.Revision)
			if e != nil || op.Status != "unknown" || op.Code != "RESOURCE_IDENTITY_CHANGED" {
				t.Fatal(op, e)
			}
		})
	}
}
func TestResourceRestorePreviewDoesNotGuessInheritedValue(t *testing.T) {
	s, a, id, _ := resourceSetup(t)
	p, e := s.PreviewResource(context.Background(), a, id, ResourceIntent{Kind: "configs", Resource: ResourceTarget{Type: "TOPIC", Name: "events"}, Changes: []kafkaadmin.ConfigChange{{Name: "retention.ms", Operation: "DELETE"}}})
	if e != nil {
		t.Fatal(e)
	}
	for _, c := range p.After.Entries {
		if c.Name == "retention.ms" && c.Value != nil {
			t.Fatal("built-in default does not establish inherited broker value")
		}
	}
}
func TestResourceHTTPFiltersBeforePagingAndRejectsSecretIntent(t *testing.T) {
	s, owner, id, _ := resourceSetup(t)
	ctx := context.Background()
	viewer := registry.Actor{ID: "topic-viewer", Role: "viewer"}
	_, e := s.Registry.CreateGrant(ctx, owner, id, registry.GrantInput{SubjectType: "user", SubjectID: viewer.ID, Role: "admin", TopicPrefix: "events", GroupPrefix: "group-"})
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Store.Update(ctx, id, func(st *State) error {
		st.ResourceOperations = map[string]ResourceOperation{"visible": {ID: "visible", Plan: ResourcePlan{Input: ResourceIntent{Resource: ResourceTarget{Type: "TOPIC", Name: "events"}}}}, "hidden": {ID: "hidden", Plan: ResourcePlan{Input: ResourceIntent{Resource: ResourceTarget{Type: "TOPIC", Name: "unrelated"}}}}}
		st.ResourceOperations["visible-other"] = ResourceOperation{ID: "visible-other", Plan: ResourcePlan{Input: ResourceIntent{Resource: ResourceTarget{Type: "TOPIC", Name: "events-other"}}}}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	Mount(r.Group("/api/v1"), s, func(*gin.Context) registry.Actor { return viewer })
	base := "/api/v1/kafka/clusters/" + id + "/workspace"
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("GET", base+"/resource-operations?page_size=1", nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"total":2`) || strings.Contains(rec.Body.String(), "unrelated") {
		t.Fatal(rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("GET", base+"/resource-operations?resource_type=TOPIC&resource_name=events&page_size=1", nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"total":1`) || strings.Contains(rec.Body.String(), "visible-other") {
		t.Fatal("filter must precede pagination and count", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("GET", base+"/resource-operations?resource_type=TOPIC&resource_name=unrelated&page_size=1", nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"total":0`) || strings.Contains(rec.Body.String(), `"id":"hidden"`) {
		t.Fatal("resource filtering must preserve authorization", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("GET", base+"/resource-operations?resource_type=BROKER&resource_name=&page_size=1", nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"total":0`) {
		t.Fatal("broker default filter must not include topic records", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("POST", base+"/resource-plans", strings.NewReader(`{"kind":"configs","resource":{"type":"TOPIC","name":"events"},"changes":[{"name":"ssl.keystore.password","operation":"SET","value":"must-not-persist"}]}`)))
	if rec.Code != 400 || strings.Contains(rec.Body.String(), "must-not-persist") {
		t.Fatal(rec.Code, rec.Body.String())
	}
}
func TestResourceReassignProgressIsNotFailure(t *testing.T) {
	s, a, id, f := resourceSetup(t)
	s.LabAutoApprove = true
	ctx := context.Background()
	f.topic.Partitions = append(f.topic.Partitions, kafkaops.Partition{Partition: 1, Replicas: []int32{0}, ISR: []int32{0}})
	op := createResourceTest(t, s, a, id, ResourceIntent{Kind: "reassign", Resource: ResourceTarget{Type: "TOPIC", Name: "events"}, Assignments: []kafkaadmin.ReplicaAssignment{{Partition: 0, Replicas: []int32{1}}, {Partition: 1, Replicas: []int32{1}}}})
	op, e := s.ExecuteResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil {
		t.Fatal(e)
	}
	f.topic.Partitions[0].Replicas = []int32{1}
	f.topic.Partitions[0].ISR = []int32{1}
	f.active = []kafkaadmin.PartitionReassignment{{Partition: 1, Replicas: []int32{0, 1}, AddingReplicas: []int32{1}}}
	op, e = s.RefreshResourceOperation(ctx, a, id, op.ID, op.Revision)
	if e != nil || op.Status != "observing" {
		t.Fatal("normal in-progress reassignment labeled failure", op, e)
	}
}
