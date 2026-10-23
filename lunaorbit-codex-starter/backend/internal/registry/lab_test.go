package registry

import (
	"context"
	"lunaorbit/backend/internal/kafkaadmin"
	"testing"
	"time"
)

func TestSuccessfulProbeStatusReflectsServerOwnedSource(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(map[bool]string{false: "mock", true: "native"}[native], func(t *testing.T) {
			s := NewService(NewMemoryStore(), NewMemorySecrets())
			defer s.Close()
			if native {
				policy, e := kafkaadmin.NewNetworkPolicy(kafkaadmin.NetworkPolicyConfig{})
				if e != nil {
					t.Fatal(e)
				}
				if e = s.UseReadOnlyKafka(policy); e != nil {
					t.Fatal(e)
				}
			}
			// Protocol I/O is covered in kafkaadmin; this fixture isolates persistence.
			s.GatewayFactory = func(r Registration, _ string) KafkaAdminGateway { return mockGateway{row: r, scenario: "success"} }
			actor := Actor{ID: "synthetic-owner", Role: "admin"}
			row, e := s.Create(context.Background(), actor, syntheticInput())
			if e != nil {
				t.Fatal(e)
			}
			job, e := s.StartProbe(context.Background(), actor, row.ID, "success")
			if e != nil {
				t.Fatal(e)
			}
			deadline := time.Now().Add(time.Second)
			for job.Status != "done" && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
				job, e = s.ProbeStatus(context.Background(), actor, row.ID)
				if e != nil {
					t.Fatal(e)
				}
			}
			if job.Status != "done" || job.ErrorCode != "" {
				t.Fatal("probe did not complete")
			}
			current, e := s.Get(context.Background(), actor, row.ID)
			if e != nil {
				t.Fatal(e)
			}
			want := "mock_connected"
			if native {
				want = "connected"
			}
			if current.Status != want {
				t.Fatalf("got %s want %s", current.Status, want)
			}
		})
	}
}
