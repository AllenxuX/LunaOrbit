package registry

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestExplicitSecretClearAndProtocolRejection(t *testing.T) {
	secrets := NewMemorySecrets()
	s := NewService(NewMemoryStore(), secrets)
	defer s.Close()
	actor := Actor{ID: "owner", Role: "admin"}
	row, e := s.Create(context.Background(), actor, syntheticInput())
	if e != nil {
		t.Fatal(e)
	}
	empty := ""
	row, e = s.ReplaceCredentials(context.Background(), actor, row.ID, CredentialUpdate{Revision: row.Revision, CredentialInput: CredentialInput{SecurityProtocol: "SASL_SSL", SASLMechanism: "PLAIN", Password: &empty}})
	if e != nil || row.CredentialsConfigured {
		t.Fatal("explicit clear failed")
	}
	if _, e = s.StartProbe(context.Background(), actor, row.ID, ""); ErrorCode(e) != "SECRET_UNAVAILABLE" {
		t.Fatal("missing secret probe accepted")
	}
	before := len(secrets.values)
	input := syntheticInput()
	input.SecurityProtocol = "PLAINTEXT"
	input.SASLMechanism = ""
	if _, e = s.Create(context.Background(), actor, input); ErrorCode(e) != "INVALID_CREDENTIALS" || len(secrets.values) != before {
		t.Fatal("inapplicable secrets retained")
	}
}

func TestSignedDevelopmentSessionCanResumeAndRejectTampering(t *testing.T) {
	key := []byte(strings.Repeat("k", 32))
	id := randomID()
	token := signedSession(id, time.Now().Add(time.Hour), key)
	actor, ok := sessionActor(token, key)
	if !ok || actor.ID != id {
		t.Fatal("valid signed session rejected")
	}
	if _, ok = sessionActor(token+"tampered", key); ok {
		t.Fatal("tampered session accepted")
	}
	if _, ok = sessionActor(token, []byte(strings.Repeat("q", 32))); ok {
		t.Fatal("wrong key accepted")
	}
	if _, ok = sessionActor(signedSession(id, time.Now().Add(-time.Second), key), key); ok {
		t.Fatal("expired session accepted")
	}
}

func TestPerUserProbeBudgetAcrossRegistrations(t *testing.T) {
	s := NewService(NewMemoryStore(), NewMemorySecrets())
	defer s.Close()
	actor := Actor{ID: "owner", Role: "admin"}
	for i := 0; i < 6; i++ {
		row, e := s.Create(context.Background(), actor, syntheticInput())
		if e != nil {
			t.Fatal(e)
		}
		_, e = s.StartProbe(context.Background(), actor, row.ID, "")
		if i == 5 {
			if ErrorCode(e) != "RATE_LIMITED" {
				t.Fatal("per-user probe budget missing")
			}
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		for deadline := time.Now().Add(time.Second); ; {
			job, e := s.ProbeStatus(context.Background(), actor, row.ID)
			if e != nil {
				t.Fatal(e)
			}
			if job.Status == "done" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("probe did not complete")
			}
			time.Sleep(time.Millisecond)
		}
	}
}

func TestDevelopmentDatabaseRejectsBusinessAndOverrides(t *testing.T) {
	for _, dsn := range []string{"postgres://127.0.0.1/lunaorbit_prod?sslmode=disable", "postgres://192.0.2.1/lunaorbit_phase02_test?sslmode=disable", "postgres://127.0.0.1/lunaorbit_phase02_test?host=remote.example", "postgres://127.0.0.1/lunaorbit_phase02_test?dbname=production"} {
		if db, e := OpenDevelopmentDatabase(dsn); e == nil {
			db.Close()
			t.Fatal("unsafe development database accepted")
		}
	}
}
