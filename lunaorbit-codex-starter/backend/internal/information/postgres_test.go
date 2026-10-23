package information

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"lunaorbit/backend/internal/registry"
	"os"
	"testing"
	"time"
)

func TestPostgresPersistence(t *testing.T) {
	dsn := os.Getenv("INFORMATION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("INFORMATION_TEST_DATABASE_URL unset")
	}
	db, err := registry.OpenDevelopmentDatabase(dsn)
	if err != nil {
		t.Fatal("dedicated local database unavailable")
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ddl := `CREATE TABLE IF NOT EXISTS information_applications(workspace_id text NOT NULL,id text NOT NULL,revision bigint NOT NULL CHECK(revision>0),payload jsonb NOT NULL CHECK(jsonb_typeof(payload)='object'),updated_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(workspace_id,id))`
	for i := 0; i < 2; i++ {
		if _, err = db.ExecContext(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	w := "information-test-" + uuid.NewString()
	defer db.ExecContext(context.Background(), `DELETE FROM information_applications WHERE workspace_id=$1`, w)
	s := NewPostgresStore(db)
	a, err := s.Create(ctx, w, Application{ID: uuid.NewString(), Name: "synthetic", Location: "synthetic-service", Relations: []Relation{{ClusterID: "synthetic-cluster", Topic: "events", Mode: "produce", Account: "synthetic-user", ClientID: "synthetic-client"}}})
	if err != nil {
		t.Fatal(err)
	}
	if a.UpdatedAt.IsZero() {
		t.Fatal("create omitted timestamp")
	}
	// Older JSON payloads have no timestamp; the database column is authoritative.
	if _, err = db.ExecContext(ctx, `UPDATE information_applications SET payload=payload - 'updated_at' WHERE workspace_id=$1 AND id=$2`, w, a.ID); err != nil {
		t.Fatal(err)
	}
	s = NewPostgresStore(db)
	p, err := s.List(ctx, w, 1, 20, "")
	if err != nil || p.Total != 1 || p.Items[0].ID != a.ID || p.Items[0].Location != a.Location || len(p.Items[0].Relations) != 1 || p.Items[0].Relations[0].Account != "synthetic-user" || p.Items[0].Relations[0].ClientID != "synthetic-client" || !p.Items[0].UpdatedAt.Equal(a.UpdatedAt) {
		t.Fatal(p, err)
	}
	updated, err := s.Update(ctx, w, a.ID, a)
	if err != nil || updated.Revision != 2 || !updated.UpdatedAt.After(a.UpdatedAt) {
		t.Fatal(updated, err)
	}
	if _, err = s.Update(ctx, w, a.ID, a); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	page, listErr := s.List(ctx, w, 1, 20, "")
	if listErr != nil || !page.Items[0].UpdatedAt.Equal(updated.UpdatedAt) {
		t.Fatal("stale update changed timestamp", page, listErr)
	}
	if err = s.Delete(ctx, w, a.ID, 1); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if err = s.Delete(ctx, w, a.ID, 2); err != nil {
		t.Fatal(err)
	}
}
