package access

import (
	"context"
	"encoding/json"
	"errors"
	"lunaorbit/backend/internal/registry"
	"os"
	"testing"
	"time"
)

func TestPostgresTransactionsOptIn(t *testing.T) {
	dsn := os.Getenv("LUNAORBIT_ACL_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("LUNAORBIT_ACL_TEST_DATABASE_URL not configured")
	}
	db, e := registry.OpenDevelopmentDatabase(dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, e = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS acl_requests(id text PRIMARY KEY,owner_id text NOT NULL,registration_id text NOT NULL,revision bigint NOT NULL CHECK(revision>0),payload jsonb NOT NULL,CHECK((payload->>'revision')::bigint=revision)); CREATE TABLE IF NOT EXISTS acl_request_audit(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,request_id text NOT NULL REFERENCES acl_requests(id),actor_id text NOT NULL,action text NOT NULL CHECK(action IN ('acl.request','acl.approve')),revision bigint NOT NULL,created_at timestamptz NOT NULL DEFAULT now())`); e != nil {
		t.Fatal("ACL fixture schema unavailable")
	}
	s := NewPostgresStore(db)
	r := Request{ID: "acl-test-" + time.Now().Format("150405.000000000"), OwnerID: "synthetic-owner", RegistrationID: "synthetic-registration", Revision: 1, Applicant: "synthetic-owner", Rule: rule(), Status: "pending_approval", CreatedAt: time.Now().UTC()}
	defer func() {
		db.ExecContext(context.Background(), "DELETE FROM acl_request_audit WHERE request_id=$1", r.ID)
		db.ExecContext(context.Background(), "DELETE FROM acl_requests WHERE id=$1", r.ID)
	}()
	if e = s.Save(ctx, r, 0, r.Applicant, "invalid-audit-action"); !errors.Is(e, ErrStorage) {
		t.Fatal("audit failure expected")
	}
	if _, e = s.Get(ctx, r.ID); !errors.Is(e, ErrNotFound) {
		t.Fatal("audit failure committed request")
	}
	if e = s.Save(ctx, r, 0, r.Applicant, "acl.request"); e != nil {
		t.Fatal(e)
	}
	v, e := s.Get(ctx, r.ID)
	if e != nil || v.OwnerID != r.OwnerID {
		t.Fatal("owner persistence")
	}
	assertImmutableRequest(t, s, r)
	r.Revision = 2
	r.Status = "approved_not_executed"
	if e = s.Save(ctx, r, 1, "synthetic-approver", "acl.approve"); e != nil {
		t.Fatal(e)
	}
	if e = s.Save(ctx, r, 1, "synthetic-approver", "acl.approve"); !errors.Is(e, ErrConflict) {
		t.Fatal("CAS missing")
	}
}

func TestPostgresInboxFloodOptIn(t *testing.T) {
	dsn := os.Getenv("LUNAORBIT_ACL_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("LUNAORBIT_ACL_TEST_DATABASE_URL not configured")
	}
	db, e := registry.OpenDevelopmentDatabase(dsn)
	if e != nil {
		t.Fatal("dedicated ACL database unavailable")
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	prefix := "acl-inbox-flood-" + time.Now().Format("150405.000000000")
	defer db.ExecContext(context.Background(), "DELETE FROM acl_requests WHERE owner_id=$1", prefix)
	ruleJSON, _ := json.Marshal(rule())
	_, e = db.ExecContext(ctx, `INSERT INTO acl_requests(id,owner_id,registration_id,revision,payload) SELECT $1||'-'||lpad(n::text,4,'0'),$1,CASE WHEN n>=1001 THEN $2 ELSE $3 END,1,jsonb_build_object('id',$1||'-'||lpad(n::text,4,'0'),'source_registration_id',CASE WHEN n>=1001 THEN $2 ELSE $3 END,'applicant',$1,'revision',1,'rule',$4::jsonb,'status','pending_approval','created_at',now()+n*interval '1 second') FROM generate_series(0,1003) n`, prefix, prefix+"-authorized", prefix+"-outside", string(ruleJSON))
	if e != nil {
		t.Fatal("dedicated inbox fixture unavailable")
	}
	store := NewPostgresStore(db)
	pending, e := store.Pending(ctx)
	if e != nil || len(pending) < 1004 {
		t.Fatal("pending truncated before authorization")
	}
	service := NewService(nil, store)
	service.ApprovalAuthorizer = StaticApprovals{"approver": []string{prefix + "-authorized"}}
	rows, e := service.Inbox(ctx, registry.Actor{ID: "approver", Role: "admin"})
	if e != nil || len(rows) != 3 {
		t.Fatal("authorized inbox starved")
	}
	for i, row := range rows {
		if row.RegistrationID != prefix+"-authorized" || i > 0 && rows[i-1].CreatedAt.Before(row.CreatedAt) {
			t.Fatal("inbox isolation or stable ordering failed")
		}
	}
}
