package changes

import (
	"bytes"
	"context"
	"errors"
	"lunaorbit/backend/internal/registry"
	"os"
	"testing"
	"time"
)

func TestPostgresChangeTransactionsOptIn(t *testing.T) {
	dsn := os.Getenv("LUNAORBIT_CHANGES_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("LUNAORBIT_CHANGES_TEST_DATABASE_URL not configured")
	}
	db, e := registry.OpenDevelopmentDatabase(dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, e = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS mm2_changes(id text PRIMARY KEY,owner_id text NOT NULL,target_id text NOT NULL,revision bigint NOT NULL CHECK(revision>0),approval_hash text NOT NULL,config_uid text NOT NULL,payload jsonb NOT NULL,CHECK((payload->>'revision')::bigint=revision)); CREATE TABLE IF NOT EXISTS mm2_change_snapshots(request_id text PRIMARY KEY REFERENCES mm2_changes(id),ciphertext bytea NOT NULL CHECK(octet_length(ciphertext)<=1048576)); CREATE TABLE IF NOT EXISTS mm2_change_audit(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,request_id text NOT NULL REFERENCES mm2_changes(id),actor_id text NOT NULL,action text NOT NULL CHECK(action IN ('change.request','change.approve','change.execute.start','change.backup','change.execute.finish')),revision bigint NOT NULL,created_at timestamptz NOT NULL DEFAULT now())`); e != nil {
		t.Fatal("fixture schema unavailable")
	}
	store := NewPostgresStore(db)
	r := Request{ID: "changes-test-" + time.Now().Format("150405.000000000"), OwnerID: "synthetic-owner", Applicant: "synthetic-owner", TargetID: "synthetic-target", ConfigMapUID: "synthetic-cm", WorkloadUID: "synthetic-deployment", Revision: 1, Status: "pending_approval", TopicRule: "orders.*", ExpectedResourceVersion: "1"}
	defer func() {
		db.ExecContext(context.Background(), "DELETE FROM mm2_change_audit WHERE request_id=$1", r.ID)
		db.ExecContext(context.Background(), "DELETE FROM mm2_change_snapshots WHERE request_id=$1", r.ID)
		db.ExecContext(context.Background(), "DELETE FROM mm2_changes WHERE id=$1", r.ID)
	}()
	if e = store.Save(ctx, r, 0, r.OwnerID, "invalid-action", nil); !errors.Is(e, ErrStorage) {
		t.Fatal(e)
	}
	if _, e = store.Get(ctx, r.ID); !errors.Is(e, ErrNotFound) {
		t.Fatal("audit failure did not rollback insert")
	}
	if e = store.Save(ctx, r, 0, r.OwnerID, "change.request", nil); e != nil {
		t.Fatal(e)
	}
	r.Revision = 2
	r.Approver = "synthetic-approver"
	r.ApprovalHash = digest(r)
	r.Status = "approved_not_executed"
	if e = store.Save(ctx, r, 1, r.Approver, "change.approve", nil); e != nil {
		t.Fatal(e)
	}
	read, e := store.Get(ctx, r.ID)
	if e != nil || read.OwnerID != r.OwnerID || read.ApprovalHash != r.ApprovalHash || read.ConfigMapUID != r.ConfigMapUID {
		t.Fatal("internal state not retained", e)
	}
	v, _ := NewVault(bytes.Repeat([]byte{3}, 32))
	b, _ := v.Seal([]byte("DO_NOT_EXPOSE"), aad(r))
	r.Revision = 3
	if e = store.Save(ctx, r, 2, r.OwnerID, "invalid-action", b); !errors.Is(e, ErrStorage) {
		t.Fatal(e)
	}
	if _, e = store.Backup(ctx, r.ID); !errors.Is(e, ErrNotFound) {
		t.Fatal("audit failure committed ciphertext")
	}
	if e = store.Save(ctx, r, 2, r.OwnerID, "change.backup", b); e != nil {
		t.Fatal(e)
	}
	if e = store.Save(ctx, r, 2, r.OwnerID, "change.backup", b); !errors.Is(e, ErrConflict) {
		t.Fatal("CAS ignored")
	}
	cipher, e := store.Backup(ctx, r.ID)
	if e != nil || !bytes.Equal(cipher, b) {
		t.Fatal("ciphertext mismatch")
	}
}
