\set ON_ERROR_STOP on
BEGIN;
SELECT pg_advisory_xact_lock(71000);
SELECT NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=6) AS apply_migration \gset
\if :apply_migration
CREATE TABLE mm2_changes(id text PRIMARY KEY,owner_id text NOT NULL,target_id text NOT NULL,revision bigint NOT NULL CHECK(revision>0),approval_hash text NOT NULL,config_uid text NOT NULL,payload jsonb NOT NULL,CHECK((payload->>'revision')::bigint=revision));
CREATE INDEX mm2_changes_owner_target ON mm2_changes(owner_id,target_id);
CREATE TABLE mm2_change_snapshots(request_id text PRIMARY KEY REFERENCES mm2_changes(id),ciphertext bytea NOT NULL CHECK(octet_length(ciphertext)<=1048576));
CREATE TABLE mm2_change_audit(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,request_id text NOT NULL REFERENCES mm2_changes(id),actor_id text NOT NULL,action text NOT NULL CHECK(action IN ('change.request','change.approve','change.execute.start','change.backup','change.execute.finish')),revision bigint NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
INSERT INTO schema_migrations(version) VALUES(6);
\endif
COMMIT;
