\set ON_ERROR_STOP on
BEGIN;
SELECT pg_advisory_xact_lock(71000);
SELECT NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=5) AS apply_migration \gset
\if :apply_migration
CREATE TABLE acl_requests(id text PRIMARY KEY,owner_id text NOT NULL,registration_id text NOT NULL,revision bigint NOT NULL CHECK(revision>0),payload jsonb NOT NULL,CHECK((payload->>'revision')::bigint=revision));
CREATE INDEX acl_requests_owner_scope ON acl_requests(owner_id,registration_id);
CREATE TABLE acl_request_audit(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,request_id text NOT NULL REFERENCES acl_requests(id),actor_id text NOT NULL,action text NOT NULL CHECK(action IN ('acl.request','acl.approve')),revision bigint NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
INSERT INTO schema_migrations(version) VALUES(5);
\endif
COMMIT;
