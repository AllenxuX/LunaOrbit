\set ON_ERROR_STOP on
BEGIN;
SELECT pg_advisory_xact_lock(71000);
SELECT NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=10) AS apply_migration \gset
\if :apply_migration
-- Manual information records are independent from accounts and ACL applications.
CREATE TABLE IF NOT EXISTS information_applications (
 workspace_id text NOT NULL,
 id text NOT NULL,
 revision bigint NOT NULL CHECK (revision > 0),
 payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
 updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (workspace_id,id)
);
INSERT INTO schema_migrations(version) VALUES(10);
\endif
COMMIT;
