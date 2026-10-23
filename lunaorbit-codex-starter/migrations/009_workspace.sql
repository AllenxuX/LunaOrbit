\set ON_ERROR_STOP on
BEGIN;
SELECT pg_advisory_xact_lock(71000);
SELECT NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=9) AS apply_migration \gset
\if :apply_migration
CREATE TABLE IF NOT EXISTS kafka_workspaces (
 registration_id text PRIMARY KEY,
 body jsonb NOT NULL,
 revision bigint NOT NULL DEFAULT 1,
 updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO schema_migrations(version) VALUES(9);
\endif
COMMIT;
