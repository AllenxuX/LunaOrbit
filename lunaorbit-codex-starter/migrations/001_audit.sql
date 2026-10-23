\set ON_ERROR_STOP on
BEGIN;
SELECT pg_advisory_xact_lock(71000);
CREATE TABLE IF NOT EXISTS schema_migrations (
    version integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);
SELECT NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 1) AS apply_migration \gset
\if :apply_migration
CREATE TABLE audit_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    actor_id varchar(128) NOT NULL,
    action varchar(128) NOT NULL,
    target_id varchar(256) NOT NULL,
    outcome varchar(16) NOT NULL CHECK (outcome IN ('success', 'failure', 'denied')),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX audit_events_created_at_idx ON audit_events (created_at);
INSERT INTO schema_migrations (version) VALUES (1);
\endif
COMMIT;
