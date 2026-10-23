\set ON_ERROR_STOP on
BEGIN;
SELECT pg_advisory_xact_lock(71000);
SELECT NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=8) AS apply_migration \gset
\if :apply_migration
CREATE TABLE IF NOT EXISTS kafka_cluster_grants (
 id text PRIMARY KEY,
 cluster_id text NOT NULL REFERENCES kafka_cluster_registrations(id),
 revision bigint NOT NULL CHECK(revision > 0),
 revoked_at timestamptz,
 payload jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS kafka_cluster_grants_cluster_idx ON kafka_cluster_grants(cluster_id) WHERE revoked_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS kafka_cluster_grants_active_identity_idx
 ON kafka_cluster_grants(cluster_id, (payload->>'subject_type'), (payload->>'subject_id'), (payload->>'role'), (COALESCE(payload->>'topic_prefix','')), (COALESCE(payload->>'group_prefix','')))
 WHERE revoked_at IS NULL;
INSERT INTO schema_migrations(version) VALUES(8);
\endif
COMMIT;
