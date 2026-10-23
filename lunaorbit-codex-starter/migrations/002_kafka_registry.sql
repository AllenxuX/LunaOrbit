\set ON_ERROR_STOP on
BEGIN;
SELECT pg_advisory_xact_lock(71000);
SELECT NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 2) AS apply_migration \gset
\if :apply_migration
-- payload is the complete, authoritative server-side Registration snapshot.
-- The typed tables below are transactionally maintained projections, never secret values.
CREATE TABLE kafka_cluster_registrations (
 id text PRIMARY KEY CHECK (id <> ''),
 created_by text NOT NULL CHECK (created_by <> ''),
 revision bigint NOT NULL CHECK (revision > 0),
 deleted_at timestamptz,
 payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
 CHECK (payload->>'ID' = id),
 CHECK (payload->>'CreatedBy' = created_by),
 CHECK ((payload->>'Revision')::bigint = revision)
);
CREATE INDEX kafka_registrations_owner_idx ON kafka_cluster_registrations(created_by,id) WHERE deleted_at IS NULL;
CREATE TABLE kafka_cluster_credentials (
 cluster_registration_id text PRIMARY KEY REFERENCES kafka_cluster_registrations(id),
 security_protocol text NOT NULL CHECK (security_protocol IN ('PLAINTEXT','SSL','SASL_PLAINTEXT','SASL_SSL')),
 sasl_mechanism text NOT NULL CHECK (sasl_mechanism IN ('','PLAIN','SCRAM-SHA-256','SCRAM-SHA-512')),
 username_masked text NOT NULL,
 username_ref text NOT NULL, password_ref text NOT NULL,
 tls_ca_ref text NOT NULL, tls_cert_ref text NOT NULL, tls_key_ref text NOT NULL,
 tls_server_name text NOT NULL,
 revision bigint NOT NULL CHECK(revision >= 0)
);
CREATE TABLE kafka_cluster_capabilities (
 cluster_registration_id text NOT NULL REFERENCES kafka_cluster_registrations(id),
 feature text NOT NULL, supported boolean NOT NULL, reason text NOT NULL,
 PRIMARY KEY(cluster_registration_id,feature)
);
CREATE TABLE kafka_cluster_probe_status (
 cluster_registration_id text PRIMARY KEY REFERENCES kafka_cluster_registrations(id),
 status text NOT NULL, last_probe_at timestamptz, last_probe_error_code text NOT NULL,
 discovered_cluster_id text NOT NULL,
 brokers jsonb NOT NULL CHECK (jsonb_typeof(brokers)='array')
);
INSERT INTO schema_migrations(version) VALUES(2);
\endif
COMMIT;
