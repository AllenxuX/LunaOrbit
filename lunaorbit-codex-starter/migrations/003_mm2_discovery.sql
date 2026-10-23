\set ON_ERROR_STOP on
BEGIN;
SELECT pg_advisory_xact_lock(71000);
SELECT NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 3) AS apply_migration \gset
\if :apply_migration
-- Only allowlisted discovery semantics and evidence; never raw ConfigMap data.
CREATE TABLE mm2_discovery_snapshots (
 owner_id text NOT NULL CHECK(owner_id <> ''),
 scope_id text NOT NULL CHECK(scope_id <> ''),
 observed_at timestamptz NOT NULL,
 attempted_at timestamptz NOT NULL,
 payload jsonb NOT NULL CHECK(jsonb_typeof(payload)='object'),
 PRIMARY KEY(owner_id,scope_id),
 CHECK(payload->>'scope_id'=scope_id)
);
CREATE TABLE mm2_cluster_alias_bindings (
 owner_id text NOT NULL CHECK(owner_id <> ''),
 scope_id text NOT NULL CHECK(scope_id <> ''),
 namespace text NOT NULL CHECK(namespace <> ''),
 workload_uid text NOT NULL CHECK(workload_uid <> ''),
 alias text NOT NULL CHECK(alias <> ''),
 registration_id text NOT NULL REFERENCES kafka_cluster_registrations(id),
 approved_by text NOT NULL,
 approved_at timestamptz NOT NULL,
 PRIMARY KEY(owner_id,scope_id,namespace,workload_uid,alias),
 CHECK(approved_by=owner_id)
);
INSERT INTO schema_migrations(version) VALUES(3);
\endif
COMMIT;
