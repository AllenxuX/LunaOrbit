\set ON_ERROR_STOP on
BEGIN;
SELECT pg_advisory_xact_lock(71000);
SELECT NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=4) AS apply_migration \gset
\if :apply_migration
CREATE TABLE topology_views (
 owner_id text NOT NULL CHECK(owner_id<>''),
 scope_id text NOT NULL CHECK(scope_id<>''),
 revision bigint NOT NULL CHECK(revision>0),
 payload jsonb NOT NULL CHECK(jsonb_typeof(payload)='object'),
 PRIMARY KEY(owner_id,scope_id),
 CHECK((payload->>'revision')::bigint=revision)
);
INSERT INTO schema_migrations(version) VALUES(4);
\endif
COMMIT;
