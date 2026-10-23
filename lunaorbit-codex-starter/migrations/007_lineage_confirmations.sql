\set ON_ERROR_STOP on
BEGIN;
SELECT pg_advisory_xact_lock(71000);
SELECT NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=7) AS apply_migration \gset
\if :apply_migration
CREATE TABLE lineage_confirmations (
 owner_id text NOT NULL CHECK(owner_id<>''),
 id text NOT NULL CHECK(id<>''),
 scope_id text NOT NULL CHECK(scope_id<>''),
 link_id text NOT NULL CHECK(link_id<>''),
 topic varchar(249) NOT NULL CHECK(topic<>''),
 description varchar(256) NOT NULL DEFAULT '',
 confirmed_by text NOT NULL CHECK(confirmed_by=owner_id),
 confirmed_at timestamptz NOT NULL,
 kind text NOT NULL DEFAULT 'manual_business_confirmation' CHECK(kind='manual_business_confirmation'),
 PRIMARY KEY(owner_id,id),
 UNIQUE(owner_id,scope_id,link_id,topic)
);
INSERT INTO schema_migrations(version) VALUES(7);
\endif
COMMIT;
