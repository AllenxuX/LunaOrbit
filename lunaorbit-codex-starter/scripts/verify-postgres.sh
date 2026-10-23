#!/usr/bin/env bash
# Phase 00 only. Requires a running dedicated laboratory Compose database.
set -euo pipefail
cd "$(dirname "$0")/.."
compose() { docker compose -p "${COMPOSE_PROJECT_NAME:-lunaorbit_phase00}" "$@"; }

# Refuse to run database verification against a business database.
compose exec -T postgres sh -c '
  case "$POSTGRES_DB" in lunaorbit_phase00_*) ;; *) echo "Dedicated Phase 00 laboratory database required" >&2; exit 1;; esac
'
migrate() {
  compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < migrations/001_audit.sql
}
migrate
migrate
migrate & first=$!
migrate & second=$!
wait "$first"
wait "$second"

compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
DO $$
BEGIN
  IF (SELECT count(*) FROM schema_migrations) <> 1
     OR NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 1) THEN
    RAISE EXCEPTION 'migration version assertion failed';
  END IF;
  IF to_regclass('public.audit_events') IS NULL
     OR to_regclass('public.audit_events_created_at_idx') IS NULL THEN
    RAISE EXCEPTION 'audit schema assertion failed';
  END IF;
END $$;
SELECT 'PASS: repeated/concurrent migration and audit schema' AS verification;
SQL
