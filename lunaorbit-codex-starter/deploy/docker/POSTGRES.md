# Independent PostgreSQL (REGISTRY_MODE=postgres)

Example manifests: `postgres-release.yaml` (no secrets).

## API settings
- `REGISTRY_MODE=postgres` (only with `PLATFORM_MODE=information`). Registrations, discovery snapshots, bindings,
  topology views and applications are stored in PostgreSQL and survive restarts.
- `DATABASE_URL` (Secret): `postgresql://<user>:<password>@<host>:5432/<db>?sslmode=disable`. Service hostnames and
  normal database names are allowed. The URL must include a host and a database name.
- Startup waits up to 20s for the database, then exits with `registry database unavailable`. It never falls back to
  memory, and neither errors nor logs contain the URL or password.
- `postgres-mock` is unchanged: development only, loopback host and `lunaorbit_phase00_test`/`lunaorbit_phase02_test`.

## Migrations 001-010
- The files use psql meta-commands (`\gset`, `\if`), so they must be run with psql, not by the API.
- An initContainer (`postgres:16-alpine`) runs every file in filename order with `ON_ERROR_STOP=1`. If any file fails,
  the Pod does not start the API.
- Each file checks `schema_migrations` first and skips itself if its version is already recorded, so it is safe to
  run on every Pod start.
- ConfigMap from the same source tree as the images (run in `lunaorbit-codex-starter/`):
  `kubectl -n lunaorbit create configmap lunaorbit-migrations --from-file=migrations/ --dry-run=client -o yaml | kubectl apply -f -`

## Order
1. Secrets `lunaorbit-db` (POSTGRES_PASSWORD, DATABASE_URL) and `lunaorbit-kite` (KITE_API_KEY).
2. ConfigMaps `lunaorbit-migrations` and `lunaorbit-env`.
3. PostgreSQL StatefulSet + Service (10Gi PVC); wait until ready.
4. LunaOrbit Deployment (initContainer migrates, then api + web start).
