$ErrorActionPreference = 'Stop'
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    foreach ($migration in (Get-ChildItem -LiteralPath migrations -Filter '*.sql' | Sort-Object Name)) {
        Get-Content -LiteralPath $migration.FullName -Raw | docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
        if ($LASTEXITCODE -ne 0) { throw "Migration failed: $($migration.Name)" }
    }
} finally { Pop-Location }
