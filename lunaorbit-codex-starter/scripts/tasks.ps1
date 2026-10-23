param([Parameter(Mandatory)][ValidateSet('backend', 'frontend', 'db', 'migrate', 'test', 'typecheck', 'lint', 'build', 'smoke', 'kite-mock', 'registry-backend', 'registry-smoke', 'discovery-backend', 'information-backend', 'discovery-mock', 'discovery-smoke', 'topology-e2e')][string]$Task)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
function Run-In([string]$Directory, [scriptblock]$Command) {
    Push-Location (Join-Path $root $Directory)
    try { & $Command; if ($LASTEXITCODE -ne 0) { throw "Task failed: $Task" } }
    finally { Pop-Location }
}
switch ($Task) {
    backend { Run-In backend { go run ./cmd/server } }
    information-backend {
        $informationNames = @('PLATFORM_MODE','INFORMATION_WORKSPACE_ID','REGISTRY_MODE','DISCOVERY_MODE','CHANGES_MODE','OBSERVABILITY_MODE','ENABLE_KITE_WRITES','ENABLE_KAFKA_WRITES')
        $informationPrevious = @{}
        foreach ($name in $informationNames) { $informationPrevious[$name] = [Environment]::GetEnvironmentVariable($name) }
        try {
            $env:PLATFORM_MODE = 'information'
            if (-not $env:INFORMATION_WORKSPACE_ID) { $env:INFORMATION_WORKSPACE_ID = 'local-information' }
            if (-not $env:REGISTRY_MODE -or $env:REGISTRY_MODE -eq 'disabled') { $env:REGISTRY_MODE = 'mock' }
            if (-not $env:DISCOVERY_MODE -or $env:DISCOVERY_MODE -eq 'disabled') { $env:DISCOVERY_MODE = 'mock' }
            $env:CHANGES_MODE = 'disabled'; $env:OBSERVABILITY_MODE = 'disabled'
            $env:ENABLE_KITE_WRITES = 'false'; $env:ENABLE_KAFKA_WRITES = 'false'
            Run-In backend { go run ./cmd/server }
        } finally { foreach ($name in $informationNames) { [Environment]::SetEnvironmentVariable($name,$informationPrevious[$name]) } }
    }
    registry-backend {
        $previousRegistryMode = $env:REGISTRY_MODE
        try { $env:REGISTRY_MODE = 'mock'; Run-In backend { go run ./cmd/server } }
        finally { $env:REGISTRY_MODE = $previousRegistryMode }
    }
    discovery-backend {
        $previousRegistryMode = $env:REGISTRY_MODE
        $previousDiscoveryMode = $env:DISCOVERY_MODE
        $previousChangesMode = $env:CHANGES_MODE
        $previousObservationMode = $env:OBSERVABILITY_MODE
        try { $env:REGISTRY_MODE = 'mock'; $env:DISCOVERY_MODE = 'mock'; $env:CHANGES_MODE = 'mock'; $env:OBSERVABILITY_MODE = 'mock'; Run-In backend { go run ./cmd/server } }
        finally { $env:REGISTRY_MODE = $previousRegistryMode; $env:DISCOVERY_MODE = $previousDiscoveryMode; $env:CHANGES_MODE = $previousChangesMode; $env:OBSERVABILITY_MODE = $previousObservationMode }
    }
    discovery-mock { Run-In backend { go run ./cmd/mm2discover } }
    discovery-smoke { & "$PSScriptRoot/discovery-smoke.ps1" }
    registry-smoke { & "$PSScriptRoot/registry-smoke.ps1" }
    topology-e2e { Run-In frontend { pnpm test:e2e } }
    frontend { Run-In frontend { pnpm dev } }
    kite-mock { Run-In backend { go run ./cmd/kitecheck } }
    db { Run-In . { docker compose up -d --wait postgres } }
    migrate { & "$PSScriptRoot/migrate.ps1" }
    test {
        Run-In backend { go test ./...; if ($LASTEXITCODE -ne 0) { throw 'Go tests failed' }; go vet ./... }
        Run-In frontend { pnpm test }
    }
    typecheck { Run-In frontend { pnpm typecheck } }
    lint { Run-In frontend { pnpm lint } }
    build {
        Run-In backend { go build -o bin/lunaorbit.exe ./cmd/server; if ($LASTEXITCODE -ne 0) { throw 'Server build failed' }; go build -o bin/kitecheck.exe ./cmd/kitecheck }
        Run-In frontend { pnpm build }
    }
    smoke { & "$PSScriptRoot/smoke.ps1" }
}
