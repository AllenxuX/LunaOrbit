param([string]$BaseUrl = 'http://127.0.0.1:8080')
$ErrorActionPreference = 'Stop'
$uri = [uri]$BaseUrl
if ($uri.Host -notin @('127.0.0.1', 'localhost', '[::1]') -or $uri.Scheme -ne 'http') { throw 'Smoke test only supports local HTTP' }
$health = Invoke-WebRequest "$BaseUrl/healthz"
if ($health.StatusCode -ne 200 -or ($health.Content | ConvertFrom-Json).status -ne 'ok' -or -not $health.Headers['X-Request-ID']) { throw 'Invalid health response' }
$missing = Invoke-WebRequest "$BaseUrl/api/v1/not-implemented" -SkipHttpErrorCheck
if ($missing.StatusCode -ne 404 -or ($missing.Content | ConvertFrom-Json).error.code -ne 'NOT_FOUND') { throw 'Invalid error envelope' }
Write-Output 'PASS: local /healthz and unified 404'
