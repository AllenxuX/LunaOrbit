param([string]$BaseUrl = 'http://127.0.0.1:8080')
$ErrorActionPreference = 'Stop'
$uri = [uri]$BaseUrl
if ($uri.Scheme -ne 'http' -or $uri.Host -notin @('127.0.0.1', 'localhost', '[::1]')) { throw 'Discovery smoke only supports local Mock' }
$first = [Microsoft.PowerShell.Commands.WebRequestSession]::new()
$second = [Microsoft.PowerShell.Commands.WebRequestSession]::new()
function Request([string]$Method, [string]$Path, $Payload, $Session, [int]$Expected) {
    $arguments = @{ Uri = "$BaseUrl/api/v1$Path"; Method = $Method; WebSession = $Session; SkipHttpErrorCheck = $true; Headers = @{ Origin = $BaseUrl }; ContentType = 'application/json' }
    if ($null -ne $Payload) { $arguments.Body = ConvertTo-Json -Depth 12 -Compress $Payload }
    $response = Invoke-WebRequest @arguments
    if ($response.StatusCode -ne $Expected) { throw "Unexpected discovery status: $($response.StatusCode), expected $Expected" }
    if ($response.Content) { return $response.Content | ConvertFrom-Json }
}
function Wait-Run([string]$Path, $Session) {
    $deadline = (Get-Date).AddSeconds(10)
    do {
        $job = Request GET "$Path/runs/latest" $null $Session 200
        if ($job.status -eq 'done') { return $job }
        Start-Sleep -Milliseconds 50
    } while ((Get-Date) -lt $deadline)
    throw 'Discovery worker did not complete'
}
$identity = Request POST '/dev/session' @{} $first 200
if ($identity.mode -ne 'mock') { throw 'Refusing non-Mock discovery' }
$null = Request POST '/dev/session' @{} $second 200
$scopes = Request GET '/discovery/scopes' $null $first 200
if ($scopes.items.Count -ne 1 -or -not $scopes.items[0].synthetic) { throw 'Synthetic scope required' }
$path = "/discovery/scopes/$($scopes.items[0].id)"
$input = @{ display_name = 'Synthetic discovery smoke'; bootstrap_servers = @('mars.synthetic.invalid:9092'); enabled = $true; security_protocol = 'PLAINTEXT'; sasl_mechanism = ''; tls_server_name = '' }
$registered = Request POST '/kafka/clusters' $input $first 201
$foreign = Request POST '/kafka/clusters' $input $second 201
$null = Request POST "$path/runs" @{scenario = 'success'} $first 202
$job = Wait-Run $path $first
if ($job.error_code) { throw 'Initial discovery failed' }
$page = Request GET "$path/snapshot?page_size=100" $null $first 200
if ($page.total -ne 3 -or $page.snapshot.stale) { throw 'Invalid initial discovery snapshot' }
$raw = ConvertTo-Json -Depth 20 $page
if ($raw.Contains('DO_NOT_EXPOSE') -or $raw.Contains('jaas')) { throw 'Sensitive discovery data exposed' }
foreach ($link in $page.snapshot.links) { if ($link.verified) { throw 'Declared discovery treated as verified' } }
foreach ($node in $page.snapshot.nodes) { if ($node.kind -eq 'kafka' -and ($null -ne $node.replication_factor -or $node.rf_status -ne 'unknown')) { throw 'RF inferred without metadata' } }
$mars = @($page.snapshot.links | Where-Object source_alias -EQ 'mars')[0]
if ($mars.source.status -ne 'candidate' -or $mars.source.candidate_ids -notcontains $registered.id) { throw 'Kafka Registry candidate missing' }
$binding = @{ namespace = $mars.namespace; workload_uid = $mars.workload_uid; alias = 'mars'; registration_id = $foreign.id }
$null = Request PUT "$path/bindings" $binding $first 404
$binding.registration_id = $registered.id
$confirmed = Request PUT "$path/bindings" $binding $first 200
if ($confirmed.approved_by -ne $identity.user_id) { throw 'Binding approver not authenticated' }
$null = Request GET "$path/snapshot" $null $second 404
$null = Request GET "$path/runs/latest" $null $second 404
Start-Sleep -Milliseconds 2100
$null = Request POST "$path/runs" @{scenario = 'success'} $first 202
$job = Wait-Run $path $first
if ($job.error_code) { throw 'Bound discovery failed' }
$page = Request GET "$path/snapshot?page_size=100" $null $first 200
$mars = @($page.snapshot.links | Where-Object source_alias -EQ 'mars')[0]
if ($mars.source.method -ne 'manual' -or $mars.source.registration_id -ne $registered.id -or $mars.verified) { throw 'Manual binding or verification state invalid' }
$observed = $mars.observed_at
Start-Sleep -Milliseconds 2100
$null = Request POST "$path/runs" @{scenario = 'partial_failure'} $first 202
$job = Wait-Run $path $first
if ($job.error_code -ne 'PARTIAL_COLLECTION') { throw 'Partial failure was hidden' }
$page = Request GET "$path/snapshot?page_size=100" $null $first 200
if ($page.total -ne 3 -or -not $page.snapshot.stale) { throw 'Partial failure removed history' }
$mars = @($page.snapshot.links | Where-Object source_alias -EQ 'mars')[0]
if (-not $mars.stale -or $mars.observed_at -ne $observed) { throw 'Failed collection refreshed old evidence' }
$paged = Request GET "$path/snapshot?page_size=1" $null $first 200
if ($paged.snapshot.links.Count -ne 1 -or $paged.snapshot.edges.Count -ne 2) { throw 'Graph pagination invalid' }
$null = Request DELETE "/kafka/clusters/$($registered.id)" @{revision = $registered.revision} $first 204
$null = Request DELETE "/kafka/clusters/$($foreign.id)" @{revision = $foreign.revision} $second 204
Write-Output 'PASS: MM2 discovery, owner isolation, candidate/manual mapping, stale retention, RF unknown and safe graph pagination'
