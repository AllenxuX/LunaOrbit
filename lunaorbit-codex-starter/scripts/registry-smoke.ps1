param([string]$BaseUrl = 'http://127.0.0.1:8080')
$ErrorActionPreference = 'Stop'
$uri = [uri]$BaseUrl
if ($uri.Scheme -ne 'http' -or $uri.Host -notin @('127.0.0.1', 'localhost', '[::1]')) { throw 'Registry smoke only supports local HTTP Mock' }
$first = [Microsoft.PowerShell.Commands.WebRequestSession]::new()
$second = [Microsoft.PowerShell.Commands.WebRequestSession]::new()
function Request([string]$Method, [string]$Path, $Payload, $Session, [int]$Expected) {
    $arguments = @{ Uri = "$BaseUrl/api/v1$Path"; Method = $Method; WebSession = $Session; SkipHttpErrorCheck = $true; Headers = @{ Origin = $BaseUrl }; ContentType = 'application/json' }
    if ($null -ne $Payload) { $arguments.Body = ConvertTo-Json -Depth 8 -Compress $Payload }
    $response = Invoke-WebRequest @arguments
    if ($response.StatusCode -ne $Expected) { throw "Unexpected registry status: $($response.StatusCode), expected $Expected" }
    if ($response.Content) { return $response.Content | ConvertFrom-Json }
}
$session = Request POST '/dev/session' @{} $first 200
if ($session.mode -ne 'mock') { throw 'Refusing non-Mock registry' }
$null = Request POST '/dev/session' @{} $second 200
$password = [guid]::NewGuid().ToString('N')
$cluster = Request POST '/kafka/clusters' @{ display_name = 'Synthetic smoke'; bootstrap_servers = @('broker.synthetic.test:9092'); enabled = $true; security_protocol = 'SASL_SSL'; sasl_mechanism = 'PLAIN'; username = 'synthetic-user'; password = $password; tls_server_name = '' } $first 201
$json = ConvertTo-Json -Depth 8 $cluster
if ($json.Contains($password) -or $json.Contains('synthetic-user') -or -not $cluster.credentials_configured) { throw 'Unsafe credential DTO' }
$id = $cluster.id
$null = Request GET "/kafka/clusters/$id" $null $second 404
$isolated = Request GET '/kafka/clusters' $null $second 200
if ($isolated.total -ne 0) { throw 'Cross-session registry leak' }
$null = Request POST "/kafka/clusters/$id/probe" @{mock_scenario = 'advertised_unreachable'} $first 202
$deadline = (Get-Date).AddSeconds(10)
do {
    $job = Request GET "/kafka/clusters/$id/probe" $null $first 200
    if ($job.status -eq 'done') { break }
    Start-Sleep -Milliseconds 50
} while ((Get-Date) -lt $deadline)
if ($job.status -ne 'done' -or $job.error_code -ne 'AdvertisedBrokerUnreachable') { throw 'Unexpected Mock probe result' }
$null = Request POST "/kafka/clusters/$id/probe" @{mock_scenario = 'success'} $first 429
$cluster = Request GET "/kafka/clusters/$id" $null $first 200
$cluster = Request PATCH "/kafka/clusters/$id" @{revision = $cluster.revision; enabled = $false} $first 200
$null = Request POST "/kafka/clusters/$id/probe" @{} $first 409
$null = Request DELETE "/kafka/clusters/$id" @{revision = $cluster.revision} $first 204
$null = Request GET "/kafka/clusters/$id" $null $first 404
$password = $null
Write-Output 'PASS: Mock session isolation, secret DTO, async probe, rate limit, disable and soft delete'
