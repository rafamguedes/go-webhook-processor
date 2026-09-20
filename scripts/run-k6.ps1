param(
    [ValidateSet('smoke', 'load', 'stress', 'spike', 'soak', 'custom')]
    [string]$Profile = 'smoke',

    [ValidateSet('Auto', 'Local', 'Docker')]
    [string]$Engine = 'Auto',

    [string]$BaseUrl,
    [int]$Rate = 25,
    [string]$Duration = '5m',
    [int]$PreAllocatedVUs = 50,
    [int]$MaxVUs = 500,
    [string]$RequestTimeout = '30s',
    [double]$MinAcceptedRate = 0.99,
    [double]$MaxFailedRate = 0.01,
    [int]$P95Milliseconds = 1000,
    [int]$P99Milliseconds = 2000,
    [string]$ReportDirectory = 'reports\k6'
)

$ErrorActionPreference = 'Stop'

if ($Rate -le 0 -or $PreAllocatedVUs -le 0 -or $MaxVUs -le 0) {
    throw 'Rate, PreAllocatedVUs, and MaxVUs must be greater than zero.'
}
if ($MaxVUs -lt $PreAllocatedVUs) {
    throw 'MaxVUs must be greater than or equal to PreAllocatedVUs.'
}
if ($MinAcceptedRate -lt 0 -or $MinAcceptedRate -gt 1 -or $MaxFailedRate -lt 0 -or $MaxFailedRate -gt 1) {
    throw 'MinAcceptedRate and MaxFailedRate must be between 0 and 1.'
}
if ($P95Milliseconds -le 0 -or $P99Milliseconds -le 0) {
    throw 'Latency thresholds must be greater than zero.'
}

$projectRoot = Split-Path -Parent $PSScriptRoot
$testScript = Join-Path $projectRoot 'tests\load\webhook-load.js'
$reportRoot = if ([System.IO.Path]::IsPathRooted($ReportDirectory)) {
    $ReportDirectory
}
else {
    Join-Path $projectRoot $ReportDirectory
}

if (-not (Test-Path -LiteralPath $testScript)) {
    throw "k6 test script not found: $testScript"
}

New-Item -ItemType Directory -Path $reportRoot -Force | Out-Null

$timestamp = [DateTimeOffset]::Now.ToString('yyyyMMdd-HHmmss')
$runId = "k6-$Profile-$timestamp-$([Guid]::NewGuid().ToString('N').Substring(0, 8))"
$reportBaseName = "$runId-report"
$htmlReport = Join-Path $reportRoot "$reportBaseName.html"
$jsonReport = Join-Path $reportRoot "$reportBaseName.json"

$localK6 = Get-Command k6 -ErrorAction SilentlyContinue
if ($Engine -eq 'Auto') {
    $Engine = if ($null -ne $localK6) { 'Local' } else { 'Docker' }
}
if ($Engine -eq 'Local' -and $null -eq $localK6) {
    throw "k6 is not installed or is not available in PATH. Use -Engine Docker or install k6."
}
if ($Engine -eq 'Docker' -and $null -eq (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw 'Docker is not installed or is not available in PATH.'
}

if ([string]::IsNullOrWhiteSpace($BaseUrl)) {
    $BaseUrl = if ($Engine -eq 'Docker') { 'http://host.docker.internal:8080' } else { 'http://localhost:8080' }
}

$scriptEnvironment = [ordered]@{
    PROFILE = $Profile
    BASE_URL = $BaseUrl.TrimEnd('/')
    TEST_RUN_ID = $runId
    REQUEST_TIMEOUT = $RequestTimeout
    MIN_ACCEPTED_RATE = $MinAcceptedRate.ToString([System.Globalization.CultureInfo]::InvariantCulture)
    MAX_FAILED_RATE = $MaxFailedRate.ToString([System.Globalization.CultureInfo]::InvariantCulture)
    P95_MS = $P95Milliseconds.ToString([System.Globalization.CultureInfo]::InvariantCulture)
    P99_MS = $P99Milliseconds.ToString([System.Globalization.CultureInfo]::InvariantCulture)
}

if ($Profile -eq 'custom') {
    $scriptEnvironment.RATE = $Rate
    $scriptEnvironment.DURATION = $Duration
    $scriptEnvironment.PRE_ALLOCATED_VUS = $PreAllocatedVUs
    $scriptEnvironment.MAX_VUS = $MaxVUs
}

Write-Host ("Starting k6 run {0} with profile '{1}' using {2}." -f $runId, $Profile, $Engine)
Write-Host ("Target: {0}" -f $scriptEnvironment.BASE_URL)
Write-Host ("HTML report: {0}" -f $htmlReport)
Write-Host ("JSON summary: {0}" -f $jsonReport)

if ($Engine -eq 'Local') {
    $managedVariables = @{
        K6_WEB_DASHBOARD = 'true'
        K6_WEB_DASHBOARD_EXPORT = $htmlReport
        K6_WEB_DASHBOARD_PORT = '-1'
        K6_WEB_DASHBOARD_PERIOD = '1s'
        K6_SUMMARY_EXPORT = $jsonReport
        K6_SUMMARY_MODE = 'full'
        K6_SUMMARY_TREND_STATS = 'avg,min,med,max,p(90),p(95),p(99),count'
    }

    $previousValues = @{}
    try {
        foreach ($item in $managedVariables.GetEnumerator()) {
            $previousValues[$item.Key] = [Environment]::GetEnvironmentVariable($item.Key, 'Process')
            [Environment]::SetEnvironmentVariable($item.Key, $item.Value, 'Process')
        }

        $k6Arguments = @('run')
        foreach ($item in $scriptEnvironment.GetEnumerator()) {
            $k6Arguments += @('-e', "$($item.Key)=$($item.Value)")
        }
        $k6Arguments += $testScript
        & $localK6.Source @k6Arguments
        $exitCode = $LASTEXITCODE
    }
    finally {
        foreach ($item in $previousValues.GetEnumerator()) {
            [Environment]::SetEnvironmentVariable($item.Key, $item.Value, 'Process')
        }
    }
}
else {
    $dockerArguments = @(
        'run', '--rm',
        '--add-host', 'host.docker.internal:host-gateway',
        '-v', "${projectRoot}:/work",
        '-v', "${reportRoot}:/reports",
        '-w', '/work',
        '-e', 'K6_WEB_DASHBOARD=true',
        '-e', "K6_WEB_DASHBOARD_EXPORT=/reports/$reportBaseName.html",
        '-e', 'K6_WEB_DASHBOARD_PORT=-1',
        '-e', 'K6_WEB_DASHBOARD_PERIOD=1s',
        '-e', "K6_SUMMARY_EXPORT=/reports/$reportBaseName.json",
        '-e', 'K6_SUMMARY_MODE=full',
        '-e', 'K6_SUMMARY_TREND_STATS=avg,min,med,max,p(90),p(95),p(99),count'
    )
    $dockerArguments += @('grafana/k6:2.2.0', 'run')
    foreach ($item in $scriptEnvironment.GetEnumerator()) {
        $dockerArguments += @('-e', "$($item.Key)=$($item.Value)")
    }
    $dockerArguments += '/work/tests/load/webhook-load.js'

    & docker @dockerArguments
    $exitCode = $LASTEXITCODE
}

if ($exitCode -ne 0) {
    Write-Error "k6 finished with exit code $exitCode. Review the threshold results and generated reports." -ErrorAction Continue
    exit $exitCode
}

Write-Host ("k6 run passed. Report: {0}" -f $htmlReport)
