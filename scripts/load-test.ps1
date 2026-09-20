param(
    [int]$Requests = 500,
    [string]$BaseUrl = "http://localhost:8080"
)

$ErrorActionPreference = "Stop"
$success = 0
$failed = 0
$bodyFile = Join-Path $env:TEMP "webhook-load-test-body.json"

try {
    for ($i = 1; $i -le $Requests; $i++) {
        $eventId = "load-test-$([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())-$i"
        $requestId = "load-request-$i"
        $body = @{
            id = $eventId
            type = "load.test"
            payload = @{ sequence = $i }
        } | ConvertTo-Json -Compress

        [System.IO.File]::WriteAllText($bodyFile, $body, [System.Text.UTF8Encoding]::new($false))
        $statusCode = & curl.exe --silent --show-error --output NUL --write-out "%{http_code}" `
            -X POST "$BaseUrl/events" `
            -H "Content-Type: application/json" `
            -H "X-Request-ID: $requestId" `
            --data-binary "@$bodyFile"

        if ($statusCode -eq "202") {
            $success++
        } else {
            $failed++
            Write-Warning "Request $i returned HTTP $statusCode"
        }
    }
}
finally {
    Remove-Item -LiteralPath $bodyFile -Force -ErrorAction SilentlyContinue
}

Write-Host "Load test finished: $Requests requests, $success accepted, $failed failed."
if ($failed -gt 0) {
    exit 1
}
