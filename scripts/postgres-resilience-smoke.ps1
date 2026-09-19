param(
    [int]$RecoveryWaitSeconds = 15,
    [int]$RecoveryTimeoutSeconds = 60
)

$ErrorActionPreference = "Stop"
$eventId = "postgres-resilience-$(Get-Date -Format 'yyyyMMddHHmmssfff')"
$eventBody = @{
    id = $eventId
    type = "resilience.postgres.smoke"
    payload = @{ source = "postgres-resilience-smoke" }
} | ConvertTo-Json -Compress

Write-Host "Starting application dependencies..."
docker compose up -d --build

try {
    Write-Host "Stopping PostgreSQL to simulate database unavailability..."
    docker compose stop postgres | Out-Null

    Write-Host "Submitting an event while PostgreSQL is unavailable..."
    $unavailableResponse = Invoke-WebRequest -Method Post -Uri "http://localhost:8080/events" -ContentType "application/json" -Body $eventBody -SkipHttpErrorCheck
    if ($unavailableResponse.StatusCode -ne 500) {
        throw "Expected HTTP 500 while PostgreSQL was unavailable, got HTTP $($unavailableResponse.StatusCode)."
    }
    Write-Host "The request failed as expected while PostgreSQL was unavailable."

    Write-Host "Starting PostgreSQL and waiting for recovery..."
    docker compose start postgres | Out-Null
    Start-Sleep -Seconds $RecoveryWaitSeconds

    $deadline = (Get-Date).AddSeconds($RecoveryTimeoutSeconds)
    do {
        $response = Invoke-WebRequest -Method Post -Uri "http://localhost:8080/events" -ContentType "application/json" -Body $eventBody -SkipHttpErrorCheck
        if ($response.StatusCode -eq 202) {
            $responseBody = $response.Content | ConvertFrom-Json
            if ($responseBody.accepted -and $responseBody.eventId -eq $eventId) {
                break
            }
        }

        if ((Get-Date) -ge $deadline) {
            throw "The application did not accept the event after PostgreSQL recovery within ${RecoveryTimeoutSeconds}s."
        }

        Start-Sleep -Seconds 2
    }
    while ($true)

    Write-Host "PostgreSQL resilience smoke test passed for event $eventId."
}
finally {
    Write-Host "Current service status:"
    docker compose ps
}
