param(
    [int]$RecoveryWaitSeconds = 15
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
    try {
        Invoke-RestMethod -Method Post -Uri "http://localhost:8080/events" -ContentType "application/json" -Body $eventBody | Out-Null
        throw "The application accepted an event while PostgreSQL was unavailable."
    }
    catch [System.Net.WebException] {
        Write-Host "The request failed as expected while PostgreSQL was unavailable."
    }

    Write-Host "Starting PostgreSQL and waiting for recovery..."
    docker compose start postgres | Out-Null
    Start-Sleep -Seconds $RecoveryWaitSeconds

    $response = Invoke-RestMethod -Method Post -Uri "http://localhost:8080/events" -ContentType "application/json" -Body $eventBody
    if (-not $response.accepted -or $response.eventId -ne $eventId) {
        throw "The application did not accept the event after PostgreSQL recovery."
    }

    Write-Host "PostgreSQL resilience smoke test passed for event $eventId."
}
finally {
    Write-Host "Current service status:"
    docker compose ps
}
