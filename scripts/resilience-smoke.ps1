param(
    [int]$RecoveryWaitSeconds = 15,
    [int]$RecoveryTimeoutSeconds = 60
)

$ErrorActionPreference = "Stop"
$eventId = "resilience-$(Get-Date -Format 'yyyyMMddHHmmssfff')"
$eventBody = @{
    id = $eventId
    type = "resilience.smoke"
    payload = @{ source = "resilience-smoke" }
} | ConvertTo-Json -Compress

Write-Host "Starting application dependencies..."
docker compose up -d --build

try {
    Write-Host "Stopping RabbitMQ to simulate broker unavailability..."
    docker compose stop rabbitmq | Out-Null

    Write-Host "Submitting event $eventId while RabbitMQ is unavailable..."
    $response = Invoke-RestMethod -Method Post -Uri "http://localhost:8080/events" -ContentType "application/json" -Body $eventBody
    if (-not $response.accepted) {
        throw "The event was not accepted into the Outbox."
    }

    Write-Host "Starting RabbitMQ and waiting for recovery..."
    docker compose start rabbitmq | Out-Null
    Start-Sleep -Seconds $RecoveryWaitSeconds

    $deadline = (Get-Date).AddSeconds($RecoveryTimeoutSeconds)
    do {
        $logs = docker compose logs --since="5m" webhook-processor
        $eventPublished = $logs -split "`r?`n" | Where-Object {
            $_ -match [regex]::Escape($eventId) -and $_ -match "outbox message published"
        }
        if ($eventPublished) {
            Write-Host "Recovered event found in application logs."
            break
        }

        if ((Get-Date) -ge $deadline) {
            throw "The recovered event was not found in application logs within ${RecoveryTimeoutSeconds}s."
        }

        Start-Sleep -Seconds 2
    }
    while ($true)

    Write-Host "Resilience smoke test passed for event $eventId."
}
finally {
    Write-Host "Current service status:"
    docker compose ps
}
