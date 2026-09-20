param(
    [int]$Requests = 500,
    [int]$Concurrency = 20,
    [int]$BatchSize = 1000,
    [int]$RequestTimeoutSeconds = 120,
    [int]$DelayBetweenBatchesMs = 0,
    [string]$BaseUrl = "http://localhost:8080"
)

$ErrorActionPreference = "Stop"

Add-Type -AssemblyName System.Net.Http

if ($Requests -le 0 -or $Concurrency -le 0 -or $BatchSize -le 0) {
    throw "Requests, Concurrency, and BatchSize must be greater than zero."
}
if ($BatchSize -lt $Concurrency) {
    throw "BatchSize must be greater than or equal to Concurrency."
}
if ($RequestTimeoutSeconds -le 0) {
    throw "RequestTimeoutSeconds must be greater than zero."
}
if ($DelayBetweenBatchesMs -lt 0) {
    throw "DelayBetweenBatchesMs cannot be negative."
}

$handler = [System.Net.Http.HttpClientHandler]::new()
$handler.MaxConnectionsPerServer = $Concurrency
$client = [System.Net.Http.HttpClient]::new($handler)
$client.Timeout = [TimeSpan]::FromSeconds($RequestTimeoutSeconds)

$runId = "parallel-load-$([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())-$([Guid]::NewGuid().ToString('N').Substring(0, 8))"
$accepted = 0
$httpFailures = 0
$transportFailures = 0
$attempted = 0
$statusCounts = @{}
$transportErrorCounts = @{}
$totalBatches = [int][Math]::Ceiling($Requests / [double]$BatchSize)
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

Write-Host ("Starting load test {0}: {1} requests, concurrency {2}, batch size {3}." -f $runId, $Requests, $Concurrency, $BatchSize)

try {
    for ($batchNumber = 1; $batchNumber -le $totalBatches; $batchNumber++) {
        $remaining = $Requests - $attempted
        $currentBatchSize = [Math]::Min($BatchSize, $remaining)
        $httpRequests = [System.Collections.Generic.List[System.Net.Http.HttpRequestMessage]]::new()
        $tasks = [System.Collections.Generic.List[System.Threading.Tasks.Task[System.Net.Http.HttpResponseMessage]]]::new()

        try {
            for ($batchIndex = 1; $batchIndex -le $currentBatchSize; $batchIndex++) {
                $sequence = $attempted + $batchIndex
                $eventId = "$runId-$sequence"
                $payload = @{
                    id = $eventId
                    type = "parallel.load.test"
                    payload = @{ sequence = $sequence; runId = $runId }
                } | ConvertTo-Json -Compress

                $request = [System.Net.Http.HttpRequestMessage]::new(
                    [System.Net.Http.HttpMethod]::Post,
                    "$($BaseUrl.TrimEnd('/'))/events"
                )
                $request.Headers.Add("X-Request-ID", "$runId-request-$sequence")
                $request.Content = [System.Net.Http.StringContent]::new(
                    $payload,
                    [System.Text.Encoding]::UTF8,
                    "application/json"
                )

                $httpRequests.Add($request)
                $tasks.Add($client.SendAsync($request))
            }

            try {
                [System.Threading.Tasks.Task]::WhenAll($tasks).GetAwaiter().GetResult()
            }
            catch {
                # Individual task results are classified below so one transport error does not abort the run.
            }

            foreach ($task in $tasks) {
                if ($task.Status -eq [System.Threading.Tasks.TaskStatus]::RanToCompletion) {
                    $response = $task.Result
                    try {
                        $statusCode = [int]$response.StatusCode
                        if (-not $statusCounts.ContainsKey($statusCode)) {
                            $statusCounts[$statusCode] = 0
                        }
                        $statusCounts[$statusCode]++

                        if ($statusCode -eq 202) {
                            $accepted++
                        }
                        else {
                            $httpFailures++
                        }
                    }
                    finally {
                        $response.Dispose()
                    }
                }
                else {
                    $transportFailures++
                    $message = if ($task.IsCanceled) {
                        "Request canceled or timed out"
                    }
                    elseif ($null -ne $task.Exception) {
                        $task.Exception.GetBaseException().Message
                    }
                    else {
                        "Unknown transport error"
                    }

                    if (-not $transportErrorCounts.ContainsKey($message)) {
                        $transportErrorCounts[$message] = 0
                    }
                    $transportErrorCounts[$message]++
                }
            }
        }
        finally {
            foreach ($request in $httpRequests) {
                $request.Dispose()
            }
        }

        $attempted += $currentBatchSize
        Write-Host (
            "Batch {0}/{1}: attempted {2}/{3}, accepted {4}, HTTP failures {5}, transport failures {6}." -f `
                $batchNumber, $totalBatches, $attempted, $Requests, $accepted, $httpFailures, $transportFailures
        )

        if ($DelayBetweenBatchesMs -gt 0 -and $batchNumber -lt $totalBatches) {
            Start-Sleep -Milliseconds $DelayBetweenBatchesMs
        }
    }
}
finally {
    $stopwatch.Stop()
    $client.Dispose()
    $handler.Dispose()
}

$failed = $httpFailures + $transportFailures
$throughput = if ($stopwatch.Elapsed.TotalSeconds -gt 0) {
    $attempted / $stopwatch.Elapsed.TotalSeconds
}
else {
    0
}

Write-Host ""
Write-Host ("Parallel load test finished: {0} attempted, {1} accepted, {2} failed." -f $attempted, $accepted, $failed)
Write-Host ("Run ID: {0}" -f $runId)
Write-Host ("Concurrency limit: {0}; batch size: {1}; total: {2:N2}s; throughput: {3:N2} req/s" -f `
        $Concurrency, $BatchSize, $stopwatch.Elapsed.TotalSeconds, $throughput)

if ($statusCounts.Count -gt 0) {
    Write-Host "HTTP status counts:"
    $statusCounts.GetEnumerator() |
        Sort-Object { [int]$_.Key } |
        ForEach-Object { Write-Host ("  HTTP {0}: {1}" -f $_.Key, $_.Value) }
}

if ($transportErrorCounts.Count -gt 0) {
    Write-Host "Transport error counts:"
    $transportErrorCounts.GetEnumerator() |
        Sort-Object Value -Descending |
        ForEach-Object { Write-Host ("  {0}: {1}" -f $_.Key, $_.Value) }
}

if ($failed -gt 0) {
    exit 1
}
