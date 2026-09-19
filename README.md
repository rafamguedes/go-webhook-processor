# Go Webhook Processor

Go HTTP service for receiving and asynchronously processing webhook events.

The service validates events, persists the event and its publication intent in the same PostgreSQL transaction, responds with `202 Accepted`, and processes work in the background through RabbitMQ and concurrent workers.

## Features

- Transactional Outbox with PostgreSQL.
- RabbitMQ as the durable event queue.
- Idempotent event acceptance and consumer processing.
- Worker retries with linear backoff.
- Persistent Dead Letter Queue with manual and automatic replay.
- Versioned database migrations.
- Multi-replica-safe Outbox leases.
- Structured logs, request correlation, metrics, health, and readiness endpoints.

## Flow

```text
External client -> POST /events -> PostgreSQL transaction (events + outbox)
                 -> HTTP 202 -> Outbox dispatcher -> RabbitMQ
                 -> workers -> retry/backoff -> processed or failed
```

## Endpoints

- `GET /health`: process health and local RabbitMQ buffer information.
- `GET /ready`: PostgreSQL readiness check (`200` ready, `503` unavailable).
- `GET /metrics`: operational counters.
- `GET /dead-letters`: permanently failed events stored in PostgreSQL.
- `POST /internal/dead-letters/{event-id}/replay`: authenticated manual replay.
- `POST /events`: accepts an event for asynchronous processing.

Example event:

```json
{
  "id": "evt-001",
  "type": "payment.created",
  "payload": { "amount": 100 }
}
```

Requests may include `X-Request-ID`. If omitted, the application generates one and returns it in the response.

## Architecture

```text
main.go             bootstrap and graceful shutdown
config.go           environment configuration and validation
app.go              application state and HTTP routes
handlers.go         HTTP handlers and validation
event_store.go      PostgreSQL persistence, Outbox, idempotency, and DLQ
migrations/         versioned PostgreSQL migrations
rabbitmq_queue.go   RabbitMQ queue implementation
outbox.go           Outbox dispatcher
dlq_retry.go        automatic DLQ retry scheduler
worker.go           workers and processing retries
metrics.go          thread-safe operational metrics
requests.http       IDE-ready HTTP requests
```

See [`docs/architecture.md`](docs/architecture.md) for detailed architecture documentation.

## Configuration

```text
PORT=8080
QUEUE_SIZE=100
WORKER_COUNT=3
READ_HEADER_TIMEOUT_SECONDS=5
SHUTDOWN_TIMEOUT_SECONDS=10
LOG_FORMAT=json
MAX_RETRIES=3
RETRY_BACKOFF_SECONDS=1
DEAD_LETTER_CAPACITY=100
DATABASE_URL=postgres://webhook:webhook_dev@localhost:5432/webhook?sslmode=disable
DLQ_REPLAY_TOKEN=change-me-local
RABBITMQ_URL=amqp://webhook:webhook_dev@localhost:5672/
RABBITMQ_QUEUE=webhook.events
RABBITMQ_RECONNECT_MS=1000
RABBITMQ_CONNECT_TIMEOUT_MS=5000
OUTBOX_POLL_INTERVAL_MS=500
OUTBOX_BATCH_SIZE=100
OUTBOX_DISPATCH_LEASE_SECONDS=30
PROCESSING_LEASE_SECONDS=300
PROCESSING_REQUEUE_DELAY_MS=1000
DLQ_AUTO_RETRY_ENABLED=false
DLQ_AUTO_RETRY_INTERVAL_SECONDS=60
DLQ_AUTO_RETRY_MAX_ATTEMPTS=3
```

`DLQ_AUTO_RETRY_ENABLED` is disabled by default. When enabled, the scheduler claims eligible dead letters with PostgreSQL locking, increments `replay_attempts`, and stops after `DLQ_AUTO_RETRY_MAX_ATTEMPTS`.

## Database and migrations

PostgreSQL stores events, Outbox messages, processing state, and dead letters. Apply migrations before starting a new service version:

```powershell
go run . migrate
```

Each schema change must add a new pair of `NNNNNN_description.up.sql` and `NNNNNN_description.down.sql` files. Docker Compose runs the `migrate` service automatically before starting the API.

## Idempotency and delivery

The event `id` is the idempotency key. The `events` primary key rejects duplicate webhook submissions. Workers atomically claim events before processing them, so concurrent deliveries do not execute the same event simultaneously.

Delivery is `at-least-once`. A crash after RabbitMQ confirmation and before `published_at` is persisted can publish the event again. Consumers must treat `event.id` as an idempotency key.

Outbox messages are claimed with `FOR UPDATE SKIP LOCKED`, a random dispatch token, and `OUTBOX_DISPATCH_LEASE_SECONDS`. Abandoned claims become eligible again after the lease expires.

## Retry and Dead Letter Queue

With the default settings, processing has one initial attempt plus three retries. The linear backoff is 1, 2, and 3 seconds. After the final failure, the event is marked `failed` and stored in PostgreSQL.

Manual replay:

```powershell
go run . replay-dead-letter <event-id>
```

With Docker Compose:

```powershell
docker compose run --rm --no-deps webhook-processor replay-dead-letter <event-id>
```

Replay resets the event to `queued`, reopens its Outbox message, records `replayed_at`, and keeps the original DLQ history.

## Local execution and Docker

```powershell
go run .
Copy-Item .env.example .env
docker compose up -d --build
```

The API listens on `http://localhost:8080`. RabbitMQ Management is available at `http://localhost:15672`.

The image includes a healthcheck based on `/ready`:

```powershell
docker inspect --format='{{.State.Health.Status}}' go-webhook-processor
```

## Tests and build

```powershell
docker compose exec postgres createdb -U webhook webhook_test
$env:TEST_DATABASE_URL = "postgres://webhook:webhook_dev@localhost:5432/webhook_test?sslmode=disable"
go test ./...
go build .
```

The CI pipeline creates the test database before running integration tests.

## Reset the Docker environment

The following commands remove PostgreSQL and RabbitMQ data:

```powershell
docker compose down --volumes --remove-orphans
docker compose up -d --build
```

Follow logs with `docker compose logs -f webhook-processor`. The [`requests.http`](requests.http) file contains ready-to-run requests for the VS Code REST Client extension.
