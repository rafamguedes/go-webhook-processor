package main

import (
	"context"
	"os"
	"sync"
	"testing"
)

type testEventQueue struct {
	events chan EventDelivery
	once   sync.Once
}

func newTestEventQueue(capacity int) *testEventQueue {
	return &testEventQueue{events: make(chan EventDelivery, capacity)}
}

func (queue *testEventQueue) Publish(ctx context.Context, event Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case queue.events <- EventDelivery{Event: event}:
		return nil
	}
}

func (queue *testEventQueue) Events() <-chan EventDelivery {
	return queue.events
}

func (queue *testEventQueue) Stats() QueueStats {
	return QueueStats{Length: len(queue.events), Capacity: cap(queue.events)}
}

func (queue *testEventQueue) StopConsuming() error {
	queue.once.Do(func() { close(queue.events) })
	return nil
}

func (queue *testEventQueue) Close() error {
	return queue.StopConsuming()
}

var testMigrationsOnce sync.Once
var testMigrationsErr error

func newTestEventStore(t *testing.T) *EventStore {
	t.Helper()
	store, _ := newTestEventStoreWithURL(t)
	return store
}

func newTestEventStoreWithURL(t *testing.T) (*EventStore, string) {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")
	}

	testMigrationsOnce.Do(func() {
		testMigrationsErr = RunMigrations(t.Context(), databaseURL, "migrations")
	})
	if testMigrationsErr != nil {
		t.Fatalf("failed to migrate test event store: %v", testMigrationsErr)
	}

	store, err := OpenEventStore(databaseURL)
	if err != nil {
		t.Fatalf("failed to open test event store: %v", err)
	}
	if _, err := store.db.ExecContext(t.Context(), `TRUNCATE TABLE dead_letters, outbox, events RESTART IDENTITY CASCADE`); err != nil {
		store.Close()
		t.Fatalf("failed to reset test event store: %v", err)
	}
	t.Cleanup(func() {
		store.db.ExecContext(context.Background(), `TRUNCATE TABLE dead_letters, outbox, events RESTART IDENTITY CASCADE`)
		store.Close()
	})
	return store, databaseURL
}

func newTestApp(t *testing.T) App {
	t.Helper()
	config := testConfig()
	return NewApp(config, newTestEventStore(t), newTestEventQueue(config.QueueSize))
}

func testConfig() Config {
	return Config{
		Port:                     "8080",
		QueueSize:                100,
		WorkerCount:              3,
		ReadHeaderTimeoutSeconds: 5,
		ShutdownTimeoutSeconds:   10,
		LogFormat:                "json",
		MaxRetries:               3,
		RetryBackoffSeconds:      1,
		DeadLetterCapacity:       100,
		DatabaseURL:              "postgres://webhook:webhook_dev@localhost:5432/webhook_test?sslmode=disable",
		RabbitMQURL:              "amqp://guest:guest@localhost:5672/",
		RabbitMQQueue:            "webhook.events.test",
		RabbitMQReconnectMs:      50,
		RabbitMQConnectTimeoutMs: 500,
		OutboxPollIntervalMs:     10,
		OutboxBatchSize:          100,
		ProcessingLeaseSeconds:   300,
		ProcessingRequeueDelayMs: 10,
	}
}
