package main

import (
	"context"
	"path/filepath"
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

func newTestEventStore(t *testing.T) *EventStore {
	t.Helper()
	store, _ := newTestEventStoreWithPath(t)
	return store
}

func newTestEventStoreWithPath(t *testing.T) (*EventStore, string) {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "events.db")
	store, err := OpenEventStore(databasePath)
	if err != nil {
		t.Fatalf("failed to open test event store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store, databasePath
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
		DatabasePath:             "./events-test.db",
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
