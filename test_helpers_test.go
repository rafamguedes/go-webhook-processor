package main

import (
	"path/filepath"
	"testing"
)

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

	t.Cleanup(func() {
		store.Close()
	})

	return store, databasePath
}

func newTestApp(t *testing.T) App {
	t.Helper()

	config := testConfig()
	return NewApp(config, newTestEventStore(t), NewMemoryEventQueue(config.QueueSize))
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
		QueueProvider:            QueueProviderMemory,
		OutboxPollIntervalMs:     10,
		OutboxBatchSize:          100,
	}
}
