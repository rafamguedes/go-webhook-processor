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
