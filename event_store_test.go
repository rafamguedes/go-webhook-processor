package main

import (
	"errors"
	"testing"
)

func TestEventStorePersistsStatusChanges(t *testing.T) {
	store := newTestEventStore(t)
	event := testEvent()

	if err := store.SaveQueuedWithOutbox(t.Context(), event); err != nil {
		t.Fatalf("failed to save queued event: %v", err)
	}

	if err := store.MarkProcessed(t.Context(), event.ID, 2); err != nil {
		t.Fatalf("failed to mark event processed: %v", err)
	}

	events, err := store.List(t.Context(), 10)
	if err != nil {
		t.Fatalf("failed to list events: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	if events[0].Status != EventStatusProcessed {
		t.Fatalf("expected status processed, got %s", events[0].Status)
	}

	if events[0].Attempts != 2 {
		t.Fatalf("expected attempts 2, got %d", events[0].Attempts)
	}
}

func TestEventStoreRejectsDuplicateID(t *testing.T) {
	store := newTestEventStore(t)
	event := testEvent()

	if err := store.SaveQueuedWithOutbox(t.Context(), event); err != nil {
		t.Fatalf("failed to save queued event: %v", err)
	}

	if err := store.SaveQueuedWithOutbox(t.Context(), event); !errors.Is(err, ErrEventAlreadyExists) {
		t.Fatalf("expected duplicate event error, got %v", err)
	}
}
