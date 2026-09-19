package main

import (
	"errors"
	"testing"
	"time"
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

func TestEventStoreClaimsEventOnlyOnce(t *testing.T) {
	store := newTestEventStore(t)
	event := testEvent()
	if err := store.SaveQueuedWithOutbox(t.Context(), event); err != nil {
		t.Fatalf("failed to save event: %v", err)
	}

	firstClaim, err := store.ClaimForProcessing(t.Context(), event.ID, time.Minute)
	if err != nil {
		t.Fatalf("failed to claim event: %v", err)
	}
	secondClaim, err := store.ClaimForProcessing(t.Context(), event.ID, time.Minute)
	if err != nil {
		t.Fatalf("failed to inspect second claim: %v", err)
	}

	if firstClaim != EventClaimed {
		t.Fatalf("expected first claim to succeed, got %v", firstClaim)
	}
	if secondClaim != EventClaimInProgress {
		t.Fatalf("expected second claim to see processing event, got %v", secondClaim)
	}
}

func TestEventStoreReclaimsExpiredProcessingLease(t *testing.T) {
	store := newTestEventStore(t)
	event := testEvent()
	if err := store.SaveQueuedWithOutbox(t.Context(), event); err != nil {
		t.Fatalf("failed to save event: %v", err)
	}
	if _, err := store.ClaimForProcessing(t.Context(), event.ID, time.Minute); err != nil {
		t.Fatalf("failed to claim event: %v", err)
	}

	oldLease := time.Now().UTC().Add(-2 * time.Minute)
	if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET processing_started_at = $1 WHERE id = $2`, oldLease, event.ID); err != nil {
		t.Fatalf("failed to expire processing lease: %v", err)
	}

	claim, err := store.ClaimForProcessing(t.Context(), event.ID, time.Minute)
	if err != nil {
		t.Fatalf("failed to reclaim event: %v", err)
	}
	if claim != EventClaimed {
		t.Fatalf("expected expired lease to be reclaimed, got %v", claim)
	}
}

func TestEventStoreTreatsProcessedEventAsFinal(t *testing.T) {
	store := newTestEventStore(t)
	event := testEvent()
	if err := store.SaveQueuedWithOutbox(t.Context(), event); err != nil {
		t.Fatalf("failed to save event: %v", err)
	}
	if err := store.MarkProcessed(t.Context(), event.ID, 1); err != nil {
		t.Fatalf("failed to mark event processed: %v", err)
	}

	claim, err := store.ClaimForProcessing(t.Context(), event.ID, time.Minute)
	if err != nil {
		t.Fatalf("failed to inspect processed event: %v", err)
	}
	if claim != EventClaimFinal {
		t.Fatalf("expected processed event to be final, got %v", claim)
	}
}

func TestEventStorePersistsDeadLetter(t *testing.T) {
	store, databaseURL := newTestEventStoreWithURL(t)
	event := testEvent()
	event.RequestID = "request-dead-letter-001"
	if err := store.SaveDeadLetter(t.Context(), event, errForTest(), 3); err != nil {
		t.Fatalf("failed to save dead letter: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("failed to close event store: %v", err)
	}

	reopened, err := OpenEventStore(databaseURL)
	if err != nil {
		t.Fatalf("failed to reopen event store: %v", err)
	}
	defer reopened.Close()

	items, err := reopened.ListDeadLetters(t.Context(), 10)
	if err != nil {
		t.Fatalf("failed to list dead letters: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 persisted dead letter, got %d", len(items))
	}
	if items[0].Event.ID != event.ID || items[0].Attempts != 3 {
		t.Fatalf("unexpected persisted dead letter: %+v", items[0])
	}
	if items[0].Event.RequestID != event.RequestID {
		t.Fatalf("expected request ID %q, got %q", event.RequestID, items[0].Event.RequestID)
	}
}
