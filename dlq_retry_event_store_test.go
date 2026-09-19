package main

import "testing"

func TestEventStoreClaimsRetryableDeadLetterOnlyUpToLimit(t *testing.T) {
	store := newTestEventStore(t)
	event := testEvent()
	if err := store.SaveDeadLetter(t.Context(), event, errForTest(), 4); err != nil {
		t.Fatalf("failed to save dead letter: %v", err)
	}

	eventIDs, err := store.ClaimRetryableDeadLetters(t.Context(), 10, 1)
	if err != nil {
		t.Fatalf("failed to claim retryable dead letter: %v", err)
	}
	if len(eventIDs) != 1 || eventIDs[0] != event.ID {
		t.Fatalf("expected event %q to be claimed, got %v", event.ID, eventIDs)
	}

	eventIDs, err = store.ClaimRetryableDeadLetters(t.Context(), 10, 1)
	if err != nil {
		t.Fatalf("failed to inspect retry limit: %v", err)
	}
	if len(eventIDs) != 0 {
		t.Fatalf("expected retry limit to prevent another claim, got %v", eventIDs)
	}
}
