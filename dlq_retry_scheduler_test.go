package main

import "testing"

func TestDLQRetrySchedulerReopensEligibleDeadLetter(t *testing.T) {
	store := newTestEventStore(t)
	event := testEvent()
	if err := store.SaveQueuedWithOutbox(t.Context(), event); err != nil {
		t.Fatalf("failed to save event: %v", err)
	}
	if err := store.MarkFailed(t.Context(), event.ID, 1, errForTest()); err != nil {
		t.Fatalf("failed to mark event failed: %v", err)
	}
	if err := store.SaveDeadLetter(t.Context(), event, errForTest(), 1); err != nil {
		t.Fatalf("failed to save dead letter: %v", err)
	}

	config := testConfig()
	config.DLQAutoRetryMaxAttempts = 1
	metrics := NewMetrics()
	scheduler := NewDLQRetryScheduler(store, config, metrics)
	scheduler.retry(t.Context())

	pending, err := store.ListPendingOutbox(t.Context(), 10)
	if err != nil {
		t.Fatalf("failed to list reopened outbox: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected one reopened outbox message, got %d", len(pending))
	}
	if metrics.Snapshot(0, 0).DeadLetterAutoRetries != 1 {
		t.Fatal("expected automatic retry metric to be incremented")
	}
}
