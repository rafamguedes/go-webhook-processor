package main

import (
	"fmt"
	"testing"
	"time"
)

func TestProcessEventWithRetrySucceedsAfterFailure(t *testing.T) {
	config := testConfig()
	config.MaxRetries = 2
	config.RetryBackoffSeconds = 1
	metrics := NewMetrics()
	deadLetters := NewDeadLetterStore(config.DeadLetterCapacity)
	eventStore := newTestEventStore(t)
	event := testEvent()
	if err := eventStore.SaveQueuedWithOutbox(t.Context(), event); err != nil {
		t.Fatalf("failed to save queued event: %v", err)
	}

	attempts := 0
	sleeps := 0

	succeeded, terminalPersisted := processEventWithRetry(1, event, config, func(workerID int, event Event) error {
		attempts++
		if attempts == 1 {
			return fmt.Errorf("temporary failure")
		}
		return nil
	}, func(duration time.Duration) {
		sleeps++
		if duration != time.Second {
			t.Fatalf("expected first backoff to be 1s, got %s", duration)
		}
	}, metrics, deadLetters, eventStore)

	if !succeeded {
		t.Fatal("expected event processing to succeed")
	}
	if !terminalPersisted {
		t.Fatal("expected processed status to be persisted")
	}

	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}

	if sleeps != 1 {
		t.Fatalf("expected 1 sleep, got %d", sleeps)
	}

	snapshot := metrics.Snapshot(0, 100)
	if snapshot.EventRetries != 1 {
		t.Fatalf("expected 1 retry, got %d", snapshot.EventRetries)
	}

	if snapshot.EventsProcessed != 1 {
		t.Fatalf("expected 1 processed event, got %d", snapshot.EventsProcessed)
	}

	deadLetterSnapshot := deadLetters.Snapshot()
	if deadLetterSnapshot.Count != 0 {
		t.Fatalf("expected no dead letters, got %d", deadLetterSnapshot.Count)
	}
}

func TestProcessEventWithRetryFailsPermanently(t *testing.T) {
	config := testConfig()
	config.MaxRetries = 2
	config.RetryBackoffSeconds = 1
	metrics := NewMetrics()
	deadLetters := NewDeadLetterStore(config.DeadLetterCapacity)
	eventStore := newTestEventStore(t)
	event := testEvent()
	if err := eventStore.SaveQueuedWithOutbox(t.Context(), event); err != nil {
		t.Fatalf("failed to save queued event: %v", err)
	}

	attempts := 0
	sleeps := 0

	succeeded, terminalPersisted := processEventWithRetry(1, event, config, func(workerID int, event Event) error {
		attempts++
		return errForTest()
	}, func(duration time.Duration) {
		sleeps++
	}, metrics, deadLetters, eventStore)

	if succeeded {
		t.Fatal("expected event processing to fail")
	}
	if !terminalPersisted {
		t.Fatal("expected failed status to be persisted")
	}

	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}

	if sleeps != 2 {
		t.Fatalf("expected 2 sleeps, got %d", sleeps)
	}

	snapshot := metrics.Snapshot(0, 100)
	if snapshot.EventRetries != 2 {
		t.Fatalf("expected 2 retries, got %d", snapshot.EventRetries)
	}

	if snapshot.EventsFailedPermanent != 1 {
		t.Fatalf("expected 1 permanent failure, got %d", snapshot.EventsFailedPermanent)
	}

	deadLetterSnapshot := deadLetters.Snapshot()
	if deadLetterSnapshot.Count != 1 {
		t.Fatalf("expected 1 dead letter, got %d", deadLetterSnapshot.Count)
	}

	if deadLetterSnapshot.Items[0].Attempts != 3 {
		t.Fatalf("expected 3 attempts in dead letter, got %d", deadLetterSnapshot.Items[0].Attempts)
	}
}

func TestDeadLetterStoreKeepsCapacity(t *testing.T) {
	store := NewDeadLetterStore(2)
	store.Add(Event{ID: "evt-001", Type: "test"}, errForTest(), 1)
	store.Add(Event{ID: "evt-002", Type: "test"}, errForTest(), 1)
	store.Add(Event{ID: "evt-003", Type: "test"}, errForTest(), 1)

	snapshot := store.Snapshot()
	if snapshot.Count != 2 {
		t.Fatalf("expected 2 dead letters, got %d", snapshot.Count)
	}

	if snapshot.Items[0].Event.ID != "evt-002" {
		t.Fatalf("expected oldest retained event to be evt-002, got %s", snapshot.Items[0].Event.ID)
	}
}

func testEvent() Event {
	return Event{
		ID:      "evt-001",
		Type:    "payment.created",
		Payload: map[string]any{"amount": 100},
	}
}

func errForTest() error {
	return fmt.Errorf("persistent failure")
}

func TestHandleDeliverySkipsAlreadyProcessedEvent(t *testing.T) {
	store := newTestEventStore(t)
	event := testEvent()
	if err := store.SaveQueuedWithOutbox(t.Context(), event); err != nil {
		t.Fatalf("failed to save event: %v", err)
	}
	if err := store.MarkProcessed(t.Context(), event.ID, 1); err != nil {
		t.Fatalf("failed to mark event processed: %v", err)
	}

	acked := false
	processed := false
	delivery := EventDelivery{
		Event: event,
		ack: func() error {
			acked = true
			return nil
		},
	}
	metrics := NewMetrics()
	handleDelivery(1, delivery, testConfig(), metrics, NewDeadLetterStore(10), store, func(int, Event) error {
		processed = true
		return nil
	}, func(time.Duration) {})

	if processed {
		t.Fatal("expected processed event not to execute again")
	}
	if !acked {
		t.Fatal("expected duplicate delivery to be acknowledged")
	}
	if metrics.Snapshot(0, 0).EventsSkippedDuplicate != 1 {
		t.Fatal("expected skipped duplicate metric to be incremented")
	}
}

func TestHandleDeliveryRequeuesEventWithActiveLease(t *testing.T) {
	store := newTestEventStore(t)
	event := testEvent()
	if err := store.SaveQueuedWithOutbox(t.Context(), event); err != nil {
		t.Fatalf("failed to save event: %v", err)
	}
	if _, err := store.ClaimForProcessing(t.Context(), event.ID, time.Minute); err != nil {
		t.Fatalf("failed to claim event: %v", err)
	}

	nacked := false
	requeued := false
	delivery := EventDelivery{
		Event: event,
		nack: func(requeue bool) error {
			nacked = true
			requeued = requeue
			return nil
		},
	}
	processed := false
	handleDelivery(1, delivery, testConfig(), NewMetrics(), NewDeadLetterStore(10), store, func(int, Event) error {
		processed = true
		return nil
	}, func(time.Duration) {})

	if processed {
		t.Fatal("expected event with active lease not to execute concurrently")
	}
	if !nacked || !requeued {
		t.Fatal("expected delivery with active lease to be nacked with requeue")
	}
}
