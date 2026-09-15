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

	attempts := 0
	sleeps := 0

	succeeded := processEventWithRetry(1, testEvent(), config, func(workerID int, event Event) error {
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
	}, metrics, deadLetters)

	if !succeeded {
		t.Fatal("expected event processing to succeed")
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

	attempts := 0
	sleeps := 0

	succeeded := processEventWithRetry(1, testEvent(), config, func(workerID int, event Event) error {
		attempts++
		return errForTest()
	}, func(duration time.Duration) {
		sleeps++
	}, metrics, deadLetters)

	if succeeded {
		t.Fatal("expected event processing to fail")
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
