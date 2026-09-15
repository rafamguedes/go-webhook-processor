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
	}, metrics)

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
}

func TestProcessEventWithRetryFailsPermanently(t *testing.T) {
	config := testConfig()
	config.MaxRetries = 2
	config.RetryBackoffSeconds = 1
	metrics := NewMetrics()

	attempts := 0
	sleeps := 0

	succeeded := processEventWithRetry(1, testEvent(), config, func(workerID int, event Event) error {
		attempts++
		return fmt.Errorf("persistent failure")
	}, func(duration time.Duration) {
		sleeps++
	}, metrics)

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
}

func testEvent() Event {
	return Event{
		ID:      "evt-001",
		Type:    "payment.created",
		Payload: map[string]any{"amount": 100},
	}
}
