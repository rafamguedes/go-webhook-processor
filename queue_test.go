package main

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryEventQueueTryPublishRejectsWhenFull(t *testing.T) {
	queue := NewMemoryEventQueue(1)

	if err := queue.TryPublish(t.Context(), testEvent()); err != nil {
		t.Fatalf("expected first event to be published, got %v", err)
	}

	secondEvent := Event{ID: "evt-002", Type: "test"}
	if err := queue.TryPublish(t.Context(), secondEvent); !errors.Is(err, ErrEventQueueFull) {
		t.Fatalf("expected queue full error, got %v", err)
	}

	stats := queue.Stats()
	if stats.Length != 1 || stats.Capacity != 1 {
		t.Fatalf("expected queue stats 1/1, got %d/%d", stats.Length, stats.Capacity)
	}
}

func TestMemoryEventQueuePublishWaitsUntilContextIsCanceled(t *testing.T) {
	queue := NewMemoryEventQueue(1)
	if err := queue.Publish(t.Context(), testEvent()); err != nil {
		t.Fatalf("expected first event to be published, got %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	secondEvent := Event{ID: "evt-002", Type: "test"}
	if err := queue.Publish(ctx, secondEvent); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled error, got %v", err)
	}
}

func TestMemoryEventQueueCloseClosesEvents(t *testing.T) {
	queue := NewMemoryEventQueue(1)
	if err := queue.Close(); err != nil {
		t.Fatalf("expected queue to close, got %v", err)
	}

	if _, open := <-queue.Events(); open {
		t.Fatal("expected events channel to be closed")
	}
}
func TestNewConfiguredEventQueueCreatesMemoryQueue(t *testing.T) {
	config := testConfig()

	queue, err := NewConfiguredEventQueue(config)
	if err != nil {
		t.Fatalf("expected memory queue to be configured, got %v", err)
	}
	if _, ok := queue.(*MemoryEventQueue); !ok {
		t.Fatalf("expected *MemoryEventQueue, got %T", queue)
	}
}

func TestNewConfiguredEventQueueRejectsUnsupportedProvider(t *testing.T) {
	config := testConfig()
	config.QueueProvider = "unsupported"

	_, err := NewConfiguredEventQueue(config)
	if err == nil {
		t.Fatal("expected unsupported provider to be rejected")
	}
}
