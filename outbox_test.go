package main

import (
	"context"
	"errors"
	"testing"
)

type failingEventPublisher struct {
	err error
}

func (publisher failingEventPublisher) Publish(context.Context, Event) error {
	return publisher.err
}

func TestOutboxDispatcherKeepsMessagePendingAfterPublishFailure(t *testing.T) {
	store := newTestEventStore(t)
	if err := store.SaveQueuedWithOutbox(t.Context(), testEvent()); err != nil {
		t.Fatalf("failed to save event with outbox: %v", err)
	}

	publishErr := errors.New("broker unavailable")
	dispatcher := NewOutboxDispatcher(store, failingEventPublisher{err: publishErr}, testConfig(), NewMetrics())
	if err := dispatcher.DispatchPending(t.Context()); err != nil {
		t.Fatalf("expected publish failure to remain retryable, got: %v", err)
	}

	pending, err := store.ListPendingOutbox(t.Context(), 10)
	if err != nil {
		t.Fatalf("failed to list pending outbox: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending message, got %d", len(pending))
	}
	if pending[0].Attempts != 1 {
		t.Fatalf("expected 1 publish attempt, got %d", pending[0].Attempts)
	}
}

func TestOutboxDispatcherMarksMessagePublished(t *testing.T) {
	store := newTestEventStore(t)
	queue := newTestEventQueue(1)
	metrics := NewMetrics()
	if err := store.SaveQueuedWithOutbox(t.Context(), testEvent()); err != nil {
		t.Fatalf("failed to save event with outbox: %v", err)
	}

	dispatcher := NewOutboxDispatcher(store, queue, testConfig(), metrics)
	if err := dispatcher.DispatchPending(t.Context()); err != nil {
		t.Fatalf("failed to dispatch outbox: %v", err)
	}

	pending, err := store.ListPendingOutbox(t.Context(), 10)
	if err != nil {
		t.Fatalf("failed to list pending outbox: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected no pending messages, got %d", len(pending))
	}
	if event := (<-queue.Events()).Event; event.ID != testEvent().ID {
		t.Fatalf("expected event %s, got %s", testEvent().ID, event.ID)
	}
	if metrics.Snapshot(0, 1).EventsQueued != 1 {
		t.Fatal("expected queued metric to be incremented")
	}
}
