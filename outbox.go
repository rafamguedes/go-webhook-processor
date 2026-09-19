package main

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type OutboxDispatcher struct {
	store         *EventStore
	publisher     EventPublisher
	pollInterval  time.Duration
	batchSize     int
	metrics       *Metrics
	dispatchLease time.Duration
}

func NewOutboxDispatcher(store *EventStore, publisher EventPublisher, config Config, metrics *Metrics) *OutboxDispatcher {
	return &OutboxDispatcher{
		store:         store,
		publisher:     publisher,
		pollInterval:  config.OutboxPollInterval(),
		batchSize:     config.OutboxBatchSize,
		metrics:       metrics,
		dispatchLease: config.OutboxDispatchLease(),
	}
}

func (dispatcher *OutboxDispatcher) Start(ctx context.Context, waitGroup *sync.WaitGroup) {
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		dispatcher.run(ctx)
	}()
}

func (dispatcher *OutboxDispatcher) run(ctx context.Context) {
	ticker := time.NewTicker(dispatcher.pollInterval)
	defer ticker.Stop()

	for {
		if err := dispatcher.DispatchPending(ctx); err != nil && ctx.Err() == nil {
			slog.Error("dispatch outbox failed", "error", err)
		}

		select {
		case <-ctx.Done():
			slog.Info("outbox dispatcher stopped")
			return
		case <-ticker.C:
		}
	}
}

func (dispatcher *OutboxDispatcher) DispatchPending(ctx context.Context) error {
	messages, err := dispatcher.store.ClaimPendingOutbox(ctx, dispatcher.batchSize, dispatcher.dispatchLease)
	if err != nil {
		return err
	}

	for _, message := range messages {
		if err := dispatcher.publisher.Publish(ctx, message.Event); err != nil {
			if markErr := dispatcher.store.MarkOutboxFailed(context.WithoutCancel(ctx), message, err); markErr != nil {
				slog.Error("mark outbox publish failure failed", "outbox_id", message.ID, "event_id", message.Event.ID, "request_id", message.Event.RequestID, "error", markErr)
			}
			slog.Warn("outbox message publish failed", "outbox_id", message.ID, "event_id", message.Event.ID, "request_id", message.Event.RequestID, "error", err)
			break
		}

		if err := dispatcher.store.MarkOutboxPublished(ctx, message); err != nil {
			return err
		}
		dispatcher.metrics.IncEventsQueued()
		slog.Info("outbox message published", "outbox_id", message.ID, "event_id", message.Event.ID, "request_id", message.Event.RequestID, "event_type", message.Event.Type)
	}
	return nil
}
