package main

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type DLQRetryScheduler struct {
	store       *EventStore
	interval    time.Duration
	maxAttempts int
}

func NewDLQRetryScheduler(store *EventStore, config Config) *DLQRetryScheduler {
	return &DLQRetryScheduler{
		store:       store,
		interval:    time.Duration(config.DLQAutoRetryIntervalSeconds) * time.Second,
		maxAttempts: config.DLQAutoRetryMaxAttempts,
	}
}

func (scheduler *DLQRetryScheduler) Start(ctx context.Context, waitGroup *sync.WaitGroup) {
	if scheduler.maxAttempts == 0 {
		return
	}
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		scheduler.run(ctx)
	}()
}

func (scheduler *DLQRetryScheduler) run(ctx context.Context) {
	scheduler.retry(ctx)
	ticker := time.NewTicker(scheduler.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.Info("dead letter retry scheduler stopped")
			return
		case <-ticker.C:
			scheduler.retry(ctx)
		}
	}
}

func (scheduler *DLQRetryScheduler) retry(ctx context.Context) {
	eventIDs, err := scheduler.store.ClaimRetryableDeadLetters(ctx, 10, scheduler.maxAttempts)
	if err != nil {
		slog.Error("claim dead letters for automatic retry failed", "error", err)
		return
	}
	for _, eventID := range eventIDs {
		if err := scheduler.store.ReplayDeadLetter(ctx, eventID); err != nil {
			slog.Error("automatic dead letter replay failed", "event_id", eventID, "error", err)
			if rescheduleErr := scheduler.store.RescheduleDeadLetterRetry(ctx, eventID, err); rescheduleErr != nil {
				slog.Error("reschedule automatic dead letter replay failed", "event_id", eventID, "error", rescheduleErr)
			}
			continue
		}
		slog.Info("automatic dead letter replay accepted", "event_id", eventID)
	}
}
