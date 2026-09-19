package main

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

type DLQRetryScheduler struct {
	store       *EventStore
	interval    time.Duration
	maxAttempts int
	metrics     *Metrics
}

func NewDLQRetryScheduler(store *EventStore, config Config, metrics *Metrics) *DLQRetryScheduler {
	return &DLQRetryScheduler{
		store:       store,
		interval:    time.Duration(config.DLQAutoRetryIntervalSeconds) * time.Second,
		maxAttempts: config.DLQAutoRetryMaxAttempts,
		metrics:     metrics,
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
	items, err := scheduler.store.ClaimRetryableDeadLetterDetails(ctx, 10, scheduler.maxAttempts)
	if err != nil {
		slog.Error("claim dead letters for automatic retry failed", "error", err)
		return
	}
	for _, item := range items {
		if scheduler.metrics != nil {
			scheduler.metrics.IncDeadLetterAutoRetries()
		}
		if err := scheduler.store.ReplayDeadLetter(ctx, item.EventID); err != nil {
			if scheduler.metrics != nil {
				scheduler.metrics.IncDeadLetterAutoRetryFailed()
			}
			delay := scheduler.retryDelay(item.ReplayAttempt)
			slog.Error("automatic dead letter replay failed", "event_id", item.EventID, "retry_delay", delay.String(), "error", err)
			if rescheduleErr := scheduler.store.RescheduleDeadLetterRetryWithDelay(ctx, item.EventID, err, delay); rescheduleErr != nil {
				slog.Error("reschedule automatic dead letter replay failed", "event_id", item.EventID, "error", rescheduleErr)
			}
			continue
		}
		slog.Info("automatic dead letter replay accepted", "event_id", item.EventID, "replay_attempt", item.ReplayAttempt)
	}
}

func (scheduler *DLQRetryScheduler) retryDelay(attempt int) time.Duration {
	delay := scheduler.interval
	for step := 1; step < attempt; step++ {
		delay *= 2
		if delay >= 24*time.Hour {
			return 24 * time.Hour
		}
	}
	jitterLimit := delay / 4
	if jitterLimit == 0 {
		return delay
	}
	return delay + time.Duration(rand.Int63n(int64(jitterLimit)+1))
}
