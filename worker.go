package main

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type eventProcessor func(workerID int, event Event) error

func startWorkers(count int, eventQueue <-chan Event, workers *sync.WaitGroup, config Config, metrics *Metrics) {
	for workerID := 1; workerID <= count; workerID++ {
		workers.Add(1)
		go worker(workerID, eventQueue, workers, config, metrics)
	}
}

func worker(workerID int, eventQueue <-chan Event, workers *sync.WaitGroup, config Config, metrics *Metrics) {
	defer workers.Done()

	for event := range eventQueue {
		processEventWithRetry(workerID, event, config, processEvent, time.Sleep, metrics)
	}

	slog.Info("worker stopped", "worker_id", workerID)
}

func processEventWithRetry(workerID int, event Event, config Config, processor eventProcessor, sleep func(time.Duration), metrics *Metrics) bool {
	maxAttempts := config.MaxRetries + 1

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := processor(workerID, event)
		if err == nil {
			metrics.IncEventsProcessed()
			slog.Info("event processing succeeded", "worker_id", workerID, "event_id", event.ID, "event_type", event.Type, "attempt", attempt)
			return true
		}

		if attempt == maxAttempts {
			metrics.IncEventsFailedPermanent()
			slog.Error("event processing failed permanently", "worker_id", workerID, "event_id", event.ID, "event_type", event.Type, "attempt", attempt, "max_attempts", maxAttempts, "error", err)
			return false
		}

		metrics.IncEventRetries()
		backoff := config.RetryBackoff(attempt)
		slog.Warn("event processing failed; retrying", "worker_id", workerID, "event_id", event.ID, "event_type", event.Type, "attempt", attempt, "max_attempts", maxAttempts, "backoff", backoff.String(), "error", err)
		sleep(backoff)
	}

	return false
}

func processEvent(workerID int, event Event) error {
	slog.Info("event processing started", "worker_id", workerID, "event_id", event.ID, "event_type", event.Type)

	time.Sleep(2 * time.Second)

	if shouldSimulateFailure(event) {
		return fmt.Errorf("simulated processing failure")
	}

	slog.Info("event processing finished", "worker_id", workerID, "event_id", event.ID, "event_type", event.Type)
	return nil
}

func shouldSimulateFailure(event Event) bool {
	value, exists := event.Payload["simulateFailure"]
	if !exists {
		return false
	}

	shouldFail, ok := value.(bool)
	return ok && shouldFail
}
