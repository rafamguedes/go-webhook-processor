package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type eventProcessor func(workerID int, event Event) error

func startWorkers(count int, eventQueue EventConsumer, workers *sync.WaitGroup, config Config, metrics *Metrics, deadLetters DeadLetterRepository, eventStore *EventStore) {
	for workerID := 1; workerID <= count; workerID++ {
		workers.Add(1)
		go worker(workerID, eventQueue, workers, config, metrics, deadLetters, eventStore)
	}
}

func worker(workerID int, eventQueue EventConsumer, workers *sync.WaitGroup, config Config, metrics *Metrics, deadLetters DeadLetterRepository, eventStore *EventStore) {
	defer workers.Done()

	for delivery := range eventQueue.Events() {
		handleDelivery(workerID, delivery, config, metrics, deadLetters, eventStore, processEvent, time.Sleep)
	}

	slog.Info("worker stopped", "worker_id", workerID)
}

func handleDelivery(workerID int, delivery EventDelivery, config Config, metrics *Metrics, deadLetters DeadLetterRepository, eventStore *EventStore, processor eventProcessor, sleep func(time.Duration)) {
	event := delivery.Event
	claim, err := eventStore.ClaimForProcessing(context.Background(), event.ID, config.ProcessingLease())
	if err != nil {
		slog.Error("claim event for processing failed", "worker_id", workerID, "event_id", event.ID, "request_id", event.RequestID, "error", err)
		nackDelivery(delivery, true, workerID, event.ID)
		return
	}

	switch claim {
	case EventClaimFinal:
		metrics.IncEventsSkippedDuplicate()
		slog.Info("duplicate event delivery skipped", "worker_id", workerID, "event_id", event.ID, "request_id", event.RequestID)
		ackDelivery(delivery, workerID, event.ID)
		return
	case EventClaimInProgress:
		slog.Info("event already processing; requeueing delivery", "worker_id", workerID, "event_id", event.ID, "request_id", event.RequestID)
		sleep(config.ProcessingRequeueDelay())
		nackDelivery(delivery, true, workerID, event.ID)
		return
	case EventClaimMissing:
		slog.Error("event delivery has no persisted event", "worker_id", workerID, "event_id", event.ID, "request_id", event.RequestID)
		nackDelivery(delivery, false, workerID, event.ID)
		return
	case EventClaimed:
		_, terminalPersisted := processEventWithRetry(workerID, event, config, processor, sleep, metrics, deadLetters, eventStore)
		if terminalPersisted {
			ackDelivery(delivery, workerID, event.ID)
			return
		}
		nackDelivery(delivery, true, workerID, event.ID)
	}
}

func ackDelivery(delivery EventDelivery, workerID int, eventID string) {
	if err := delivery.Ack(); err != nil {
		slog.Error("acknowledge event failed", "worker_id", workerID, "event_id", eventID, "error", err)
	}
}

func nackDelivery(delivery EventDelivery, requeue bool, workerID int, eventID string) {
	if err := delivery.Nack(requeue); err != nil {
		slog.Error("reject event delivery failed", "worker_id", workerID, "event_id", eventID, "requeue", requeue, "error", err)
	}
}
func processEventWithRetry(workerID int, event Event, config Config, processor eventProcessor, sleep func(time.Duration), metrics *Metrics, deadLetters DeadLetterRepository, eventStore *EventStore) (bool, bool) {
	maxAttempts := config.MaxRetries + 1

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := processor(workerID, event)
		if err == nil {
			metrics.IncEventsProcessed()
			if markErr := eventStore.MarkProcessed(context.Background(), event.ID, attempt); markErr != nil {
				slog.Error("mark event processed failed", "event_id", event.ID, "error", markErr)
				return true, false
			}
			slog.Info("event processing succeeded", "worker_id", workerID, "event_id", event.ID, "request_id", event.RequestID, "event_type", event.Type, "attempt", attempt)
			return true, true
		}

		if attempt == maxAttempts {
			metrics.IncEventsFailedPermanent()
			if deadLetterErr := deadLetters.Add(context.Background(), event, err, attempt); deadLetterErr != nil {
				slog.Error("save dead letter failed", "event_id", event.ID, "error", deadLetterErr)
				return false, false
			}
			if markErr := eventStore.MarkFailed(context.Background(), event.ID, attempt, err); markErr != nil {
				slog.Error("mark event failed failed", "event_id", event.ID, "error", markErr)
				return false, false
			}
			slog.Error("event processing failed permanently", "worker_id", workerID, "event_id", event.ID, "request_id", event.RequestID, "event_type", event.Type, "attempt", attempt, "max_attempts", maxAttempts, "error", err)
			return false, true
		}

		metrics.IncEventRetries()
		backoff := config.RetryBackoff(attempt)
		slog.Warn("event processing failed; retrying", "worker_id", workerID, "event_id", event.ID, "request_id", event.RequestID, "event_type", event.Type, "attempt", attempt, "max_attempts", maxAttempts, "backoff", backoff.String(), "error", err)
		sleep(backoff)
	}

	return false, false
}

func processEvent(workerID int, event Event) error {
	slog.Info("event processing started", "worker_id", workerID, "event_id", event.ID, "request_id", event.RequestID, "event_type", event.Type)

	time.Sleep(2 * time.Second)

	if shouldSimulateFailure(event) {
		return fmt.Errorf("simulated processing failure")
	}

	slog.Info("event processing finished", "worker_id", workerID, "event_id", event.ID, "request_id", event.RequestID, "event_type", event.Type)
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
