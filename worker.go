package main

import (
	"log/slog"
	"sync"
	"time"
)

func startWorkers(count int, eventQueue <-chan Event, workers *sync.WaitGroup) {
	for workerID := 1; workerID <= count; workerID++ {
		workers.Add(1)
		go worker(workerID, eventQueue, workers)
	}
}

func worker(workerID int, eventQueue <-chan Event, workers *sync.WaitGroup) {
	defer workers.Done()

	for event := range eventQueue {
		processEvent(workerID, event)
	}

	slog.Info("worker stopped", "worker_id", workerID)
}

func processEvent(workerID int, event Event) {
	slog.Info("event processing started", "worker_id", workerID, "event_id", event.ID, "event_type", event.Type)

	time.Sleep(2 * time.Second)

	slog.Info("event processing finished", "worker_id", workerID, "event_id", event.ID, "event_type", event.Type)
}
