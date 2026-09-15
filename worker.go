package main

import (
	"log"
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

	log.Printf("worker=%d stopped", workerID)
}

func processEvent(workerID int, event Event) {
	log.Printf("worker=%d processing event: id=%s type=%s", workerID, event.ID, event.Type)

	time.Sleep(2 * time.Second)

	log.Printf("worker=%d finished event: id=%s type=%s", workerID, event.ID, event.Type)
}
