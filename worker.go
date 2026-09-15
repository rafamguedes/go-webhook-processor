package main

import (
	"log"
	"time"
)

func startWorkers(count int, eventQueue <-chan Event) {
	for workerID := 1; workerID <= count; workerID++ {
		go worker(workerID, eventQueue)
	}
}

func worker(workerID int, eventQueue <-chan Event) {
	for event := range eventQueue {
		processEvent(workerID, event)
	}
}

func processEvent(workerID int, event Event) {
	log.Printf("worker=%d processing event: id=%s type=%s", workerID, event.ID, event.Type)

	time.Sleep(2 * time.Second)

	log.Printf("worker=%d finished event: id=%s type=%s", workerID, event.ID, event.Type)
}
