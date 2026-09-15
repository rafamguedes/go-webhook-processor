package main

import (
	"sync"
	"time"
)

type DeadLetter struct {
	Event    Event     `json:"event"`
	Error    string    `json:"error"`
	Attempts int       `json:"attempts"`
	FailedAt time.Time `json:"failedAt"`
}

type DeadLetterResponse struct {
	Count int          `json:"count"`
	Items []DeadLetter `json:"items"`
}

type DeadLetterStore struct {
	capacity int
	items    []DeadLetter
	mu       sync.Mutex
}

func NewDeadLetterStore(capacity int) *DeadLetterStore {
	return &DeadLetterStore{
		capacity: capacity,
		items:    make([]DeadLetter, 0, capacity),
	}
}

func (store *DeadLetterStore) Add(event Event, err error, attempts int) {
	store.mu.Lock()
	defer store.mu.Unlock()

	if len(store.items) == store.capacity {
		store.items = store.items[1:]
	}

	store.items = append(store.items, DeadLetter{
		Event:    event,
		Error:    err.Error(),
		Attempts: attempts,
		FailedAt: time.Now().UTC(),
	})
}

func (store *DeadLetterStore) Snapshot() DeadLetterResponse {
	store.mu.Lock()
	defer store.mu.Unlock()

	items := make([]DeadLetter, len(store.items))
	copy(items, store.items)

	return DeadLetterResponse{
		Count: len(items),
		Items: items,
	}
}
