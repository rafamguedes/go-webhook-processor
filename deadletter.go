package main

import (
	"context"
	"sync"
	"time"
)

type DeadLetterRepository interface {
	Add(ctx context.Context, event Event, err error, attempts int) error
	Snapshot(ctx context.Context) (DeadLetterResponse, error)
}

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
	return &DeadLetterStore{capacity: capacity, items: make([]DeadLetter, 0, capacity)}
}

func (store *DeadLetterStore) Add(_ context.Context, event Event, processingError error, attempts int) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.items) == store.capacity {
		store.items = store.items[1:]
	}
	store.items = append(store.items, DeadLetter{Event: event, Error: processingError.Error(), Attempts: attempts, FailedAt: time.Now().UTC()})
	return nil
}

func (store *DeadLetterStore) Snapshot(_ context.Context) (DeadLetterResponse, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	items := make([]DeadLetter, len(store.items))
	copy(items, store.items)
	return DeadLetterResponse{Count: len(items), Items: items}, nil
}

type PersistentDeadLetterStore struct {
	store    *EventStore
	capacity int
}

func NewPersistentDeadLetterStore(store *EventStore, capacity int) *PersistentDeadLetterStore {
	return &PersistentDeadLetterStore{store: store, capacity: capacity}
}

func (store *PersistentDeadLetterStore) Add(ctx context.Context, event Event, processingError error, attempts int) error {
	return store.store.SaveDeadLetter(ctx, event, processingError, attempts)
}

func (store *PersistentDeadLetterStore) Snapshot(ctx context.Context) (DeadLetterResponse, error) {
	items, err := store.store.ListDeadLetters(ctx, store.capacity)
	if err != nil {
		return DeadLetterResponse{}, err
	}
	return DeadLetterResponse{Count: len(items), Items: items}, nil
}
