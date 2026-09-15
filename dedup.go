package main

import "sync"

type EventDeduplicator struct {
	capacity int
	seen     map[string]struct{}
	order    []string
	mu       sync.Mutex
}

func NewEventDeduplicator(capacity int) *EventDeduplicator {
	return &EventDeduplicator{
		capacity: capacity,
		seen:     make(map[string]struct{}, capacity),
		order:    make([]string, 0, capacity),
	}
}

func (deduplicator *EventDeduplicator) Remember(eventID string) bool {
	deduplicator.mu.Lock()
	defer deduplicator.mu.Unlock()

	if _, exists := deduplicator.seen[eventID]; exists {
		return false
	}

	if len(deduplicator.order) == deduplicator.capacity {
		oldest := deduplicator.order[0]
		delete(deduplicator.seen, oldest)
		deduplicator.order = deduplicator.order[1:]
	}

	deduplicator.seen[eventID] = struct{}{}
	deduplicator.order = append(deduplicator.order, eventID)

	return true
}

func (deduplicator *EventDeduplicator) Len() int {
	deduplicator.mu.Lock()
	defer deduplicator.mu.Unlock()

	return len(deduplicator.order)
}
