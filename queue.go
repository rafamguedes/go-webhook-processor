package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var ErrEventQueueFull = errors.New("event queue is full")

type QueueStats struct {
	Length   int
	Capacity int
}

type EventPublisher interface {
	Publish(ctx context.Context, event Event) error
	TryPublish(ctx context.Context, event Event) error
}

type EventConsumer interface {
	Events() <-chan Event
}

type EventQueue interface {
	EventPublisher
	EventConsumer
	Stats() QueueStats
	Close() error
}

var _ EventQueue = (*MemoryEventQueue)(nil)

type MemoryEventQueue struct {
	events    chan Event
	closeOnce sync.Once
}

func NewConfiguredEventQueue(config Config) (EventQueue, error) {
	switch config.QueueProvider {
	case QueueProviderMemory:
		return NewMemoryEventQueue(config.QueueSize), nil
	case QueueProviderRabbitMQ:
		return nil, fmt.Errorf("queue provider rabbitmq is not implemented yet")
	default:
		return nil, fmt.Errorf("unsupported queue provider %q", config.QueueProvider)
	}
}

func NewMemoryEventQueue(capacity int) *MemoryEventQueue {
	return &MemoryEventQueue{
		events: make(chan Event, capacity),
	}
}

func (queue *MemoryEventQueue) Publish(ctx context.Context, event Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case queue.events <- event:
		return nil
	}
}

func (queue *MemoryEventQueue) TryPublish(ctx context.Context, event Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	select {
	case queue.events <- event:
		return nil
	default:
		return ErrEventQueueFull
	}
}

func (queue *MemoryEventQueue) Events() <-chan Event {
	return queue.events
}

func (queue *MemoryEventQueue) Stats() QueueStats {
	return QueueStats{
		Length:   len(queue.events),
		Capacity: cap(queue.events),
	}
}

func (queue *MemoryEventQueue) Close() error {
	queue.closeOnce.Do(func() {
		close(queue.events)
	})
	return nil
}
