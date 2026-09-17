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

type EventDelivery struct {
	Event Event
	ack   func() error
	nack  func(requeue bool) error
}

func (delivery EventDelivery) Ack() error {
	if delivery.ack == nil {
		return nil
	}
	return delivery.ack()
}

func (delivery EventDelivery) Nack(requeue bool) error {
	if delivery.nack == nil {
		return nil
	}
	return delivery.nack(requeue)
}

type EventPublisher interface {
	Publish(ctx context.Context, event Event) error
	TryPublish(ctx context.Context, event Event) error
}

type EventConsumer interface {
	Events() <-chan EventDelivery
}

type EventQueue interface {
	EventPublisher
	EventConsumer
	Stats() QueueStats
	StopConsuming() error
	Close() error
}

type MemoryEventQueue struct {
	events   chan EventDelivery
	stopOnce sync.Once
}

var _ EventQueue = (*MemoryEventQueue)(nil)

func NewConfiguredEventQueue(config Config) (EventQueue, error) {
	switch config.QueueProvider {
	case QueueProviderMemory:
		return NewMemoryEventQueue(config.QueueSize), nil
	case QueueProviderRabbitMQ:
		return OpenRabbitMQEventQueue(config)
	default:
		return nil, fmt.Errorf("unsupported queue provider %q", config.QueueProvider)
	}
}

func NewMemoryEventQueue(capacity int) *MemoryEventQueue {
	return &MemoryEventQueue{
		events: make(chan EventDelivery, capacity),
	}
}

func (queue *MemoryEventQueue) Publish(ctx context.Context, event Event) error {
	delivery := EventDelivery{Event: event}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case queue.events <- delivery:
		return nil
	}
}

func (queue *MemoryEventQueue) TryPublish(ctx context.Context, event Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	delivery := EventDelivery{Event: event}
	select {
	case queue.events <- delivery:
		return nil
	default:
		return ErrEventQueueFull
	}
}

func (queue *MemoryEventQueue) Events() <-chan EventDelivery {
	return queue.events
}

func (queue *MemoryEventQueue) Stats() QueueStats {
	return QueueStats{
		Length:   len(queue.events),
		Capacity: cap(queue.events),
	}
}

func (queue *MemoryEventQueue) StopConsuming() error {
	queue.stopOnce.Do(func() {
		close(queue.events)
	})
	return nil
}

func (queue *MemoryEventQueue) Close() error {
	return queue.StopConsuming()
}
