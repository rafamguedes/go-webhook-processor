package main

import "context"

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
