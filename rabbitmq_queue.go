package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQEventQueue struct {
	queueName       string
	publishConn     *amqp.Connection
	publishChannel  *amqp.Channel
	consumerConn    *amqp.Connection
	consumerChannel *amqp.Channel
	events          chan EventDelivery
	consumeCancel   context.CancelFunc
	consumeWorkers  sync.WaitGroup
	publishMu       sync.Mutex
	stopOnce        sync.Once
	closeOnce       sync.Once
	closeErr        error
}

var _ EventQueue = (*RabbitMQEventQueue)(nil)

func OpenRabbitMQEventQueue(config Config) (*RabbitMQEventQueue, error) {
	publishConn, err := amqp.Dial(config.RabbitMQURL)
	if err != nil {
		return nil, fmt.Errorf("connect RabbitMQ publisher: %w", err)
	}

	publishChannel, err := publishConn.Channel()
	if err != nil {
		publishConn.Close()
		return nil, fmt.Errorf("open RabbitMQ publisher channel: %w", err)
	}

	if _, err := declareRabbitMQQueue(publishChannel, config.RabbitMQQueue); err != nil {
		publishChannel.Close()
		publishConn.Close()
		return nil, err
	}
	if err := publishChannel.Confirm(false); err != nil {
		publishChannel.Close()
		publishConn.Close()
		return nil, fmt.Errorf("enable RabbitMQ publisher confirms: %w", err)
	}

	consumerConn, err := amqp.Dial(config.RabbitMQURL)
	if err != nil {
		publishChannel.Close()
		publishConn.Close()
		return nil, fmt.Errorf("connect RabbitMQ consumer: %w", err)
	}

	consumerChannel, err := consumerConn.Channel()
	if err != nil {
		consumerConn.Close()
		publishChannel.Close()
		publishConn.Close()
		return nil, fmt.Errorf("open RabbitMQ consumer channel: %w", err)
	}

	if _, err := declareRabbitMQQueue(consumerChannel, config.RabbitMQQueue); err != nil {
		consumerChannel.Close()
		consumerConn.Close()
		publishChannel.Close()
		publishConn.Close()
		return nil, err
	}
	if err := consumerChannel.Qos(config.WorkerCount, 0, false); err != nil {
		consumerChannel.Close()
		consumerConn.Close()
		publishChannel.Close()
		publishConn.Close()
		return nil, fmt.Errorf("configure RabbitMQ prefetch: %w", err)
	}

	consumeContext, consumeCancel := context.WithCancel(context.Background())
	deliveries, err := consumerChannel.ConsumeWithContext(
		consumeContext,
		config.RabbitMQQueue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		consumeCancel()
		consumerChannel.Close()
		consumerConn.Close()
		publishChannel.Close()
		publishConn.Close()
		return nil, fmt.Errorf("start RabbitMQ consumer: %w", err)
	}

	queue := &RabbitMQEventQueue{
		queueName:       config.RabbitMQQueue,
		publishConn:     publishConn,
		publishChannel:  publishChannel,
		consumerConn:    consumerConn,
		consumerChannel: consumerChannel,
		events:          make(chan EventDelivery, config.QueueSize),
		consumeCancel:   consumeCancel,
	}
	queue.consumeWorkers.Add(1)
	go queue.forwardDeliveries(consumeContext, deliveries)

	return queue, nil
}

func declareRabbitMQQueue(channel *amqp.Channel, queueName string) (amqp.Queue, error) {
	queue, err := channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("declare RabbitMQ queue %q: %w", queueName, err)
	}
	return queue, nil
}

func (queue *RabbitMQEventQueue) Publish(ctx context.Context, event Event) error {
	return queue.publish(ctx, event)
}

func (queue *RabbitMQEventQueue) TryPublish(ctx context.Context, event Event) error {
	return queue.publish(ctx, event)
}

func (queue *RabbitMQEventQueue) publish(ctx context.Context, event Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal RabbitMQ event: %w", err)
	}

	queue.publishMu.Lock()
	defer queue.publishMu.Unlock()

	confirmation, err := queue.publishChannel.PublishWithDeferredConfirmWithContext(
		ctx,
		"",
		queue.queueName,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    event.ID,
			Timestamp:    time.Now().UTC(),
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("publish RabbitMQ event: %w", err)
	}
	if confirmation == nil {
		return fmt.Errorf("RabbitMQ publisher confirmation is unavailable")
	}

	confirmed, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("wait for RabbitMQ publisher confirmation: %w", err)
	}
	if !confirmed {
		return fmt.Errorf("RabbitMQ rejected published event")
	}

	return nil
}

func (queue *RabbitMQEventQueue) Events() <-chan EventDelivery {
	return queue.events
}

func (queue *RabbitMQEventQueue) Stats() QueueStats {
	return QueueStats{
		Length:   len(queue.events),
		Capacity: cap(queue.events),
	}
}

func (queue *RabbitMQEventQueue) StopConsuming() error {
	queue.stopOnce.Do(func() {
		queue.consumeCancel()
		queue.consumeWorkers.Wait()
	})
	return nil
}

func (queue *RabbitMQEventQueue) Close() error {
	queue.closeOnce.Do(func() {
		queue.StopConsuming()
		queue.closeErr = errors.Join(
			closeAMQPChannel(queue.consumerChannel),
			closeAMQPConnection(queue.consumerConn),
			closeAMQPChannel(queue.publishChannel),
			closeAMQPConnection(queue.publishConn),
		)
	})
	return queue.closeErr
}

func (queue *RabbitMQEventQueue) forwardDeliveries(ctx context.Context, deliveries <-chan amqp.Delivery) {
	defer queue.consumeWorkers.Done()
	defer close(queue.events)

	for delivery := range deliveries {
		var event Event
		if err := json.Unmarshal(delivery.Body, &event); err != nil {
			slog.Error("decode RabbitMQ event failed", "message_id", delivery.MessageId, "error", err)
			if nackErr := delivery.Nack(false, false); nackErr != nil {
				slog.Error("reject invalid RabbitMQ event failed", "message_id", delivery.MessageId, "error", nackErr)
			}
			continue
		}

		currentDelivery := delivery
		eventDelivery := EventDelivery{
			Event: event,
			ack: func() error {
				return currentDelivery.Ack(false)
			},
			nack: func(requeue bool) error {
				return currentDelivery.Nack(false, requeue)
			},
		}

		select {
		case queue.events <- eventDelivery:
		case <-ctx.Done():
			if err := currentDelivery.Nack(false, true); err != nil {
				slog.Error("requeue RabbitMQ event during shutdown failed", "event_id", event.ID, "error", err)
			}
			continue
		}
	}
}

func closeAMQPChannel(channel *amqp.Channel) error {
	if channel == nil || channel.IsClosed() {
		return nil
	}
	return channel.Close()
}

func closeAMQPConnection(connection *amqp.Connection) error {
	if connection == nil || connection.IsClosed() {
		return nil
	}
	return connection.Close()
}
