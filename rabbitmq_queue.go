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
	url               string
	queueName         string
	workerCount       int
	connectTimeout    time.Duration
	reconnectInterval time.Duration
	events            chan EventDelivery
	consumeContext    context.Context
	consumeCancel     context.CancelFunc
	consumeWorkers    sync.WaitGroup
	publishMu         sync.Mutex
	publishConn       *amqp.Connection
	publishChannel    *amqp.Channel
	consumerMu        sync.Mutex
	consumerConn      *amqp.Connection
	consumerChannel   *amqp.Channel
	stopOnce          sync.Once
	closeOnce         sync.Once
	closeErr          error
}

var _ EventQueue = (*RabbitMQEventQueue)(nil)

func OpenRabbitMQEventQueue(config Config) (*RabbitMQEventQueue, error) {
	consumeContext, consumeCancel := context.WithCancel(context.Background())
	queue := &RabbitMQEventQueue{
		url:               config.RabbitMQURL,
		queueName:         config.RabbitMQQueue,
		workerCount:       config.WorkerCount,
		connectTimeout:    config.RabbitMQConnectTimeout(),
		reconnectInterval: config.RabbitMQReconnectInterval(),
		events:            make(chan EventDelivery, config.QueueSize),
		consumeContext:    consumeContext,
		consumeCancel:     consumeCancel,
	}

	queue.consumeWorkers.Add(1)
	go queue.consumeLoop()
	return queue, nil
}

func declareRabbitMQQueue(channel *amqp.Channel, queueName string) (amqp.Queue, error) {
	queue, err := channel.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("declare RabbitMQ queue %q: %w", queueName, err)
	}
	return queue, nil
}

func (queue *RabbitMQEventQueue) dial() (*amqp.Connection, error) {
	connection, err := amqp.DialConfig(queue.url, amqp.Config{Dial: amqp.DefaultDial(queue.connectTimeout)})
	if err != nil {
		return nil, fmt.Errorf("connect RabbitMQ: %w", err)
	}
	return connection, nil
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

	if err := queue.ensurePublisherLocked(); err != nil {
		return err
	}

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
		queue.invalidatePublisherLocked()
		return fmt.Errorf("publish RabbitMQ event: %w", err)
	}
	if confirmation == nil {
		queue.invalidatePublisherLocked()
		return fmt.Errorf("RabbitMQ publisher confirmation is unavailable")
	}

	confirmed, err := confirmation.WaitContext(ctx)
	if err != nil {
		queue.invalidatePublisherLocked()
		return fmt.Errorf("wait for RabbitMQ publisher confirmation: %w", err)
	}
	if !confirmed {
		return fmt.Errorf("RabbitMQ rejected published event")
	}
	return nil
}

func (queue *RabbitMQEventQueue) ensurePublisherLocked() error {
	if queue.publishConn != nil && !queue.publishConn.IsClosed() && queue.publishChannel != nil && !queue.publishChannel.IsClosed() {
		return nil
	}
	queue.invalidatePublisherLocked()

	connection, err := queue.dial()
	if err != nil {
		return fmt.Errorf("connect RabbitMQ publisher: %w", err)
	}
	channel, err := connection.Channel()
	if err != nil {
		connection.Close()
		return fmt.Errorf("open RabbitMQ publisher channel: %w", err)
	}
	if _, err := declareRabbitMQQueue(channel, queue.queueName); err != nil {
		channel.Close()
		connection.Close()
		return err
	}
	if err := channel.Confirm(false); err != nil {
		channel.Close()
		connection.Close()
		return fmt.Errorf("enable RabbitMQ publisher confirms: %w", err)
	}

	queue.publishConn = connection
	queue.publishChannel = channel
	slog.Info("RabbitMQ publisher connected", "queue", queue.queueName)
	return nil
}

func (queue *RabbitMQEventQueue) invalidatePublisherLocked() {
	closeAMQPChannel(queue.publishChannel)
	closeAMQPConnection(queue.publishConn)
	queue.publishChannel = nil
	queue.publishConn = nil
}

func (queue *RabbitMQEventQueue) consumeLoop() {
	defer queue.consumeWorkers.Done()
	defer close(queue.events)

	for queue.consumeContext.Err() == nil {
		connection, channel, deliveries, err := queue.openConsumer()
		if err != nil {
			slog.Warn("connect RabbitMQ consumer failed", "queue", queue.queueName, "error", err)
			if !waitForRetry(queue.consumeContext, queue.reconnectInterval) {
				return
			}
			continue
		}

		queue.setConsumer(connection, channel)
		slog.Info("RabbitMQ consumer connected", "queue", queue.queueName)
		queue.forwardDeliveries(queue.consumeContext, deliveries)

		if queue.consumeContext.Err() != nil {
			return
		}
		queue.clearConsumer(connection, channel)
		closeAMQPChannel(channel)
		closeAMQPConnection(connection)
		slog.Warn("RabbitMQ consumer disconnected", "queue", queue.queueName)
		if !waitForRetry(queue.consumeContext, queue.reconnectInterval) {
			return
		}
	}
}

func (queue *RabbitMQEventQueue) openConsumer() (*amqp.Connection, *amqp.Channel, <-chan amqp.Delivery, error) {
	connection, err := queue.dial()
	if err != nil {
		return nil, nil, nil, err
	}
	channel, err := connection.Channel()
	if err != nil {
		connection.Close()
		return nil, nil, nil, fmt.Errorf("open RabbitMQ consumer channel: %w", err)
	}
	if _, err := declareRabbitMQQueue(channel, queue.queueName); err != nil {
		channel.Close()
		connection.Close()
		return nil, nil, nil, err
	}
	if err := channel.Qos(queue.workerCount, 0, false); err != nil {
		channel.Close()
		connection.Close()
		return nil, nil, nil, fmt.Errorf("configure RabbitMQ prefetch: %w", err)
	}
	deliveries, err := channel.ConsumeWithContext(queue.consumeContext, queue.queueName, "", false, false, false, false, nil)
	if err != nil {
		channel.Close()
		connection.Close()
		return nil, nil, nil, fmt.Errorf("start RabbitMQ consumer: %w", err)
	}
	return connection, channel, deliveries, nil
}

func (queue *RabbitMQEventQueue) setConsumer(connection *amqp.Connection, channel *amqp.Channel) {
	queue.consumerMu.Lock()
	defer queue.consumerMu.Unlock()
	queue.consumerConn = connection
	queue.consumerChannel = channel
}

func (queue *RabbitMQEventQueue) clearConsumer(connection *amqp.Connection, channel *amqp.Channel) {
	queue.consumerMu.Lock()
	defer queue.consumerMu.Unlock()
	if queue.consumerConn == connection {
		queue.consumerConn = nil
	}
	if queue.consumerChannel == channel {
		queue.consumerChannel = nil
	}
}

func (queue *RabbitMQEventQueue) Events() <-chan EventDelivery {
	return queue.events
}

func (queue *RabbitMQEventQueue) Stats() QueueStats {
	return QueueStats{Length: len(queue.events), Capacity: cap(queue.events)}
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

		queue.consumerMu.Lock()
		consumerChannel := queue.consumerChannel
		consumerConn := queue.consumerConn
		queue.consumerChannel = nil
		queue.consumerConn = nil
		queue.consumerMu.Unlock()

		queue.publishMu.Lock()
		publishChannel := queue.publishChannel
		publishConn := queue.publishConn
		queue.publishChannel = nil
		queue.publishConn = nil
		queue.publishMu.Unlock()

		queue.closeErr = errors.Join(
			closeAMQPChannel(consumerChannel),
			closeAMQPConnection(consumerConn),
			closeAMQPChannel(publishChannel),
			closeAMQPConnection(publishConn),
		)
	})
	return queue.closeErr
}

func (queue *RabbitMQEventQueue) forwardDeliveries(ctx context.Context, deliveries <-chan amqp.Delivery) {
	for {
		select {
		case <-ctx.Done():
			return
		case delivery, ok := <-deliveries:
			if !ok {
				return
			}

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
				ack:   func() error { return currentDelivery.Ack(false) },
				nack:  func(requeue bool) error { return currentDelivery.Nack(false, requeue) },
			}
			select {
			case queue.events <- eventDelivery:
			case <-ctx.Done():
				if err := currentDelivery.Nack(false, true); err != nil {
					slog.Error("requeue RabbitMQ event during shutdown failed", "event_id", event.ID, "error", err)
				}
				return
			}
		}
	}
}

func waitForRetry(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
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
