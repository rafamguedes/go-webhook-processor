package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestRabbitMQEventQueuePublishesAndAcknowledgesEvent(t *testing.T) {
	rabbitMQURL := os.Getenv("RABBITMQ_INTEGRATION_URL")
	if rabbitMQURL == "" {
		t.Skip("RABBITMQ_INTEGRATION_URL is not configured")
	}

	config := testConfig()
	config.RabbitMQURL = rabbitMQURL
	config.RabbitMQQueue = fmt.Sprintf("webhook.events.integration.%d", time.Now().UnixNano())
	config.QueueSize = 2
	config.WorkerCount = 1
	config.RabbitMQReconnectMs = 50

	queue, err := OpenRabbitMQEventQueue(config)
	if err != nil {
		t.Fatalf("failed to open RabbitMQ queue: %v", err)
	}
	t.Cleanup(func() {
		queue.StopConsuming()
		queue.publishMu.Lock()
		if queue.publishChannel != nil && !queue.publishChannel.IsClosed() {
			queue.publishChannel.QueueDelete(config.RabbitMQQueue, false, false, false)
		}
		queue.publishMu.Unlock()
		queue.Close()
	})

	assertRabbitMQDelivery(t, queue, testEvent())
}

func TestRabbitMQEventQueueReconnectsPublisherAndConsumer(t *testing.T) {
	rabbitMQURL := os.Getenv("RABBITMQ_INTEGRATION_URL")
	if rabbitMQURL == "" {
		t.Skip("RABBITMQ_INTEGRATION_URL is not configured")
	}

	config := testConfig()
	config.RabbitMQURL = rabbitMQURL
	config.RabbitMQQueue = fmt.Sprintf("webhook.events.reconnect.%d", time.Now().UnixNano())
	config.QueueSize = 2
	config.WorkerCount = 1
	config.RabbitMQReconnectMs = 50

	queue, err := OpenRabbitMQEventQueue(config)
	if err != nil {
		t.Fatalf("failed to open RabbitMQ queue: %v", err)
	}
	t.Cleanup(func() {
		queue.StopConsuming()
		queue.publishMu.Lock()
		if queue.publishChannel != nil && !queue.publishChannel.IsClosed() {
			queue.publishChannel.QueueDelete(config.RabbitMQQueue, false, false, false)
		}
		queue.publishMu.Unlock()
		queue.Close()
	})

	assertRabbitMQDelivery(t, queue, Event{ID: "evt-before-reconnect", Type: "test", Payload: map[string]any{}})

	queue.publishMu.Lock()
	closeAMQPConnection(queue.publishConn)
	queue.publishMu.Unlock()

	queue.consumerMu.Lock()
	closeAMQPConnection(queue.consumerConn)
	queue.consumerMu.Unlock()

	assertRabbitMQDelivery(t, queue, Event{ID: "evt-after-reconnect", Type: "test", Payload: map[string]any{}})
}

func assertRabbitMQDelivery(t *testing.T, queue *RabbitMQEventQueue, event Event) {
	t.Helper()
	deadline, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	for {
		err := queue.Publish(deadline, event)
		if err == nil {
			break
		}
		select {
		case <-deadline.Done():
			t.Fatalf("failed to publish RabbitMQ event before timeout: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}

	select {
	case delivery := <-queue.Events():
		if delivery.Event.ID != event.ID {
			t.Fatalf("expected event %s, got %s", event.ID, delivery.Event.ID)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatalf("failed to acknowledge RabbitMQ event: %v", err)
		}
	case <-deadline.Done():
		t.Fatalf("timed out waiting for RabbitMQ event %s", event.ID)
	}
}
