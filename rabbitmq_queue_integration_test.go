package main

import (
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
	config.QueueProvider = QueueProviderRabbitMQ
	config.RabbitMQURL = rabbitMQURL
	config.RabbitMQQueue = fmt.Sprintf("webhook.events.integration.%d", time.Now().UnixNano())
	config.QueueSize = 1
	config.WorkerCount = 1

	queue, err := OpenRabbitMQEventQueue(config)
	if err != nil {
		t.Fatalf("failed to open RabbitMQ queue: %v", err)
	}
	t.Cleanup(func() {
		queue.StopConsuming()
		queue.consumerChannel.QueueDelete(config.RabbitMQQueue, false, false, false)
		queue.Close()
	})

	event := testEvent()
	if err := queue.Publish(t.Context(), event); err != nil {
		t.Fatalf("failed to publish RabbitMQ event: %v", err)
	}

	select {
	case delivery := <-queue.Events():
		if delivery.Event.ID != event.ID {
			t.Fatalf("expected event %s, got %s", event.ID, delivery.Event.ID)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatalf("failed to acknowledge RabbitMQ event: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for RabbitMQ event")
	}
}
