package main

import "testing"

func TestRabbitMQEventQueueStartsWhileBrokerIsUnavailable(t *testing.T) {
	config := testConfig()
	config.RabbitMQURL = "amqp://guest:guest@127.0.0.1:1/"
	config.RabbitMQQueue = "unavailable.test"
	config.RabbitMQConnectTimeoutMs = 100
	config.RabbitMQReconnectMs = 10

	queue, err := OpenRabbitMQEventQueue(config)
	if err != nil {
		t.Fatalf("expected queue initialization not to depend on broker availability: %v", err)
	}
	if err := queue.Close(); err != nil {
		t.Fatalf("failed to close queue: %v", err)
	}
}
