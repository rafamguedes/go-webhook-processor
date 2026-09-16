package main

import "testing"

func TestLoadConfigUsesDefaults(t *testing.T) {
	t.Setenv("PORT", "")
	t.Setenv("QUEUE_SIZE", "")
	t.Setenv("WORKER_COUNT", "")
	t.Setenv("READ_HEADER_TIMEOUT_SECONDS", "")
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "")
	t.Setenv("LOG_FORMAT", "")
	t.Setenv("MAX_RETRIES", "")
	t.Setenv("RETRY_BACKOFF_SECONDS", "")
	t.Setenv("DEAD_LETTER_CAPACITY", "")

	config, err := LoadConfig()
	if err != nil {
		t.Fatalf("expected config to load, got error: %v", err)
	}

	if config.Port != "8080" {
		t.Fatalf("expected default port 8080, got %s", config.Port)
	}

	if config.QueueSize != 100 {
		t.Fatalf("expected default queue size 100, got %d", config.QueueSize)
	}

	if config.WorkerCount != 3 {
		t.Fatalf("expected default worker count 3, got %d", config.WorkerCount)
	}

	if config.LogFormat != "json" {
		t.Fatalf("expected default log format json, got %s", config.LogFormat)
	}

	if config.MaxRetries != 3 {
		t.Fatalf("expected default max retries 3, got %d", config.MaxRetries)
	}

	if config.RetryBackoffSeconds != 1 {
		t.Fatalf("expected default retry backoff 1, got %d", config.RetryBackoffSeconds)
	}

	if config.DeadLetterCapacity != 100 {
		t.Fatalf("expected default dead letter capacity 100, got %d", config.DeadLetterCapacity)
	}
}

func TestLoadConfigReadsEnvironmentVariables(t *testing.T) {
	t.Setenv("PORT", "9090")
	t.Setenv("QUEUE_SIZE", "10")
	t.Setenv("WORKER_COUNT", "2")
	t.Setenv("READ_HEADER_TIMEOUT_SECONDS", "7")
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "15")
	t.Setenv("LOG_FORMAT", "text")
	t.Setenv("MAX_RETRIES", "5")
	t.Setenv("RETRY_BACKOFF_SECONDS", "2")
	t.Setenv("DEAD_LETTER_CAPACITY", "20")

	config, err := LoadConfig()
	if err != nil {
		t.Fatalf("expected config to load, got error: %v", err)
	}

	if config.ServerAddress() != ":9090" {
		t.Fatalf("expected server address :9090, got %s", config.ServerAddress())
	}

	if config.QueueSize != 10 {
		t.Fatalf("expected queue size 10, got %d", config.QueueSize)
	}

	if config.WorkerCount != 2 {
		t.Fatalf("expected worker count 2, got %d", config.WorkerCount)
	}

	if config.LogFormat != "text" {
		t.Fatalf("expected log format text, got %s", config.LogFormat)
	}

	if config.MaxRetries != 5 {
		t.Fatalf("expected max retries 5, got %d", config.MaxRetries)
	}

	if config.RetryBackoffSeconds != 2 {
		t.Fatalf("expected retry backoff 2, got %d", config.RetryBackoffSeconds)
	}

	if config.DeadLetterCapacity != 20 {
		t.Fatalf("expected dead letter capacity 20, got %d", config.DeadLetterCapacity)
	}
}

func TestLoadConfigRejectsInvalidQueueSize(t *testing.T) {
	t.Setenv("QUEUE_SIZE", "0")

	_, err := LoadConfig()
	if err == nil {
		t.Fatal("expected config to reject invalid queue size")
	}
}

func TestLoadConfigRejectsInvalidLogFormat(t *testing.T) {
	t.Setenv("LOG_FORMAT", "xml")

	_, err := LoadConfig()
	if err == nil {
		t.Fatal("expected config to reject invalid log format")
	}
}

func TestLoadConfigRejectsNegativeMaxRetries(t *testing.T) {
	t.Setenv("MAX_RETRIES", "-1")

	_, err := LoadConfig()
	if err == nil {
		t.Fatal("expected config to reject negative max retries")
	}
}

func TestLoadConfigRejectsInvalidDeadLetterCapacity(t *testing.T) {
	t.Setenv("DEAD_LETTER_CAPACITY", "0")

	_, err := LoadConfig()
	if err == nil {
		t.Fatal("expected config to reject invalid dead letter capacity")
	}
}
