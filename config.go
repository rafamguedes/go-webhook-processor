package main

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port                        string
	QueueSize                   int
	WorkerCount                 int
	ReadHeaderTimeoutSeconds    int
	ShutdownTimeoutSeconds      int
	LogFormat                   string
	MaxRetries                  int
	RetryBackoffSeconds         int
	DeadLetterCapacity          int
	DatabaseURL                 string
	DLQReplayToken              string
	DLQAutoRetryEnabled         bool
	DLQAutoRetryIntervalSeconds int
	DLQAutoRetryMaxAttempts     int
	RabbitMQURL                 string
	RabbitMQQueue               string
	RabbitMQReconnectMs         int
	RabbitMQConnectTimeoutMs    int
	OutboxPollIntervalMs        int
	OutboxBatchSize             int
	OutboxDispatchLeaseSeconds  int
	ProcessingLeaseSeconds      int
	ProcessingRequeueDelayMs    int
}

func LoadConfig() (Config, error) {
	config := Config{
		Port:                        getEnv("PORT", "8080"),
		QueueSize:                   getEnvAsInt("QUEUE_SIZE", 100),
		WorkerCount:                 getEnvAsInt("WORKER_COUNT", 3),
		ReadHeaderTimeoutSeconds:    getEnvAsInt("READ_HEADER_TIMEOUT_SECONDS", 5),
		ShutdownTimeoutSeconds:      getEnvAsInt("SHUTDOWN_TIMEOUT_SECONDS", 10),
		LogFormat:                   getEnv("LOG_FORMAT", "json"),
		MaxRetries:                  getEnvAsInt("MAX_RETRIES", 3),
		RetryBackoffSeconds:         getEnvAsInt("RETRY_BACKOFF_SECONDS", 1),
		DeadLetterCapacity:          getEnvAsInt("DEAD_LETTER_CAPACITY", 100),
		DatabaseURL:                 getEnv("DATABASE_URL", "postgres://webhook:webhook_dev@localhost:5432/webhook?sslmode=disable"),
		DLQReplayToken:              getEnv("DLQ_REPLAY_TOKEN", ""),
		DLQAutoRetryEnabled:         getEnvAsBool("DLQ_AUTO_RETRY_ENABLED", false),
		DLQAutoRetryIntervalSeconds: getEnvAsInt("DLQ_AUTO_RETRY_INTERVAL_SECONDS", 60),
		DLQAutoRetryMaxAttempts:     getEnvAsInt("DLQ_AUTO_RETRY_MAX_ATTEMPTS", 3),
		RabbitMQURL:                 getEnv("RABBITMQ_URL", "amqp://webhook:webhook_dev@localhost:5672/"),
		RabbitMQQueue:               getEnv("RABBITMQ_QUEUE", "webhook.events"),
		RabbitMQReconnectMs:         getEnvAsInt("RABBITMQ_RECONNECT_MS", 1000),
		RabbitMQConnectTimeoutMs:    getEnvAsInt("RABBITMQ_CONNECT_TIMEOUT_MS", 5000),
		OutboxPollIntervalMs:        getEnvAsInt("OUTBOX_POLL_INTERVAL_MS", 500),
		OutboxBatchSize:             getEnvAsInt("OUTBOX_BATCH_SIZE", 100),
		OutboxDispatchLeaseSeconds:  getEnvAsInt("OUTBOX_DISPATCH_LEASE_SECONDS", 30),
		ProcessingLeaseSeconds:      getEnvAsInt("PROCESSING_LEASE_SECONDS", 300),
		ProcessingRequeueDelayMs:    getEnvAsInt("PROCESSING_REQUEUE_DELAY_MS", 1000),
	}

	if config.QueueSize <= 0 {
		return Config{}, fmt.Errorf("QUEUE_SIZE must be greater than zero")
	}
	if config.WorkerCount <= 0 {
		return Config{}, fmt.Errorf("WORKER_COUNT must be greater than zero")
	}
	if config.ReadHeaderTimeoutSeconds <= 0 {
		return Config{}, fmt.Errorf("READ_HEADER_TIMEOUT_SECONDS must be greater than zero")
	}
	if config.ShutdownTimeoutSeconds <= 0 {
		return Config{}, fmt.Errorf("SHUTDOWN_TIMEOUT_SECONDS must be greater than zero")
	}
	if config.LogFormat != "json" && config.LogFormat != "text" {
		return Config{}, fmt.Errorf("LOG_FORMAT must be json or text")
	}
	if config.MaxRetries < 0 {
		return Config{}, fmt.Errorf("MAX_RETRIES must be zero or greater")
	}
	if config.RetryBackoffSeconds <= 0 {
		return Config{}, fmt.Errorf("RETRY_BACKOFF_SECONDS must be greater than zero")
	}
	if config.DeadLetterCapacity <= 0 {
		return Config{}, fmt.Errorf("DEAD_LETTER_CAPACITY must be greater than zero")
	}
	if config.DLQAutoRetryIntervalSeconds <= 0 {
		return Config{}, fmt.Errorf("DLQ_AUTO_RETRY_INTERVAL_SECONDS must be greater than zero")
	}
	if config.DLQAutoRetryMaxAttempts < 0 {
		return Config{}, fmt.Errorf("DLQ_AUTO_RETRY_MAX_ATTEMPTS must be zero or greater")
	}
	databaseURL, err := url.Parse(config.DatabaseURL)
	if err != nil || databaseURL.Host == "" ||
		(databaseURL.Scheme != "postgres" && databaseURL.Scheme != "postgresql") {
		return Config{}, fmt.Errorf("DATABASE_URL must be a valid postgres or postgresql URL")
	}
	if config.RabbitMQReconnectMs <= 0 {
		return Config{}, fmt.Errorf("RABBITMQ_RECONNECT_MS must be greater than zero")
	}
	if config.RabbitMQConnectTimeoutMs <= 0 {
		return Config{}, fmt.Errorf("RABBITMQ_CONNECT_TIMEOUT_MS must be greater than zero")
	}
	if config.OutboxPollIntervalMs <= 0 {
		return Config{}, fmt.Errorf("OUTBOX_POLL_INTERVAL_MS must be greater than zero")
	}
	if config.OutboxBatchSize <= 0 {
		return Config{}, fmt.Errorf("OUTBOX_BATCH_SIZE must be greater than zero")
	}
	if config.OutboxDispatchLeaseSeconds <= 0 {
		return Config{}, fmt.Errorf("OUTBOX_DISPATCH_LEASE_SECONDS must be greater than zero")
	}
	if config.ProcessingLeaseSeconds <= 0 {
		return Config{}, fmt.Errorf("PROCESSING_LEASE_SECONDS must be greater than zero")
	}
	if config.ProcessingRequeueDelayMs <= 0 {
		return Config{}, fmt.Errorf("PROCESSING_REQUEUE_DELAY_MS must be greater than zero")
	}
	if strings.TrimSpace(config.RabbitMQQueue) == "" {
		return Config{}, fmt.Errorf("RABBITMQ_QUEUE is required")
	}

	rabbitMQURL, err := url.Parse(config.RabbitMQURL)
	if err != nil || rabbitMQURL.Host == "" || (rabbitMQURL.Scheme != "amqp" && rabbitMQURL.Scheme != "amqps") {
		return Config{}, fmt.Errorf("RABBITMQ_URL must be a valid amqp or amqps URL")
	}
	return config, nil
}

func (config Config) ServerAddress() string {
	return ":" + config.Port
}

func (config Config) ReadHeaderTimeout() time.Duration {
	return time.Duration(config.ReadHeaderTimeoutSeconds) * time.Second
}

func (config Config) ShutdownTimeout() time.Duration {
	return time.Duration(config.ShutdownTimeoutSeconds) * time.Second
}

func (config Config) RabbitMQReconnectInterval() time.Duration {
	return time.Duration(config.RabbitMQReconnectMs) * time.Millisecond
}

func (config Config) RabbitMQConnectTimeout() time.Duration {
	return time.Duration(config.RabbitMQConnectTimeoutMs) * time.Millisecond
}

func (config Config) OutboxPollInterval() time.Duration {
	return time.Duration(config.OutboxPollIntervalMs) * time.Millisecond
}

func (config Config) OutboxDispatchLease() time.Duration {
	return time.Duration(config.OutboxDispatchLeaseSeconds) * time.Second
}

func (config Config) ProcessingLease() time.Duration {
	return time.Duration(config.ProcessingLeaseSeconds) * time.Second
}

func (config Config) ProcessingRequeueDelay() time.Duration {
	return time.Duration(config.ProcessingRequeueDelayMs) * time.Millisecond
}
func (config Config) RetryBackoff(attempt int) time.Duration {
	return time.Duration(config.RetryBackoffSeconds*attempt) * time.Second
}

func getEnv(key string, fallback string) string {
	value, exists := os.LookupEnv(key)
	if !exists || value == "" {
		return fallback
	}
	return value
}

func getEnvAsInt(key string, fallback int) int {
	value, exists := os.LookupEnv(key)
	if !exists || value == "" {
		return fallback
	}
	intValue, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return intValue
}

func getEnvAsBool(key string, fallback bool) bool {
	value, exists := os.LookupEnv(key)
	if !exists || value == "" {
		return fallback
	}
	boolValue, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return boolValue
}
