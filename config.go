package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port                     string
	QueueSize                int
	WorkerCount              int
	ReadHeaderTimeoutSeconds int
	ShutdownTimeoutSeconds   int
	LogFormat                string
	MaxRetries               int
	RetryBackoffSeconds      int
	DeadLetterCapacity       int
	EventDedupCapacity       int
	DatabasePath             string
}

func LoadConfig() (Config, error) {
	config := Config{
		Port:                     getEnv("PORT", "8080"),
		QueueSize:                getEnvAsInt("QUEUE_SIZE", 100),
		WorkerCount:              getEnvAsInt("WORKER_COUNT", 3),
		ReadHeaderTimeoutSeconds: getEnvAsInt("READ_HEADER_TIMEOUT_SECONDS", 5),
		ShutdownTimeoutSeconds:   getEnvAsInt("SHUTDOWN_TIMEOUT_SECONDS", 10),
		LogFormat:                getEnv("LOG_FORMAT", "json"),
		MaxRetries:               getEnvAsInt("MAX_RETRIES", 3),
		RetryBackoffSeconds:      getEnvAsInt("RETRY_BACKOFF_SECONDS", 1),
		DeadLetterCapacity:       getEnvAsInt("DEAD_LETTER_CAPACITY", 100),
		EventDedupCapacity:       getEnvAsInt("EVENT_DEDUP_CAPACITY", 1000),
		DatabasePath:             getEnv("DATABASE_PATH", "./events.db"),
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

	if config.EventDedupCapacity <= 0 {
		return Config{}, fmt.Errorf("EVENT_DEDUP_CAPACITY must be greater than zero")
	}

	if config.DatabasePath == "" {
		return Config{}, fmt.Errorf("DATABASE_PATH is required")
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
