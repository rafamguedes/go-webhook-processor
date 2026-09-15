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
}

func LoadConfig() (Config, error) {
	config := Config{
		Port:                     getEnv("PORT", "8080"),
		QueueSize:                getEnvAsInt("QUEUE_SIZE", 100),
		WorkerCount:              getEnvAsInt("WORKER_COUNT", 3),
		ReadHeaderTimeoutSeconds: getEnvAsInt("READ_HEADER_TIMEOUT_SECONDS", 5),
		ShutdownTimeoutSeconds:   getEnvAsInt("SHUTDOWN_TIMEOUT_SECONDS", 10),
		LogFormat:                getEnv("LOG_FORMAT", "json"),
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
