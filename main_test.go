package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLoadConfigUsesDefaults(t *testing.T) {
	t.Setenv("PORT", "")
	t.Setenv("QUEUE_SIZE", "")
	t.Setenv("WORKER_COUNT", "")
	t.Setenv("READ_HEADER_TIMEOUT_SECONDS", "")
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "")

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
}

func TestLoadConfigReadsEnvironmentVariables(t *testing.T) {
	t.Setenv("PORT", "9090")
	t.Setenv("QUEUE_SIZE", "10")
	t.Setenv("WORKER_COUNT", "2")
	t.Setenv("READ_HEADER_TIMEOUT_SECONDS", "7")
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "15")

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
}

func TestLoadConfigRejectsInvalidQueueSize(t *testing.T) {
	t.Setenv("QUEUE_SIZE", "0")

	_, err := LoadConfig()
	if err == nil {
		t.Fatal("expected config to reject invalid queue size")
	}
}
func TestHealthHandler(t *testing.T) {
	app := newTestApp()

	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	response := httptest.NewRecorder()

	app.healthHandler(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}

	var body HealthResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if body.Status != "ok" {
		t.Fatalf("expected status ok, got %s", body.Status)
	}

	if body.QueueCapacity != app.config.QueueSize {
		t.Fatalf("expected queue capacity %d, got %d", app.config.QueueSize, body.QueueCapacity)
	}
}

func TestCreateEventHandlerQueuesValidEvent(t *testing.T) {
	app := newTestApp()

	body := `{"id":"evt-001","type":"payment.created","payload":{"amount":100}}`
	request := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body))
	response := httptest.NewRecorder()

	app.createEventHandler(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d", http.StatusAccepted, response.Code)
	}

	if len(app.eventQueue) != 1 {
		t.Fatalf("expected queue length 1, got %d", len(app.eventQueue))
	}

	event := <-app.eventQueue
	if event.ID != "evt-001" {
		t.Fatalf("expected event id evt-001, got %s", event.ID)
	}

	if event.Type != "payment.created" {
		t.Fatalf("expected event type payment.created, got %s", event.Type)
	}
}

func TestCreateEventHandlerRejectsMissingID(t *testing.T) {
	app := newTestApp()

	body := `{"type":"payment.created","payload":{"amount":100}}`
	request := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body))
	response := httptest.NewRecorder()

	app.createEventHandler(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, response.Code)
	}

	if len(app.eventQueue) != 0 {
		t.Fatalf("expected queue length 0, got %d", len(app.eventQueue))
	}
}

func newTestApp() App {
	return NewApp(Config{
		Port:                     "8080",
		QueueSize:                100,
		WorkerCount:              3,
		ReadHeaderTimeoutSeconds: 5,
		ShutdownTimeoutSeconds:   10,
	})
}
