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
	t.Setenv("LOG_FORMAT", "")
	t.Setenv("MAX_RETRIES", "")
	t.Setenv("RETRY_BACKOFF_SECONDS", "")
	t.Setenv("DEAD_LETTER_CAPACITY", "")
	t.Setenv("EVENT_DEDUP_CAPACITY", "")

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

	if config.EventDedupCapacity != 1000 {
		t.Fatalf("expected default event dedup capacity 1000, got %d", config.EventDedupCapacity)
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
	t.Setenv("EVENT_DEDUP_CAPACITY", "30")

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

	if config.EventDedupCapacity != 30 {
		t.Fatalf("expected event dedup capacity 30, got %d", config.EventDedupCapacity)
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

func TestLoadConfigRejectsInvalidEventDedupCapacity(t *testing.T) {
	t.Setenv("EVENT_DEDUP_CAPACITY", "0")

	_, err := LoadConfig()
	if err == nil {
		t.Fatal("expected config to reject invalid event dedup capacity")
	}
}

func TestHealthHandler(t *testing.T) {
	app := newTestApp(t)

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

func TestMetricsHandler(t *testing.T) {
	app := newTestApp(t)
	app.metrics.IncEventsQueued()
	app.metrics.IncEventsProcessed()
	app.metrics.IncEventRetries()
	app.metrics.IncEventsDuplicated()

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()

	app.metricsHandler(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}

	var body MetricsResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if body.EventsQueued != 1 {
		t.Fatalf("expected 1 queued event, got %d", body.EventsQueued)
	}

	if body.EventsProcessed != 1 {
		t.Fatalf("expected 1 processed event, got %d", body.EventsProcessed)
	}

	if body.EventRetries != 1 {
		t.Fatalf("expected 1 retry, got %d", body.EventRetries)
	}

	if body.EventsDuplicated != 1 {
		t.Fatalf("expected 1 duplicated event, got %d", body.EventsDuplicated)
	}

	if body.QueueCapacity != app.config.QueueSize {
		t.Fatalf("expected queue capacity %d, got %d", app.config.QueueSize, body.QueueCapacity)
	}
}

func TestDeadLettersHandler(t *testing.T) {
	app := newTestApp(t)
	app.deadLetters.Add(testEvent(), errForTest(), 4)

	request := httptest.NewRequest(http.MethodGet, "/dead-letters", nil)
	response := httptest.NewRecorder()

	app.deadLettersHandler(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}

	var body DeadLetterResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if body.Count != 1 {
		t.Fatalf("expected 1 dead letter, got %d", body.Count)
	}

	if body.Items[0].Event.ID != "evt-001" {
		t.Fatalf("expected dead letter event id evt-001, got %s", body.Items[0].Event.ID)
	}
}

func TestCreateEventHandlerQueuesValidEvent(t *testing.T) {
	app := newTestApp(t)

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

func TestCreateEventHandlerRejectsDuplicateEventID(t *testing.T) {
	app := newTestApp(t)
	body := `{"id":"evt-001","type":"payment.created","payload":{"amount":100}}`

	firstRequest := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body))
	firstResponse := httptest.NewRecorder()
	app.createEventHandler(firstResponse, firstRequest)

	secondRequest := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body))
	secondResponse := httptest.NewRecorder()
	app.createEventHandler(secondResponse, secondRequest)

	if secondResponse.Code != http.StatusConflict {
		t.Fatalf("expected status %d, got %d", http.StatusConflict, secondResponse.Code)
	}

	snapshot := app.metrics.Snapshot(len(app.eventQueue), cap(app.eventQueue))
	if snapshot.EventsDuplicated != 1 {
		t.Fatalf("expected 1 duplicated event, got %d", snapshot.EventsDuplicated)
	}
}

func TestCreateEventHandlerRejectsMissingID(t *testing.T) {
	app := newTestApp(t)

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

func newTestApp(t *testing.T) App {
	t.Helper()

	return NewApp(testConfig(), newTestEventStore(t))
}

func testConfig() Config {
	return Config{
		Port:                     "8080",
		QueueSize:                100,
		WorkerCount:              3,
		ReadHeaderTimeoutSeconds: 5,
		ShutdownTimeoutSeconds:   10,
		LogFormat:                "json",
		MaxRetries:               3,
		RetryBackoffSeconds:      1,
		DeadLetterCapacity:       100,
		EventDedupCapacity:       1000,
		DatabasePath:             "./events-test.db",
	}
}
