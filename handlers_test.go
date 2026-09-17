package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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

func TestCreateEventHandlerPersistsValidEventInOutbox(t *testing.T) {
	app := newTestApp(t)

	body := `{"id":"evt-001","type":"payment.created","payload":{"amount":100}}`
	request := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body))
	response := httptest.NewRecorder()

	app.createEventHandler(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d", http.StatusAccepted, response.Code)
	}

	if queueStats := app.eventQueue.Stats(); queueStats.Length != 0 {
		t.Fatalf("expected handler not to publish directly, got queue length %d", queueStats.Length)
	}

	pending, err := app.eventStore.ListPendingOutbox(t.Context(), 10)
	if err != nil {
		t.Fatalf("failed to list pending outbox: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending outbox message, got %d", len(pending))
	}
	if pending[0].Event.ID != "evt-001" {
		t.Fatalf("expected event id evt-001, got %s", pending[0].Event.ID)
	}

	dispatcher := NewOutboxDispatcher(app.eventStore, app.eventQueue, app.config, app.metrics)
	if err := dispatcher.DispatchPending(t.Context()); err != nil {
		t.Fatalf("failed to dispatch outbox: %v", err)
	}

	event := (<-app.eventQueue.Events()).Event
	if event.ID != "evt-001" {
		t.Fatalf("expected published event id evt-001, got %s", event.ID)
	}
}
func TestCreateEventHandlerRejectsDuplicateEventIDAfterAppRestart(t *testing.T) {
	app := newTestApp(t)
	body := `{"id":"evt-001","type":"payment.created","payload":{"amount":100}}`

	firstRequest := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body))
	firstResponse := httptest.NewRecorder()
	app.createEventHandler(firstResponse, firstRequest)

	config := testConfig()
	restartedApp := NewApp(config, app.eventStore, NewMemoryEventQueue(config.QueueSize))
	secondRequest := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body))
	secondResponse := httptest.NewRecorder()
	restartedApp.createEventHandler(secondResponse, secondRequest)

	if secondResponse.Code != http.StatusConflict {
		t.Fatalf("expected status %d, got %d", http.StatusConflict, secondResponse.Code)
	}

	queueStats := restartedApp.eventQueue.Stats()
	snapshot := restartedApp.metrics.Snapshot(queueStats.Length, queueStats.Capacity)
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

	queueStats := app.eventQueue.Stats()
	if queueStats.Length != 0 {
		t.Fatalf("expected queue length 0, got %d", queueStats.Length)
	}
}
