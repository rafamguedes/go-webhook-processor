package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthHandler(t *testing.T) {
	app := App{
		eventQueue: make(chan Event, queueSize),
	}

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

	if body.QueueCapacity != queueSize {
		t.Fatalf("expected queue capacity %d, got %d", queueSize, body.QueueCapacity)
	}
}

func TestCreateEventHandlerQueuesValidEvent(t *testing.T) {
	app := App{
		eventQueue: make(chan Event, queueSize),
	}

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
	app := App{
		eventQueue: make(chan Event, queueSize),
	}

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
