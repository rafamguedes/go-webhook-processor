package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRequestIDIsGeneratedAndPersistedWithEvent(t *testing.T) {
	app := newTestApp(t)
	request := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(`{"id":"evt-request-id","type":"payment.created","payload":{}}`))
	response := httptest.NewRecorder()

	app.routes().ServeHTTP(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d", http.StatusAccepted, response.Code)
	}
	requestID := response.Header().Get("X-Request-ID")
	if requestID == "" {
		t.Fatal("expected generated X-Request-ID response header")
	}

	pending, err := app.eventStore.ListPendingOutbox(t.Context(), 10)
	if err != nil {
		t.Fatalf("failed to list pending outbox: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected one pending message, got %d", len(pending))
	}
	if pending[0].Event.RequestID != requestID {
		t.Fatalf("expected persisted request ID %q, got %q", requestID, pending[0].Event.RequestID)
	}
}

func TestRequestIDFromClientIsPreserved(t *testing.T) {
	app := newTestApp(t)
	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	request.Header.Set("X-Request-ID", "client-request-001")
	response := httptest.NewRecorder()

	app.routes().ServeHTTP(response, request)

	if response.Header().Get("X-Request-ID") != "client-request-001" {
		t.Fatalf("expected client request ID to be preserved, got %q", response.Header().Get("X-Request-ID"))
	}
}
