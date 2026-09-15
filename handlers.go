package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

func (app App) healthHandler(w http.ResponseWriter, r *http.Request) {
	response := HealthResponse{
		Status:        "ok",
		Time:          time.Now().Format(time.RFC3339),
		Date:          time.Now().Format("2006-01-02"),
		QueueLength:   len(app.eventQueue),
		QueueCapacity: cap(app.eventQueue),
	}

	writeJSON(w, http.StatusOK, response)
}

func (app App) metricsHandler(w http.ResponseWriter, r *http.Request) {
	response := app.metrics.Snapshot(len(app.eventQueue), cap(app.eventQueue))
	writeJSON(w, http.StatusOK, response)
}

func (app App) deadLettersHandler(w http.ResponseWriter, r *http.Request) {
	response := app.deadLetters.Snapshot()
	writeJSON(w, http.StatusOK, response)
}

func (app App) createEventHandler(w http.ResponseWriter, r *http.Request) {
	var event Event

	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		app.metrics.IncEventsRejected()
		slog.Warn("invalid event payload", "error", err)
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if event.ID == "" {
		app.metrics.IncEventsRejected()
		slog.Warn("event rejected", "reason", "missing id", "event_type", event.Type)
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}

	if event.Type == "" {
		app.metrics.IncEventsRejected()
		slog.Warn("event rejected", "reason", "missing type", "event_id", event.ID)
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}

	select {
	case app.eventQueue <- event:
		app.metrics.IncEventsQueued()
		slog.Info("event queued", "event_id", event.ID, "event_type", event.Type, "queue_length", len(app.eventQueue), "queue_capacity", cap(app.eventQueue))
	default:
		app.metrics.IncEventsRejected()
		slog.Warn("event queue is full", "event_id", event.ID, "event_type", event.Type, "queue_capacity", cap(app.eventQueue))
		writeError(w, http.StatusServiceUnavailable, "event queue is full")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"accepted": true,
		"eventId":  event.ID,
	})
}

func writeJSON(w http.ResponseWriter, statusCode int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, map[string]any{
		"error": message,
	})
}
