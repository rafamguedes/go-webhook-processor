package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

func (app App) healthHandler(w http.ResponseWriter, r *http.Request) {
	queueStats := app.eventQueue.Stats()
	response := HealthResponse{
		Status:        "ok",
		Time:          time.Now().Format(time.RFC3339),
		Date:          time.Now().Format("2006-01-02"),
		QueueLength:   queueStats.Length,
		QueueCapacity: queueStats.Capacity,
	}

	writeJSON(w, http.StatusOK, response)
}

func (app App) metricsHandler(w http.ResponseWriter, r *http.Request) {
	queueStats := app.eventQueue.Stats()
	response := app.metrics.Snapshot(queueStats.Length, queueStats.Capacity)
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

	if err := app.eventStore.SaveQueuedWithOutbox(r.Context(), event); err != nil {
		app.metrics.IncEventsRejected()
		if errors.Is(err, ErrEventAlreadyExists) {
			app.metrics.IncEventsDuplicated()
			slog.Warn("event rejected", "reason", "duplicate event id", "event_id", event.ID, "event_type", event.Type)
			writeError(w, http.StatusConflict, "duplicate event id")
			return
		}

		slog.Error("failed to persist event and outbox message", "event_id", event.ID, "event_type", event.Type, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to persist event")
		return
	}

	slog.Info("event accepted into outbox", "event_id", event.ID, "event_type", event.Type)
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
