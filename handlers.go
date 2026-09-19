package main

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
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

func (app App) readinessHandler(w http.ResponseWriter, r *http.Request) {
	if err := app.eventStore.db.PingContext(r.Context()); err != nil {
		slog.Warn("readiness check failed", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "not_ready",
			"error":  "database unavailable",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ready",
	})
}

func (app App) metricsHandler(w http.ResponseWriter, r *http.Request) {
	queueStats := app.eventQueue.Stats()
	response := app.metrics.Snapshot(queueStats.Length, queueStats.Capacity)
	writeJSON(w, http.StatusOK, response)
}

func (app App) deadLettersHandler(w http.ResponseWriter, r *http.Request) {
	response, err := app.deadLetters.Snapshot(r.Context())
	if err != nil {
		slog.Error("list dead letters failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list dead letters")
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (app App) replayDeadLetterHandler(w http.ResponseWriter, r *http.Request) {
	if app.config.DLQReplayToken == "" {
		app.metrics.IncDeadLetterReplayRejected()
		writeError(w, http.StatusServiceUnavailable, "dead letter replay is not configured")
		return
	}

	authorization := r.Header.Get("Authorization")
	const bearerPrefix = "Bearer "
	providedToken := ""
	if strings.HasPrefix(authorization, bearerPrefix) {
		providedToken = strings.TrimSpace(strings.TrimPrefix(authorization, bearerPrefix))
	}
	if subtle.ConstantTimeCompare([]byte(providedToken), []byte(app.config.DLQReplayToken)) != 1 {
		app.metrics.IncDeadLetterReplayRejected()
		slog.Warn("dead letter replay unauthorized", "event_id", r.PathValue("eventID"))
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	eventID := strings.TrimSpace(r.PathValue("eventID"))
	if eventID == "" {
		app.metrics.IncDeadLetterReplayRejected()
		writeError(w, http.StatusBadRequest, "event id is required")
		return
	}

	if err := app.eventStore.ReplayDeadLetter(r.Context(), eventID); err != nil {
		if errors.Is(err, ErrDeadLetterNotFound) {
			app.metrics.IncDeadLetterReplayRejected()
			writeError(w, http.StatusNotFound, "dead letter not found")
			return
		}
		if errors.Is(err, ErrDeadLetterNotFailed) {
			app.metrics.IncDeadLetterReplayRejected()
			writeError(w, http.StatusConflict, "event is not failed")
			return
		}

		app.metrics.IncDeadLetterReplayRejected()
		slog.Error("replay dead letter failed", "event_id", eventID, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to replay dead letter")
		return
	}

	app.metrics.IncDeadLetterReplays()
	slog.Info("dead letter replay accepted", "event_id", eventID)
	writeJSON(w, http.StatusAccepted, map[string]any{
		"accepted": true,
		"eventId":  eventID,
		"status":   EventStatusQueued,
	})
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

	event.RequestID = r.Header.Get("X-Request-ID")
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
