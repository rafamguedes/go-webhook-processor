package main

import "net/http"

type App struct {
	config      Config
	eventQueue  EventQueue
	metrics     *Metrics
	deadLetters DeadLetterRepository
	eventStore  *EventStore
}

func NewApp(config Config, eventStore *EventStore, eventQueue EventQueue) App {
	return App{
		config:      config,
		eventQueue:  eventQueue,
		metrics:     NewMetrics(),
		deadLetters: NewPersistentDeadLetterStore(eventStore, config.DeadLetterCapacity),
		eventStore:  eventStore,
	}
}

func (app App) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", app.healthHandler)
	mux.HandleFunc("GET /ready", app.readinessHandler)
	mux.HandleFunc("GET /metrics", app.metricsHandler)
	mux.HandleFunc("GET /metrics/prometheus", app.prometheusMetricsHandler)
	mux.HandleFunc("GET /dead-letters", app.deadLettersHandler)
	mux.HandleFunc("POST /internal/dead-letters/{eventID}/replay", app.replayDeadLetterHandler)
	mux.HandleFunc("POST /events", app.createEventHandler)

	return requestIDMiddleware(mux)
}
