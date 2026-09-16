package main

import "net/http"

type App struct {
	config      Config
	eventQueue  EventQueue
	metrics     *Metrics
	deadLetters *DeadLetterStore
	eventStore  *EventStore
}

func NewApp(config Config, eventStore *EventStore, eventQueue EventQueue) App {
	return App{
		config:      config,
		eventQueue:  eventQueue,
		metrics:     NewMetrics(),
		deadLetters: NewDeadLetterStore(config.DeadLetterCapacity),
		eventStore:  eventStore,
	}
}

func (app App) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", app.healthHandler)
	mux.HandleFunc("GET /metrics", app.metricsHandler)
	mux.HandleFunc("GET /dead-letters", app.deadLettersHandler)
	mux.HandleFunc("POST /events", app.createEventHandler)

	return mux
}
