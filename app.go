package main

import "net/http"

type App struct {
	config       Config
	eventQueue   chan Event
	metrics      *Metrics
	deadLetters  *DeadLetterStore
	deduplicator *EventDeduplicator
	eventStore   *EventStore
}

func NewApp(config Config, eventStore *EventStore) App {
	return App{
		config:       config,
		eventQueue:   make(chan Event, config.QueueSize),
		metrics:      NewMetrics(),
		deadLetters:  NewDeadLetterStore(config.DeadLetterCapacity),
		deduplicator: NewEventDeduplicator(config.EventDedupCapacity),
		eventStore:   eventStore,
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
