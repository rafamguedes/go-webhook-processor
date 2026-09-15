package main

import "net/http"

const (
	queueSize   = 100
	workerCount = 3
)

type App struct {
	eventQueue chan Event
}

func NewApp() App {
	return App{
		eventQueue: make(chan Event, queueSize),
	}
}

func (app App) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", app.healthHandler)
	mux.HandleFunc("POST /events", app.createEventHandler)

	return mux
}
