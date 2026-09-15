package main

import "net/http"

type App struct {
	config     Config
	eventQueue chan Event
}

func NewApp(config Config) App {
	return App{
		config:     config,
		eventQueue: make(chan Event, config.QueueSize),
	}
}

func (app App) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", app.healthHandler)
	mux.HandleFunc("POST /events", app.createEventHandler)

	return mux
}
