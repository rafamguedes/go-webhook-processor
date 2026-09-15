package main

import (
	"log"
	"net/http"
	"time"
)

func main() {
	app := NewApp()

	startWorkers(workerCount, app.eventQueue)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           app.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Println("server listening on http://localhost:8080")

	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
