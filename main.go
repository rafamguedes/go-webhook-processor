package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	app := NewApp()

	var workers sync.WaitGroup
	startWorkers(workerCount, app.eventQueue, &workers)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           app.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErrors := make(chan error, 1)
	go func() {
		log.Println("server listening on http://localhost:8080")
		serverErrors <- server.ListenAndServe()
	}()

	shutdownSignal, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	case <-shutdownSignal.Done():
		log.Println("shutdown signal received")
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownContext); err != nil {
		log.Printf("server shutdown error: %v", err)
	}

	close(app.eventQueue)
	workers.Wait()

	log.Println("shutdown complete")
}
