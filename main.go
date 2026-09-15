package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	config, err := LoadConfig()
	if err != nil {
		log.Fatal(err)
	}

	app := NewApp(config)

	var workers sync.WaitGroup
	startWorkers(config.WorkerCount, app.eventQueue, &workers)

	server := &http.Server{
		Addr:              config.ServerAddress(),
		Handler:           app.routes(),
		ReadHeaderTimeout: config.ReadHeaderTimeout(),
	}

	serverErrors := make(chan error, 1)
	go func() {
		log.Printf("server listening on http://localhost%s", config.ServerAddress())
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

	shutdownContext, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout())
	defer cancel()

	if err := server.Shutdown(shutdownContext); err != nil {
		log.Printf("server shutdown error: %v", err)
	}

	close(app.eventQueue)
	workers.Wait()

	log.Println("shutdown complete")
}
