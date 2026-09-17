package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	config, err := LoadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	setupLogger(config)

	eventStore, err := OpenEventStore(config.DatabasePath)
	if err != nil {
		slog.Error("open event store failed", "error", err)
		os.Exit(1)
	}
	defer eventStore.Close()

	eventQueue, err := NewConfiguredEventQueue(config)
	if err != nil {
		slog.Error("configure event queue failed", "provider", config.QueueProvider, "error", err)
		os.Exit(1)
	}
	app := NewApp(config, eventStore, eventQueue)

	var workers sync.WaitGroup
	startWorkers(config.WorkerCount, app.eventQueue, &workers, config, app.metrics, app.deadLetters, app.eventStore)

	if config.QueueProvider == QueueProviderMemory {
		recoveredEvents, err := recoverQueuedEvents(context.Background(), app.eventQueue, app.metrics, app.eventStore)
		if err != nil {
			slog.Error("recover queued events failed", "error", err)
			if stopErr := app.eventQueue.StopConsuming(); stopErr != nil {
				slog.Error("stop event consumption failed", "error", stopErr)
			}
			workers.Wait()
			if closeErr := app.eventQueue.Close(); closeErr != nil {
				slog.Error("close event queue failed", "error", closeErr)
			}
			os.Exit(1)
		}
		slog.Info("queued events recovered", "count", recoveredEvents)
	} else {
		slog.Info("database queue recovery skipped", "provider", config.QueueProvider)
	}

	server := &http.Server{
		Addr:              config.ServerAddress(),
		Handler:           app.routes(),
		ReadHeaderTimeout: config.ReadHeaderTimeout(),
	}

	serverErrors := make(chan error, 1)
	go func() {
		slog.Info("server listening", "url", "http://localhost"+config.ServerAddress())
		serverErrors <- server.ListenAndServe()
	}()

	shutdownSignal, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server stopped unexpectedly", "error", err)
			os.Exit(1)
		}
	case <-shutdownSignal.Done():
		slog.Info("shutdown signal received")
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout())
	defer cancel()

	if err := server.Shutdown(shutdownContext); err != nil {
		slog.Error("server shutdown error", "error", err)
	}

	if err := app.eventQueue.StopConsuming(); err != nil {
		slog.Error("stop event consumption failed", "error", err)
	}
	workers.Wait()
	if err := app.eventQueue.Close(); err != nil {
		slog.Error("close event queue failed", "error", err)
	}

	slog.Info("shutdown complete")
}
