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

	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		if len(os.Args) != 2 {
			fmt.Fprintln(os.Stderr, "uso: go run . migrate")
			os.Exit(1)
		}
		if err := RunMigrations(context.Background(), config.DatabaseURL, "migrations"); err != nil {
			slog.Error("database migration failed", "error", err)
			os.Exit(1)
		}
		slog.Info("database migrations complete")
		return
	}

	eventStore, err := OpenEventStore(config.DatabaseURL)
	if err != nil {
		slog.Error("open event store failed", "error", err)
		os.Exit(1)
	}
	defer eventStore.Close()

	if handled, err := runCommand(context.Background(), os.Args[1:], eventStore); handled {
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	eventQueue, err := OpenRabbitMQEventQueue(config)
	if err != nil {
		slog.Error("configure RabbitMQ event queue failed", "error", err)
		os.Exit(1)
	}
	app := NewApp(config, eventStore, eventQueue)

	var workers sync.WaitGroup
	startWorkers(config.WorkerCount, app.eventQueue, &workers, config, app.metrics, app.deadLetters, app.eventStore)

	dispatcherContext, stopDispatcher := context.WithCancel(context.Background())
	var dispatchers sync.WaitGroup
	NewOutboxDispatcher(app.eventStore, app.eventQueue, config, app.metrics).Start(dispatcherContext, &dispatchers)
	if config.DLQAutoRetryEnabled {
		NewDLQRetryScheduler(app.eventStore, config, app.metrics).Start(dispatcherContext, &dispatchers)
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

	stopDispatcher()
	dispatchers.Wait()

	if err := app.eventQueue.StopConsuming(); err != nil {
		slog.Error("stop event consumption failed", "error", err)
	}
	workers.Wait()
	if err := app.eventQueue.Close(); err != nil {
		slog.Error("close event queue failed", "error", err)
	}

	slog.Info("shutdown complete")
}
