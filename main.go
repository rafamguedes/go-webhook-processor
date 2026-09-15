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

	close(app.eventQueue)
	workers.Wait()

	slog.Info("shutdown complete")
}
