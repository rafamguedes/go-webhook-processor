package main

import (
	"log/slog"
	"os"
	"strings"
)

func setupLogger(config Config) {
	options := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}

	var handler slog.Handler
	if strings.EqualFold(config.LogFormat, "text") {
		handler = slog.NewTextHandler(os.Stdout, options)
	} else {
		handler = slog.NewJSONHandler(os.Stdout, options)
	}

	slog.SetDefault(slog.New(handler))
}
