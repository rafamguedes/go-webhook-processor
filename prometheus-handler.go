package main

import "net/http"

func (app App) prometheusMetricsHandler(w http.ResponseWriter, r *http.Request) {
	queueStats := app.eventQueue.Stats()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(app.metrics.Prometheus(queueStats.Length, queueStats.Capacity)))
}
