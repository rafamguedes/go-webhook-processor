package main

import "net/http"

func (app App) prometheusMetricsHandler(w http.ResponseWriter, r *http.Request) {
	queueStats := app.eventQueue.Stats()
	operational, err := app.eventStore.OperationalMetrics(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "metrics unavailable")
		return
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(app.metrics.PrometheusWithOperationalMetrics(queueStats.Length, queueStats.Capacity, operational)))
}
