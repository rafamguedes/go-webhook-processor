package main

import "sync/atomic"

type Metrics struct {
	eventsQueued          atomic.Int64
	eventsRejected        atomic.Int64
	eventsProcessed       atomic.Int64
	eventsFailedPermanent atomic.Int64
	eventRetries          atomic.Int64
}

type MetricsResponse struct {
	EventsQueued          int64 `json:"eventsQueued"`
	EventsRejected        int64 `json:"eventsRejected"`
	EventsProcessed       int64 `json:"eventsProcessed"`
	EventsFailedPermanent int64 `json:"eventsFailedPermanent"`
	EventRetries          int64 `json:"eventRetries"`
	QueueLength           int   `json:"queueLength"`
	QueueCapacity         int   `json:"queueCapacity"`
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

func (metrics *Metrics) IncEventsQueued() {
	metrics.eventsQueued.Add(1)
}

func (metrics *Metrics) IncEventsRejected() {
	metrics.eventsRejected.Add(1)
}

func (metrics *Metrics) IncEventsProcessed() {
	metrics.eventsProcessed.Add(1)
}

func (metrics *Metrics) IncEventsFailedPermanent() {
	metrics.eventsFailedPermanent.Add(1)
}

func (metrics *Metrics) IncEventRetries() {
	metrics.eventRetries.Add(1)
}

func (metrics *Metrics) Snapshot(queueLength int, queueCapacity int) MetricsResponse {
	return MetricsResponse{
		EventsQueued:          metrics.eventsQueued.Load(),
		EventsRejected:        metrics.eventsRejected.Load(),
		EventsProcessed:       metrics.eventsProcessed.Load(),
		EventsFailedPermanent: metrics.eventsFailedPermanent.Load(),
		EventRetries:          metrics.eventRetries.Load(),
		QueueLength:           queueLength,
		QueueCapacity:         queueCapacity,
	}
}
