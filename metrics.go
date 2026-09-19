package main

import "sync/atomic"

type Metrics struct {
	eventsQueued              atomic.Int64
	eventsRejected            atomic.Int64
	eventsDuplicated          atomic.Int64
	eventsSkippedDuplicate    atomic.Int64
	eventsProcessed           atomic.Int64
	eventsFailedPermanent     atomic.Int64
	eventRetries              atomic.Int64
	deadLetterReplays         atomic.Int64
	deadLetterReplayRejected  atomic.Int64
	deadLetterAutoRetries     atomic.Int64
	deadLetterAutoRetryFailed atomic.Int64
}

type MetricsResponse struct {
	EventsQueued              int64 `json:"eventsQueued"`
	EventsRejected            int64 `json:"eventsRejected"`
	EventsDuplicated          int64 `json:"eventsDuplicated"`
	EventsSkippedDuplicate    int64 `json:"eventsSkippedDuplicate"`
	EventsProcessed           int64 `json:"eventsProcessed"`
	EventsFailedPermanent     int64 `json:"eventsFailedPermanent"`
	EventRetries              int64 `json:"eventRetries"`
	DeadLetterReplays         int64 `json:"deadLetterReplays"`
	DeadLetterReplayRejected  int64 `json:"deadLetterReplayRejected"`
	DeadLetterAutoRetries     int64 `json:"deadLetterAutoRetries"`
	DeadLetterAutoRetryFailed int64 `json:"deadLetterAutoRetryFailed"`
	QueueLength               int   `json:"queueLength"`
	QueueCapacity             int   `json:"queueCapacity"`
}

func NewMetrics() *Metrics                             { return &Metrics{} }
func (metrics *Metrics) IncEventsQueued()              { metrics.eventsQueued.Add(1) }
func (metrics *Metrics) IncEventsRejected()            { metrics.eventsRejected.Add(1) }
func (metrics *Metrics) IncEventsDuplicated()          { metrics.eventsDuplicated.Add(1) }
func (metrics *Metrics) IncEventsSkippedDuplicate()    { metrics.eventsSkippedDuplicate.Add(1) }
func (metrics *Metrics) IncEventsProcessed()           { metrics.eventsProcessed.Add(1) }
func (metrics *Metrics) IncEventsFailedPermanent()     { metrics.eventsFailedPermanent.Add(1) }
func (metrics *Metrics) IncEventRetries()              { metrics.eventRetries.Add(1) }
func (metrics *Metrics) IncDeadLetterReplays()         { metrics.deadLetterReplays.Add(1) }
func (metrics *Metrics) IncDeadLetterReplayRejected()  { metrics.deadLetterReplayRejected.Add(1) }
func (metrics *Metrics) IncDeadLetterAutoRetries()     { metrics.deadLetterAutoRetries.Add(1) }
func (metrics *Metrics) IncDeadLetterAutoRetryFailed() { metrics.deadLetterAutoRetryFailed.Add(1) }

func (metrics *Metrics) Snapshot(queueLength int, queueCapacity int) MetricsResponse {
	return MetricsResponse{
		EventsQueued:              metrics.eventsQueued.Load(),
		EventsRejected:            metrics.eventsRejected.Load(),
		EventsDuplicated:          metrics.eventsDuplicated.Load(),
		EventsSkippedDuplicate:    metrics.eventsSkippedDuplicate.Load(),
		EventsProcessed:           metrics.eventsProcessed.Load(),
		EventsFailedPermanent:     metrics.eventsFailedPermanent.Load(),
		EventRetries:              metrics.eventRetries.Load(),
		DeadLetterReplays:         metrics.deadLetterReplays.Load(),
		DeadLetterReplayRejected:  metrics.deadLetterReplayRejected.Load(),
		DeadLetterAutoRetries:     metrics.deadLetterAutoRetries.Load(),
		DeadLetterAutoRetryFailed: metrics.deadLetterAutoRetryFailed.Load(),
		QueueLength:               queueLength,
		QueueCapacity:             queueCapacity,
	}
}
