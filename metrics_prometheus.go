package main

import (
	"fmt"
	"strings"
)

func (metrics *Metrics) Prometheus(queueLength int, queueCapacity int) string {
	snapshot := metrics.Snapshot(queueLength, queueCapacity)
	var output strings.Builder

	writeCounter := func(name, help string, value int64) {
		fmt.Fprintf(&output, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, value)
	}
	writeGauge := func(name, help string, value int64) {
		fmt.Fprintf(&output, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", name, help, name, name, value)
	}

	writeCounter("webhook_events_queued_total", "Events accepted into the outbox.", snapshot.EventsQueued)
	writeCounter("webhook_events_rejected_total", "Events rejected by the API.", snapshot.EventsRejected)
	writeCounter("webhook_events_duplicated_total", "Duplicate events rejected by the API.", snapshot.EventsDuplicated)
	writeCounter("webhook_events_processed_total", "Events processed successfully.", snapshot.EventsProcessed)
	writeCounter("webhook_events_failed_permanent_total", "Events that reached permanent failure.", snapshot.EventsFailedPermanent)
	writeCounter("webhook_event_retries_total", "Event processing retries.", snapshot.EventRetries)
	writeCounter("webhook_dead_letter_replays_total", "Dead letters replayed manually.", snapshot.DeadLetterReplays)
	writeCounter("webhook_dead_letter_replay_rejected_total", "Rejected dead letter replay requests.", snapshot.DeadLetterReplayRejected)
	writeCounter("webhook_dead_letter_auto_retries_total", "Dead letters replayed automatically.", snapshot.DeadLetterAutoRetries)
	writeCounter("webhook_dead_letter_auto_retry_failed_total", "Failed automatic dead letter replays.", snapshot.DeadLetterAutoRetryFailed)
	writeGauge("webhook_queue_length", "Current in-process queue length.", int64(snapshot.QueueLength))
	writeGauge("webhook_queue_capacity", "Configured in-process queue capacity.", int64(snapshot.QueueCapacity))
	writeCounter("webhook_event_processing_duration_nanoseconds_total", "Total event processing duration in nanoseconds.", metrics.processingLatency.sumNS.Load())
	writeCounter("webhook_event_processing_attempts_total", "Total event processing attempts measured for latency.", metrics.processingLatency.count.Load())
	writeGauge("webhook_event_processing_duration_nanoseconds_max", "Maximum observed event processing duration in nanoseconds.", metrics.processingLatency.maxNS.Load())

	return output.String()
}
