package main

import (
	"fmt"
	"strings"
)

func (metrics *Metrics) PrometheusWithOperationalMetrics(queueLength int, queueCapacity int, operational OperationalMetrics) string {
	output := metrics.Prometheus(queueLength, queueCapacity)
	var extra strings.Builder
	fmt.Fprintf(&extra, "# HELP webhook_outbox_pending_messages Current unpublished Outbox messages.\n# TYPE webhook_outbox_pending_messages gauge\nwebhook_outbox_pending_messages %d\n", operational.PendingOutbox)
	fmt.Fprintf(&extra, "# HELP webhook_dead_letters_pending Current unreplayed dead letters.\n# TYPE webhook_dead_letters_pending gauge\nwebhook_dead_letters_pending %d\n", operational.PendingDeadLetters)
	fmt.Fprintf(&extra, "# HELP webhook_events_rejection_ratio Rejected events divided by accepted and rejected events.\n# TYPE webhook_events_rejection_ratio gauge\nwebhook_events_rejection_ratio %s\n", ratio(metrics.eventsRejected.Load(), metrics.eventsQueued.Load()+metrics.eventsRejected.Load()))
	fmt.Fprintf(&extra, "# HELP webhook_events_failure_ratio Permanently failed events divided by processed and failed events.\n# TYPE webhook_events_failure_ratio gauge\nwebhook_events_failure_ratio %s\n", ratio(metrics.eventsFailedPermanent.Load(), metrics.eventsProcessed.Load()+metrics.eventsFailedPermanent.Load()))
	return output + extra.String()
}

func ratio(numerator int64, denominator int64) string {
	if denominator == 0 {
		return "0"
	}
	return fmt.Sprintf("%.6f", float64(numerator)/float64(denominator))
}
