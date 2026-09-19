package main

import (
	"sync/atomic"
	"time"
)

type ProcessingLatency struct {
	count atomic.Int64
	sumNS atomic.Int64
	maxNS atomic.Int64
}

func (metrics *Metrics) ObserveEventProcessingDuration(duration time.Duration) {
	nanoseconds := duration.Nanoseconds()
	metrics.processingLatency.count.Add(1)
	metrics.processingLatency.sumNS.Add(nanoseconds)
	for current := metrics.processingLatency.maxNS.Load(); nanoseconds > current; {
		if metrics.processingLatency.maxNS.CompareAndSwap(current, nanoseconds) {
			break
		}
		current = metrics.processingLatency.maxNS.Load()
	}
}
