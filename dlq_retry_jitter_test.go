package main

import (
	"testing"
	"time"
)

func TestDLQRetryDelayUsesProgressiveBackoffWithJitter(t *testing.T) {
	scheduler := &DLQRetryScheduler{interval: time.Minute}

	for attempt, expectedBase := range map[int]time.Duration{
		1: time.Minute,
		2: 2 * time.Minute,
		3: 4 * time.Minute,
	} {
		delay := scheduler.retryDelay(attempt)
		maxDelay := expectedBase + expectedBase/4
		if delay < expectedBase || delay > maxDelay {
			t.Fatalf("attempt %d: expected delay between %s and %s, got %s", attempt, expectedBase, maxDelay, delay)
		}
	}
}

func TestDLQRetryDelayCapsAt24Hours(t *testing.T) {
	scheduler := &DLQRetryScheduler{interval: 12 * time.Hour}

	delay := scheduler.retryDelay(3)
	if delay < 24*time.Hour || delay > 27*time.Hour {
		t.Fatalf("expected capped delay between 24h and 27h, got %s", delay)
	}
}
