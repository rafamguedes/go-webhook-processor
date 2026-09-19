package main

import (
	"context"
	"fmt"
)

type OperationalMetrics struct {
	PendingOutbox      int64
	PendingDeadLetters int64
}

func (store *EventStore) OperationalMetrics(ctx context.Context) (OperationalMetrics, error) {
	var metrics OperationalMetrics
	err := store.db.QueryRowContext(ctx, `
		SELECT
			(SELECT COUNT(*) FROM outbox WHERE published_at IS NULL),
			(SELECT COUNT(*) FROM dead_letters WHERE replayed_at IS NULL)
	`).Scan(&metrics.PendingOutbox, &metrics.PendingDeadLetters)
	if err != nil {
		return OperationalMetrics{}, fmt.Errorf("read operational metrics: %w", err)
	}
	return metrics, nil
}
