package main

import (
	"context"
	"fmt"
	"time"
)

type RetryableDeadLetterDetail struct {
	EventID       string
	ReplayAttempt int
}

func (store *EventStore) ClaimRetryableDeadLetterDetails(ctx context.Context, limit, maxAttempts int) ([]RetryableDeadLetterDetail, error) {
	rows, err := store.db.QueryContext(ctx, store.bind(`
		WITH candidates AS (
			SELECT event_id
			FROM dead_letters
			WHERE replayed_at IS NULL
			  AND next_retry_at IS NOT NULL
			  AND next_retry_at <= ?
			  AND replay_attempts < ?
			ORDER BY next_retry_at ASC, event_id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT ?
		)
		UPDATE dead_letters
		SET replay_attempts = replay_attempts + 1, next_retry_at = NULL
		FROM candidates
		WHERE dead_letters.event_id = candidates.event_id
		RETURNING dead_letters.event_id, dead_letters.replay_attempts
	`), time.Now().UTC(), maxAttempts, limit)
	if err != nil {
		return nil, fmt.Errorf("claim retryable dead letter details: %w", err)
	}
	defer rows.Close()

	items := make([]RetryableDeadLetterDetail, 0)
	for rows.Next() {
		var item RetryableDeadLetterDetail
		if err := rows.Scan(&item.EventID, &item.ReplayAttempt); err != nil {
			return nil, fmt.Errorf("scan retryable dead letter details: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate retryable dead letter details: %w", err)
	}
	return items, nil
}

func (store *EventStore) RescheduleDeadLetterRetryWithDelay(ctx context.Context, eventID string, retryError error, delay time.Duration) error {
	_, err := store.db.ExecContext(ctx, store.bind(`
		UPDATE dead_letters
		SET next_retry_at = ?, last_retry_error = ?
		WHERE event_id = ? AND replayed_at IS NULL
	`), time.Now().UTC().Add(delay), retryError.Error(), eventID)
	if err != nil {
		return fmt.Errorf("reschedule dead letter retry with delay: %w", err)
	}
	return nil
}
