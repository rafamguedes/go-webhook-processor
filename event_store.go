package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var ErrEventAlreadyExists = errors.New("event already exists")
var ErrDeadLetterNotFound = errors.New("dead letter not found")
var ErrDeadLetterNotFailed = errors.New("event is not failed")

const (
	EventStatusQueued     = "queued"
	EventStatusProcessing = "processing"
	EventStatusProcessed  = "processed"
	EventStatusFailed     = "failed"
)

type StoredEvent struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Payload   any       `json:"payload"`
	Status    string    `json:"status"`
	Error     string    `json:"error,omitempty"`
	Attempts  int       `json:"attempts"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type EventClaimResult int

const (
	EventClaimed EventClaimResult = iota
	EventClaimInProgress
	EventClaimFinal
	EventClaimMissing
)

type EventStore struct {
	db *sql.DB
}

func OpenEventStore(databaseURL string) (*EventStore, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open event store: %w", err)
	}

	if err := db.PingContext(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping event store: %w", err)
	}

	return &EventStore{db: db}, nil
}

func (store *EventStore) bind(query string) string {
	var builder strings.Builder
	placeholder := 1
	for _, character := range query {
		if character == '?' {
			builder.WriteString(fmt.Sprintf("$%d", placeholder))
			placeholder++
			continue
		}
		builder.WriteRune(character)
	}
	return builder.String()
}

func (store *EventStore) Close() error {
	return store.db.Close()
}

func (store *EventStore) SaveQueuedWithOutbox(ctx context.Context, event Event) error {
	payload, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("marshal event payload: %w", err)
	}

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin event transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	result, err := tx.ExecContext(ctx, store.bind(`
		INSERT INTO events (id, type, payload, status, attempts, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO NOTHING
	`), event.ID, event.Type, string(payload), EventStatusQueued, 0, now, now)
	if err != nil {
		return fmt.Errorf("save queued event: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check saved event: %w", err)
	}
	if rowsAffected == 0 {
		return ErrEventAlreadyExists
	}

	_, err = tx.ExecContext(ctx, store.bind(`
		INSERT INTO outbox (event_id, event_type, payload, attempts, created_at)
		VALUES (?, ?, ?, 0, ?)
	`), event.ID, event.Type, string(payload), now)
	if err != nil {
		return fmt.Errorf("save outbox message: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit event transaction: %w", err)
	}
	return nil
}

type OutboxMessage struct {
	ID       int64
	Event    Event
	Attempts int
}

func (store *EventStore) ListPendingOutbox(ctx context.Context, limit int) ([]OutboxMessage, error) {
	rows, err := store.db.QueryContext(ctx, store.bind(`
		SELECT id, event_id, event_type, payload, attempts
		FROM outbox
		WHERE published_at IS NULL
		ORDER BY id ASC
		LIMIT ?
	`), limit)
	if err != nil {
		return nil, fmt.Errorf("list pending outbox messages: %w", err)
	}
	defer rows.Close()

	messages := make([]OutboxMessage, 0)
	for rows.Next() {
		var message OutboxMessage
		var payload string
		if err := rows.Scan(&message.ID, &message.Event.ID, &message.Event.Type, &payload, &message.Attempts); err != nil {
			return nil, fmt.Errorf("scan outbox message: %w", err)
		}
		if err := json.Unmarshal([]byte(payload), &message.Event.Payload); err != nil {
			return nil, fmt.Errorf("decode outbox message %d payload: %w", message.ID, err)
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox messages: %w", err)
	}
	return messages, nil
}

func (store *EventStore) MarkOutboxPublished(ctx context.Context, messageID int64) error {
	_, err := store.db.ExecContext(ctx, store.bind(`
		UPDATE outbox SET published_at = ?, last_error = ''
		WHERE id = ? AND published_at IS NULL
	`), time.Now().UTC(), messageID)
	if err != nil {
		return fmt.Errorf("mark outbox message published: %w", err)
	}
	return nil
}

func (store *EventStore) MarkOutboxFailed(ctx context.Context, messageID int64, publishError error) error {
	_, err := store.db.ExecContext(ctx, store.bind(`
		UPDATE outbox SET attempts = attempts + 1, last_error = ?
		WHERE id = ? AND published_at IS NULL
	`), publishError.Error(), messageID)
	if err != nil {
		return fmt.Errorf("mark outbox message failed: %w", err)
	}
	return nil
}
func (store *EventStore) ClaimForProcessing(ctx context.Context, eventID string, leaseDuration time.Duration) (EventClaimResult, error) {
	now := time.Now().UTC()
	leaseExpiredBefore := now.Add(-leaseDuration)
	result, err := store.db.ExecContext(ctx, store.bind(`
		UPDATE events
		SET status = ?, processing_started_at = ?, updated_at = ?
		WHERE id = ?
		  AND (
			status = ?
			OR (status = ? AND processing_started_at <= ?)
		  )
	`), EventStatusProcessing, now, now, eventID, EventStatusQueued, EventStatusProcessing, leaseExpiredBefore)
	if err != nil {
		return EventClaimMissing, fmt.Errorf("claim event for processing: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return EventClaimMissing, fmt.Errorf("check claimed event: %w", err)
	}
	if rowsAffected == 1 {
		return EventClaimed, nil
	}

	var status string
	if err := store.db.QueryRowContext(ctx, store.bind(`SELECT status FROM events WHERE id = ?`), eventID).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return EventClaimMissing, nil
		}
		return EventClaimMissing, fmt.Errorf("read event claim status: %w", err)
	}

	switch status {
	case EventStatusProcessed, EventStatusFailed:
		return EventClaimFinal, nil
	case EventStatusProcessing:
		return EventClaimInProgress, nil
	default:
		return EventClaimInProgress, nil
	}
}
func (store *EventStore) SaveDeadLetter(ctx context.Context, event Event, processingError error, attempts int) error {
	payload, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("marshal dead letter payload: %w", err)
	}
	_, err = store.db.ExecContext(ctx, store.bind(`
		INSERT INTO dead_letters (event_id, event_type, payload, error, attempts, failed_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(event_id) DO UPDATE SET
			error = excluded.error,
			attempts = excluded.attempts,
			failed_at = excluded.failed_at,
			replayed_at = NULL
	`), event.ID, event.Type, string(payload), processingError.Error(), attempts, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("save dead letter: %w", err)
	}
	return nil
}

func (store *EventStore) ReplayDeadLetter(ctx context.Context, eventID string) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin dead letter replay: %w", err)
	}
	defer tx.Rollback()

	var status, eventType, payload string
	if err := tx.QueryRowContext(ctx, store.bind(`
		SELECT e.status, e.type, e.payload
		FROM events e
		JOIN dead_letters d ON d.event_id = e.id
		WHERE e.id = ?
	`), eventID).Scan(&status, &eventType, &payload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrDeadLetterNotFound
		}
		return fmt.Errorf("read event for dead letter replay: %w", err)
	}
	if status != EventStatusFailed {
		return fmt.Errorf("%w: current status is %s", ErrDeadLetterNotFailed, status)
	}

	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, store.bind(`
		UPDATE events
		SET status = ?, attempts = 0, error = '', processing_started_at = NULL, updated_at = ?
		WHERE id = ? AND status = ?
	`), EventStatusQueued, now, eventID, EventStatusFailed); err != nil {
		return fmt.Errorf("reset event for dead letter replay: %w", err)
	}

	result, err := tx.ExecContext(ctx, store.bind(`
		UPDATE outbox
		SET attempts = 0, last_error = '', published_at = NULL
		WHERE event_id = ?
	`), eventID)
	if err != nil {
		return fmt.Errorf("reopen outbox message: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check reopened outbox message: %w", err)
	}
	if rowsAffected == 0 {
		if _, err := tx.ExecContext(ctx, store.bind(`
			INSERT INTO outbox (event_id, event_type, payload, attempts, created_at)
			VALUES (?, ?, ?, 0, ?)
		`), eventID, eventType, payload, now); err != nil {
			return fmt.Errorf("create outbox message for replay: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, store.bind(`UPDATE dead_letters SET replayed_at = ? WHERE event_id = ?`), now, eventID); err != nil {
		return fmt.Errorf("mark dead letter replayed: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit dead letter replay: %w", err)
	}
	return nil
}
func (store *EventStore) ListDeadLetters(ctx context.Context, limit int) ([]DeadLetter, error) {
	rows, err := store.db.QueryContext(ctx, store.bind(`
		SELECT event_id, event_type, payload, error, attempts, failed_at, replayed_at
		FROM dead_letters
		ORDER BY failed_at DESC
		LIMIT ?
	`), limit)
	if err != nil {
		return nil, fmt.Errorf("list dead letters: %w", err)
	}
	defer rows.Close()

	items := make([]DeadLetter, 0)
	for rows.Next() {
		var item DeadLetter
		var payload string
		var replayedAt sql.NullTime
		if err := rows.Scan(&item.Event.ID, &item.Event.Type, &payload, &item.Error, &item.Attempts, &item.FailedAt, &replayedAt); err != nil {
			return nil, fmt.Errorf("scan dead letter: %w", err)
		}
		if replayedAt.Valid {
			item.ReplayedAt = &replayedAt.Time
		}
		if err := json.Unmarshal([]byte(payload), &item.Event.Payload); err != nil {
			return nil, fmt.Errorf("decode dead letter %s payload: %w", item.Event.ID, err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dead letters: %w", err)
	}
	return items, nil
}
func (store *EventStore) MarkProcessed(ctx context.Context, eventID string, attempts int) error {
	now := time.Now().UTC()
	_, err := store.db.ExecContext(ctx, store.bind(`
		UPDATE events
		SET status = ?, attempts = ?, error = '', processing_started_at = NULL, updated_at = ?
		WHERE id = ?
	`), EventStatusProcessed, attempts, now, eventID)
	if err != nil {
		return fmt.Errorf("mark event processed: %w", err)
	}

	return nil
}

func (store *EventStore) MarkFailed(ctx context.Context, eventID string, attempts int, processingError error) error {
	now := time.Now().UTC()
	_, err := store.db.ExecContext(ctx, store.bind(`
		UPDATE events
		SET status = ?, attempts = ?, error = ?, processing_started_at = NULL, updated_at = ?
		WHERE id = ?
	`), EventStatusFailed, attempts, processingError.Error(), now, eventID)
	if err != nil {
		return fmt.Errorf("mark event failed: %w", err)
	}

	return nil
}

func (store *EventStore) List(ctx context.Context, limit int) ([]StoredEvent, error) {
	rows, err := store.db.QueryContext(ctx, store.bind(`
		SELECT id, type, payload, status, error, attempts, created_at, updated_at
		FROM events
		ORDER BY created_at DESC
		LIMIT ?
	`), limit)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()

	events := make([]StoredEvent, 0)
	for rows.Next() {
		var event StoredEvent
		var payload string
		if err := rows.Scan(&event.ID, &event.Type, &payload, &event.Status, &event.Error, &event.Attempts, &event.CreatedAt, &event.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}

		var decodedPayload any
		if err := json.Unmarshal([]byte(payload), &decodedPayload); err != nil {
			decodedPayload = payload
		}
		event.Payload = decodedPayload

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}

	return events, nil
}
