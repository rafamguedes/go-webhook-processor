package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
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

const sqliteBusyTimeoutMs = 5000

func OpenEventStore(databasePath string) (*EventStore, error) {
	db, err := sql.Open("sqlite", sqliteDSN(databasePath))
	if err != nil {
		return nil, fmt.Errorf("open event store: %w", err)
	}

	store := &EventStore{db: db}
	if err := store.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}

	return store, nil
}

func sqliteDSN(databasePath string) string {
	separator := "?"
	if strings.Contains(databasePath, "?") {
		separator = "&"
	}
	return fmt.Sprintf("%s%s_pragma=busy_timeout(%d)&_pragma=journal_mode(WAL)", databasePath, separator, sqliteBusyTimeoutMs)
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
	result, err := tx.ExecContext(ctx, `
		INSERT INTO events (id, type, payload, status, attempts, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO NOTHING
	`, event.ID, event.Type, string(payload), EventStatusQueued, 0, now, now)
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

	_, err = tx.ExecContext(ctx, `
		INSERT INTO outbox (event_id, event_type, payload, attempts, created_at)
		VALUES (?, ?, ?, 0, ?)
	`, event.ID, event.Type, string(payload), now)
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
	rows, err := store.db.QueryContext(ctx, `
		SELECT id, event_id, event_type, payload, attempts
		FROM outbox
		WHERE published_at IS NULL
		ORDER BY id ASC
		LIMIT ?
	`, limit)
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
	_, err := store.db.ExecContext(ctx, `
		UPDATE outbox SET published_at = ?, last_error = ''
		WHERE id = ? AND published_at IS NULL
	`, time.Now().UTC(), messageID)
	if err != nil {
		return fmt.Errorf("mark outbox message published: %w", err)
	}
	return nil
}

func (store *EventStore) MarkOutboxFailed(ctx context.Context, messageID int64, publishError error) error {
	_, err := store.db.ExecContext(ctx, `
		UPDATE outbox SET attempts = attempts + 1, last_error = ?
		WHERE id = ? AND published_at IS NULL
	`, publishError.Error(), messageID)
	if err != nil {
		return fmt.Errorf("mark outbox message failed: %w", err)
	}
	return nil
}
func (store *EventStore) ClaimForProcessing(ctx context.Context, eventID string, leaseDuration time.Duration) (EventClaimResult, error) {
	now := time.Now().UTC()
	leaseExpiredBefore := now.Add(-leaseDuration)
	result, err := store.db.ExecContext(ctx, `
		UPDATE events
		SET status = ?, processing_started_at = ?, updated_at = ?
		WHERE id = ?
		  AND (
			status = ?
			OR (status = ? AND processing_started_at <= ?)
		  )
	`, EventStatusProcessing, now, now, eventID, EventStatusQueued, EventStatusProcessing, leaseExpiredBefore)
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
	if err := store.db.QueryRowContext(ctx, `SELECT status FROM events WHERE id = ?`, eventID).Scan(&status); err != nil {
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
	_, err = store.db.ExecContext(ctx, `
		INSERT INTO dead_letters (event_id, event_type, payload, error, attempts, failed_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(event_id) DO UPDATE SET
			error = excluded.error,
			attempts = excluded.attempts,
			failed_at = excluded.failed_at,
			replayed_at = NULL
	`, event.ID, event.Type, string(payload), processingError.Error(), attempts, time.Now().UTC())
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
	if err := tx.QueryRowContext(ctx, `
		SELECT e.status, e.type, e.payload
		FROM events e
		JOIN dead_letters d ON d.event_id = e.id
		WHERE e.id = ?
	`, eventID).Scan(&status, &eventType, &payload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrDeadLetterNotFound
		}
		return fmt.Errorf("read event for dead letter replay: %w", err)
	}
	if status != EventStatusFailed {
		return fmt.Errorf("%w: current status is %s", ErrDeadLetterNotFailed, status)
	}

	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
		UPDATE events
		SET status = ?, attempts = 0, error = '', processing_started_at = NULL, updated_at = ?
		WHERE id = ? AND status = ?
	`, EventStatusQueued, now, eventID, EventStatusFailed); err != nil {
		return fmt.Errorf("reset event for dead letter replay: %w", err)
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE outbox
		SET attempts = 0, last_error = '', published_at = NULL
		WHERE event_id = ?
	`, eventID)
	if err != nil {
		return fmt.Errorf("reopen outbox message: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check reopened outbox message: %w", err)
	}
	if rowsAffected == 0 {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO outbox (event_id, event_type, payload, attempts, created_at)
			VALUES (?, ?, ?, 0, ?)
		`, eventID, eventType, payload, now); err != nil {
			return fmt.Errorf("create outbox message for replay: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, `UPDATE dead_letters SET replayed_at = ? WHERE event_id = ?`, now, eventID); err != nil {
		return fmt.Errorf("mark dead letter replayed: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit dead letter replay: %w", err)
	}
	return nil
}
func (store *EventStore) ListDeadLetters(ctx context.Context, limit int) ([]DeadLetter, error) {
	rows, err := store.db.QueryContext(ctx, `
		SELECT event_id, event_type, payload, error, attempts, failed_at, replayed_at
		FROM dead_letters
		ORDER BY failed_at DESC
		LIMIT ?
	`, limit)
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
	_, err := store.db.ExecContext(ctx, `
		UPDATE events
		SET status = ?, attempts = ?, error = '', processing_started_at = NULL, updated_at = ?
		WHERE id = ?
	`, EventStatusProcessed, attempts, now, eventID)
	if err != nil {
		return fmt.Errorf("mark event processed: %w", err)
	}

	return nil
}

func (store *EventStore) MarkFailed(ctx context.Context, eventID string, attempts int, processingError error) error {
	now := time.Now().UTC()
	_, err := store.db.ExecContext(ctx, `
		UPDATE events
		SET status = ?, attempts = ?, error = ?, processing_started_at = NULL, updated_at = ?
		WHERE id = ?
	`, EventStatusFailed, attempts, processingError.Error(), now, eventID)
	if err != nil {
		return fmt.Errorf("mark event failed: %w", err)
	}

	return nil
}

func (store *EventStore) List(ctx context.Context, limit int) ([]StoredEvent, error) {
	rows, err := store.db.QueryContext(ctx, `
		SELECT id, type, payload, status, error, attempts, created_at, updated_at
		FROM events
		ORDER BY created_at DESC
		LIMIT ?
	`, limit)
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

func (store *EventStore) migrate(ctx context.Context) error {
	_, err := store.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS events (
			id TEXT PRIMARY KEY,
			type TEXT NOT NULL,
			payload TEXT NOT NULL,
			status TEXT NOT NULL,
			error TEXT NOT NULL DEFAULT '',
			attempts INTEGER NOT NULL DEFAULT 0,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL,
			processing_started_at TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS dead_letters (
			event_id TEXT PRIMARY KEY,
			event_type TEXT NOT NULL,
			payload TEXT NOT NULL,
			error TEXT NOT NULL,
			attempts INTEGER NOT NULL,
			failed_at TIMESTAMP NOT NULL,
			replayed_at TIMESTAMP
		);
		CREATE TABLE IF NOT EXISTS outbox (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_id TEXT NOT NULL UNIQUE,
			event_type TEXT NOT NULL,
			payload TEXT NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			last_error TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMP NOT NULL,
			published_at TIMESTAMP,
			FOREIGN KEY (event_id) REFERENCES events(id)
		);

		INSERT INTO outbox (event_id, event_type, payload, attempts, created_at)
		SELECT id, type, payload, 0, created_at
		FROM events
		WHERE status = 'queued'
		ON CONFLICT(event_id) DO NOTHING;

		CREATE INDEX IF NOT EXISTS idx_events_status ON events(status);
		CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);
		CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox(published_at, id);
	`)
	if err != nil {
		return fmt.Errorf("migrate event store: %w", err)
	}
	if err := store.ensureProcessingStartedAtColumn(ctx); err != nil {
		return err
	}
	if err := store.ensureDeadLetterReplayedAtColumn(ctx); err != nil {
		return err
	}
	return nil
}

func (store *EventStore) ensureDeadLetterReplayedAtColumn(ctx context.Context) error {
	rows, err := store.db.QueryContext(ctx, `PRAGMA table_info(dead_letters)`)
	if err != nil {
		return fmt.Errorf("inspect dead letters schema: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return fmt.Errorf("scan dead letters schema: %w", err)
		}
		if name == "replayed_at" {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate dead letters schema: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close dead letters schema cursor: %w", err)
	}
	if _, err := store.db.ExecContext(ctx, `ALTER TABLE dead_letters ADD COLUMN replayed_at TIMESTAMP`); err != nil {
		return fmt.Errorf("add dead letter replay column: %w", err)
	}
	return nil
}

func (store *EventStore) ensureProcessingStartedAtColumn(ctx context.Context) error {
	rows, err := store.db.QueryContext(ctx, `PRAGMA table_info(events)`)
	if err != nil {
		return fmt.Errorf("inspect events schema: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return fmt.Errorf("scan events schema: %w", err)
		}
		if name == "processing_started_at" {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate events schema: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close events schema cursor: %w", err)
	}
	if _, err := store.db.ExecContext(ctx, `ALTER TABLE events ADD COLUMN processing_started_at TIMESTAMP`); err != nil {
		return fmt.Errorf("add processing lease column: %w", err)
	}
	return nil
}
