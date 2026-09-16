package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

var ErrEventAlreadyExists = errors.New("event already exists")

const (
	EventStatusQueued    = "queued"
	EventStatusProcessed = "processed"
	EventStatusFailed    = "failed"
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

type EventStore struct {
	db *sql.DB
}

func OpenEventStore(databasePath string) (*EventStore, error) {
	db, err := sql.Open("sqlite", databasePath)
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

func (store *EventStore) Close() error {
	return store.db.Close()
}

func (store *EventStore) SaveQueued(ctx context.Context, event Event) error {
	payload, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("marshal event payload: %w", err)
	}

	now := time.Now().UTC()
	result, err := store.db.ExecContext(ctx, `
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

	return nil
}

func (store *EventStore) MarkProcessed(ctx context.Context, eventID string, attempts int) error {
	now := time.Now().UTC()
	_, err := store.db.ExecContext(ctx, `
		UPDATE events
		SET status = ?, attempts = ?, error = '', updated_at = ?
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
		SET status = ?, attempts = ?, error = ?, updated_at = ?
		WHERE id = ?
	`, EventStatusFailed, attempts, processingError.Error(), now, eventID)
	if err != nil {
		return fmt.Errorf("mark event failed: %w", err)
	}

	return nil
}

func (store *EventStore) ListQueued(ctx context.Context) ([]Event, error) {
	rows, err := store.db.QueryContext(ctx, `
		SELECT id, type, payload
		FROM events
		WHERE status = ?
		ORDER BY created_at ASC
	`, EventStatusQueued)
	if err != nil {
		return nil, fmt.Errorf("list queued events: %w", err)
	}
	defer rows.Close()

	events := make([]Event, 0)
	for rows.Next() {
		var event Event
		var payload string
		if err := rows.Scan(&event.ID, &event.Type, &payload); err != nil {
			return nil, fmt.Errorf("scan queued event: %w", err)
		}
		if err := json.Unmarshal([]byte(payload), &event.Payload); err != nil {
			return nil, fmt.Errorf("decode queued event %s payload: %w", event.ID, err)
		}
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate queued events: %w", err)
	}

	return events, nil
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
			updated_at TIMESTAMP NOT NULL
		);

		CREATE INDEX IF NOT EXISTS idx_events_status ON events(status);
		CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);
	`)
	if err != nil {
		return fmt.Errorf("migrate event store: %w", err)
	}

	return nil
}
