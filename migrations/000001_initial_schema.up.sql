CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    error TEXT NOT NULL DEFAULT '',
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    processing_started_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS dead_letters (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    error TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL,
    replayed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS outbox (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE REFERENCES events(id),
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ
);

INSERT INTO outbox (event_id, event_type, payload, attempts, created_at)
SELECT id, type, payload, 0, created_at
FROM events
WHERE status = 'queued'
ON CONFLICT (event_id) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_events_status ON events(status);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox(published_at, id);
