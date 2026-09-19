ALTER TABLE events ADD COLUMN request_id TEXT NOT NULL DEFAULT '';
ALTER TABLE outbox ADD COLUMN request_id TEXT NOT NULL DEFAULT '';
CREATE INDEX idx_events_request_id ON events(request_id) WHERE request_id <> '';
