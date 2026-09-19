DROP INDEX IF EXISTS idx_events_request_id;
ALTER TABLE outbox DROP COLUMN request_id;
ALTER TABLE events DROP COLUMN request_id;
