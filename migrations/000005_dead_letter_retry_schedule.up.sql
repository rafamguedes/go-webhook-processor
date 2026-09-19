ALTER TABLE dead_letters ADD COLUMN replay_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE dead_letters ADD COLUMN next_retry_at TIMESTAMPTZ;
ALTER TABLE dead_letters ADD COLUMN last_retry_error TEXT NOT NULL DEFAULT '';
CREATE INDEX idx_dead_letters_retryable ON dead_letters(next_retry_at, failed_at) WHERE replayed_at IS NULL AND next_retry_at IS NOT NULL;
