DROP INDEX IF EXISTS idx_dead_letters_retryable;
ALTER TABLE dead_letters DROP COLUMN last_retry_error;
ALTER TABLE dead_letters DROP COLUMN next_retry_at;
ALTER TABLE dead_letters DROP COLUMN replay_attempts;
