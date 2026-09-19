DROP INDEX IF EXISTS idx_outbox_claimable;
ALTER TABLE outbox DROP COLUMN IF EXISTS dispatching_at;
ALTER TABLE outbox DROP COLUMN IF EXISTS dispatch_token;
