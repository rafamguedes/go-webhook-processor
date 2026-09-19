ALTER TABLE outbox ADD COLUMN IF NOT EXISTS dispatch_token TEXT;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS dispatching_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_outbox_claimable
    ON outbox (dispatching_at, id)
    WHERE published_at IS NULL;
