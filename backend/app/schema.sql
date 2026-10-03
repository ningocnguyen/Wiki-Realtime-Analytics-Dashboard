CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    user_id TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL DEFAULT 0,
    props JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, event_id)
);

CREATE INDEX IF NOT EXISTS events_occurred_brin ON events USING BRIN (occurred_at);
CREATE INDEX IF NOT EXISTS events_src_type_time_idx
    ON events (source, event_type, occurred_at DESC);
CREATE INDEX IF NOT EXISTS events_user_time_idx ON events (user_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS events_src_id_idx ON events (source, id DESC);

CREATE TABLE IF NOT EXISTS event_rollup_minute (
    source TEXT NOT NULL,
    bucket TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    cnt BIGINT NOT NULL DEFAULT 0,
    value_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    PRIMARY KEY (source, bucket, event_type)
);
