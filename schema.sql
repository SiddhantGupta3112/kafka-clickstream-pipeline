CREATE TABLE IF NOT EXISTS active_sessions (
    user_id TEXT PRIMARY KEY,
    session_id UUID NOT NULL DEFAULT gen_random_uuid(),
    session_start TIMESTAMPTZ NOT NULL,
    last_event_at TIMESTAMPTZ NOT NULL,
    event_count INT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS closed_sessions (
    session_id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    session_start TIMESTAMPTZ NOT NULL,
    session_end TIMESTAMPTZ NOT NULL,
    event_count INT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_active_sessions_last_event
    ON active_sessions (last_event_at);