CREATE TABLE transactions (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    amount_original NUMERIC NOT NULL,
    currency TEXT NOT NULL,
    amount_usd NUMERIC NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE INDEX transactions_user_id_timestamp_idx
    ON transactions (user_id, timestamp);

CREATE TABLE processed_events (
    id TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL
);
