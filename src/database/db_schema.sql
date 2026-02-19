CREATE TABLE IF NOT EXISTS heartbeats (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id UUID NOT NULL,
    heart_rate INTEGER NOT NULL CHECK (heart_rate BETWEEN 30 AND 220),
    event_time TIMESTAMP NOT NULL,
    risk_level VARCHAR(20),
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_customer_time 
ON heartbeats (customer_id, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_event_time 
ON heartbeats (event_time DESC);

CREATE TABLE IF NOT EXISTS heartbeat_aggregates (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    customer_id UUID NOT NULL,
    avg_heart_rate DOUBLE PRECISION NOT NULL,
    max_heart_rate INTEGER NOT NULL,
    PRIMARY KEY (window_start, customer_id)
);
