CREATE TABLE IF NOT EXISTS players (
    id TEXT PRIMARY KEY,
    username TEXT,
    level INTEGER,
    score BIGINT,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    cdc_operation TEXT,
    cdc_timestamp TIMESTAMP WITH TIME ZONE
);
