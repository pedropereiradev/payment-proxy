CREATE TABLE IF NOT EXISTS payments (
    id SERIAL PRIMARY KEY,
    correlation_id UUID NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    processor VARCHAR(10) NOT NULL CHECK (processor IN ('default', 'fallback')),
    requested_at TIMESTAMPTZ NOT NULL
)
