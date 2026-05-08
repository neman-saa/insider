CREATE TABLE IF NOT EXISTS tokens
(
    id Nullable(String),
    market_id Nullable(String),
    outcome Nullable(String),
    last_price Nullable(Decimal(38, 9))
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS allow_nullable_key = 1;
