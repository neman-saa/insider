CREATE TABLE IF NOT EXISTS operations_audit
(
    token_id Nullable(String),
    side Nullable(String),
    moment_score Nullable(Decimal(38, 9)),
    price Nullable(Decimal(38, 9)),
    timestamp Nullable(DateTime64(6))
)
ENGINE = MergeTree()
ORDER BY token_id
SETTINGS allow_nullable_key = 1;
