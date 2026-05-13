CREATE TABLE IF NOT EXISTS operations_audit
(
    token_id Nullable(String),
    side Nullable(String),
    moment_score Nullable(Decimal(38, 9)),
    total_price Nullable(Decimal(38, 9)),
    total_shares Nullable(Decimal(38, 9)),
    prev_single_token_price Nullable(Decimal(38, 9)),
    single_token_price Nullable(Decimal(38, 9)),
    timestamp Nullable(DateTime64(6))
)
ENGINE = MergeTree()
ORDER BY token_id
SETTINGS allow_nullable_key = 1;
