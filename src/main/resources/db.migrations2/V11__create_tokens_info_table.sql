CREATE TABLE IF NOT EXISTS tokens_info
(
    id String,
    price Decimal(38, 9),
    score Decimal(38, 9),
    resolve_date DateTime64(6),
    last_updated_block_num Int64,
    buy_time Nullable(DateTime64(6)),
    buy_price Nullable(Decimal(38, 9))
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS allow_nullable_key = 1;
