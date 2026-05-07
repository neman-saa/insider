CREATE TABLE IF NOT EXISTS trades
(
    maker_address Nullable(String),
    token_id Nullable(String),
    side Nullable(String),
    amount Nullable(Decimal(38, 9)),
    total_price Nullable(Decimal(38, 9)),
    block_num Nullable(Int64),
    tx_hash Nullable(String),
    tx_index Nullable(Int32),
    block_timestamp Nullable(DateTime64(6)),
    ctf_type Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (maker_address, token_id)
SETTINGS allow_nullable_key = 1;
