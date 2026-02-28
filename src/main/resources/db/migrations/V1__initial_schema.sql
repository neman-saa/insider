CREATE TABLE IF NOT EXISTS events
(
    id Nullable(String),
    volume Nullable(Decimal(38, 9)),
    title Nullable(String),
    start_date Nullable(DateTime64(6)),
    end_date Nullable(DateTime64(6)),
    tags Array(Nullable(String))
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS allow_nullable_key = 1;

CREATE TABLE TABLE IF NOT EXISTS markets
(
    id Nullable(String),
    condition_id Nullable(String),
    question Nullable(String),
    start_date Nullable(DateTime64(6)),
    end_date Nullable(DateTime64(6)),
    event_id Nullable(String),
    volume Nullable(Decimal(38, 9))
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS allow_nullable_key = 1;

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

CREATE TABLE IF NOT EXISTS trades
(
    maker_address Nullable(String),
    token_id Nullable(String),
    side Nullable(String),
    amount Nullable(Decimal(38, 9)),
    total_price Nullable(Decimal(38, 9)),
    polygon_tx_hash Nullable(String),
    created_at Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (maker_address, token_id)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS trades_v2
(
    maker_address Nullable(String),
    token_id Nullable(String),
    side Nullable(String),
    amount Nullable(Decimal(38, 9)),
    total_price Nullable(Decimal(38, 9)),
    block_num Nullable(Int64),
    tx_hash Nullable(String),
    tx_index Nullable(Int32),
    block_timestamp Nullable(DateTime64(6))
)
ENGINE = MergeTree()
ORDER BY (maker_address, token_id) -- review
SETTINGS allow_nullable_key = 1;
