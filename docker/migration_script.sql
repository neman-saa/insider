
-- DESCRIBE TABLE postgresql('insider_postgres:5432', 'insider', 'events', 'postgres', 'postgres') SETTINGS describe_compact_output = 1;

CREATE TABLE events
(
    id Nullable(String),
    volume Nullable(Decimal(38, 19)),
    title Nullable(String),
    start_date Nullable(DateTime64(6)),
    end_date Nullable(DateTime64(6)),
    tags Array(Nullable(String))
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS allow_nullable_key = 1;


INSERT INTO events
SELECT * FROM postgresql('insider_postgres:5432', 'insider', 'events', 'postgres', 'postgres');

-- DESCRIBE TABLE postgresql('insider_postgres:5432', 'insider', 'markets', 'postgres', 'postgres') SETTINGS describe_compact_output = 1;

CREATE TABLE markets
(
    id Nullable(String),
    condition_id Nullable(String),
    question Nullable(String),
    start_date Nullable(DateTime64(6)),
    end_date Nullable(DateTime64(6)),
    event_id Nullable(String),
    volume Nullable(Decimal(38, 19))
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO markets
SELECT * FROM postgresql('insider_postgres:5432', 'insider', 'markets', 'postgres', 'postgres');


--DESCRIBE TABLE postgresql('insider_postgres:5432', 'insider', 'tokens', 'postgres', 'postgres') SETTINGS describe_compact_output = 1;

CREATE TABLE tokens
(
    id Nullable(String),
    market_id Nullable(String),
    outcome Nullable(String),
    last_price Nullable(Decimal(38, 19))
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO tokens
SELECT * FROM postgresql('insider_postgres:5432', 'insider', 'tokens', 'postgres', 'postgres');

-- DESCRIBE TABLE postgresql('insider_postgres:5432', 'insider', 'trades', 'postgres', 'postgres') SETTINGS describe_compact_output = 1;

CREATE TABLE trades
(
    maker_address Nullable(String),
    token_id Nullable(String),
    side Nullable(String),
    amount Nullable(Decimal(38, 19)),
    total_price Nullable(Decimal(38, 19)),
    polygon_tx_hash Nullable(String),
    created_at Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (maker_address, token_id)
SETTINGS allow_nullable_key = 1;

INSERT INTO trades
SELECT * FROM postgresql('insider_postgres:5432', 'insider', 'trades', 'postgres', 'postgres');
