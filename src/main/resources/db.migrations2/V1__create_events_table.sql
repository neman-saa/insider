CREATE TABLE IF NOT EXISTS events
(
    id Nullable(String),
    volume Nullable(Decimal(38, 9)),
    title Nullable(String),
    created_at Nullable(DateTime64(6)),
    closed_time Nullable(DateTime64(6)),
    tags Array(Nullable(String)),
    start_date Nullable(DateTime64(6)),
    end_date Nullable(DateTime64(6)),
    slug Nullable(String),
    closed Nullable(Bool)
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS allow_nullable_key = 1;
