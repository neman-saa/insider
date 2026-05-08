CREATE TABLE IF NOT EXISTS markets
(
    id Nullable(String),
    condition_id Nullable(String),
    question Nullable(String),
    created_at Nullable(DateTime64(6)),
    closed_time Nullable(DateTime64(6)),
    event_id Nullable(String),
    volume Nullable(Decimal(38, 9)),
    start_date Nullable(DateTime64(6)),
    end_date Nullable(DateTime64(6))
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS allow_nullable_key = 1;
