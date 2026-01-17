CREATE TABLE "events"
(
    "id"         varchar,
    "volume"     decimal,
    "title"      varchar,
    "start_date" timestamptz,
    "end_date"   timestamptz,
    "tags"       text[]
);

CREATE INDEX idx_events_id ON events (id);

CREATE TABLE "markets"
(
    "id"           varchar,
    "condition_id" varchar,
    "question"     varchar,
    "start_date"   timestamptz,
    "end_date"     timestamptz,
    "event_id"     varchar,
    "volume"       decimal
);

CREATE INDEX idx_markets_id ON markets (id);

CREATE TABLE "tokens"
(
    "id"         varchar,
    "market_id"  varchar,
    "outcome"    varchar,
    "last_price" decimal
);

CREATE INDEX idx_tokens_id ON tokens (id);

CREATE TABLE "trades"
(
    "maker_address"   varchar,
    "token_id"        varchar,
    "side"            varchar,
    "amount"          decimal,
    "total_price"     decimal,
    "polygon_tx_hash" varchar,
    "created_at"      varchar
);

CREATE INDEX idx_trades_maker_address_token_id ON trades (maker_address, token_id);
