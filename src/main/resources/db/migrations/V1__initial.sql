CREATE TABLE "markets"
(
    "id"                 varchar PRIMARY KEY,
    "condition_id"       varchar UNIQUE,
    "market_maker_address" varchar,
    "question"           varchar,
    "description"        varchar,
    "start_date"         timestamp,
    "end_date"           timestamp,
    "created_at"         timestamp,
    "event_id"           varchar
);

CREATE TABLE "outcome_tokens" (
    "id" varchar PRIMARY KEY,
    "market_id" varchar NOT NULL
);

CREATE TABLE "events"
(
    "id"          varchar PRIMARY KEY,
    "ticker"      varchar,
    "title"       varchar,
    "description" varchar
);

CREATE TABLE "trades"
(
    "id"               uuid PRIMARY KEY,
    "side" varchar CHECK (side IN ('BUY', 'SELL')),
    "maker_address" varchar,
    "market_id"        varchar,
    "amount"           integer,
    "total_price"            decimal,
    "created_at"       timestamp,
    "token" varchar NOT NULL,
    "hash" varchar
);

CREATE TABLE "resolutions"
(
    "id"         uuid PRIMARY KEY,
    "market_id"  varchar UNIQUE,
    "true_token" varchar,
    "created_at" timestamp
);

ALTER TABLE "markets"
    ADD FOREIGN KEY ("event_id") REFERENCES "events" ("id");

ALTER TABLE "outcome_tokens"
    ADD FOREIGN KEY ("market_id") REFERENCES "markets" ("id");

--ALTER TABLE "trades"
--    ADD FOREIGN KEY ("token") REFERENCES "outcome_tokens" ("id");
--
--ALTER TABLE "trades"
--    ADD FOREIGN KEY ("market_id") REFERENCES "markets" ("id");

ALTER TABLE "resolutions"
    ADD FOREIGN KEY ("market_id") REFERENCES "markets" ("id");

ALTER TABLE "resolutions"
    ADD FOREIGN KEY ("true_token") REFERENCES "outcome_tokens" ("id");

CREATE INDEX idx_trades_market_id ON trades(market_id);
CREATE INDEX idx_trades_token ON trades(token);
CREATE INDEX idx_trades_created_at ON trades(created_at);
CREATE UNIQUE INDEX idx_trades_hash ON trades(hash);

