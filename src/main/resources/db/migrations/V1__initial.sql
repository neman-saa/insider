CREATE TABLE "markets"
(
    "id"                 varchar PRIMARY KEY,
    "creator_address_id" varchar,
    "question"           varchar,
    "description"        varchar,
    "start_date"         timestamp,
    "end_date"           timestamp,
    "created_at"         timestamp
);

CREATE TABLE "events"
(
    "id"          varchar PRIMARY KEY,
    "market_id"   varchar,
    "ticker"      varchar,
    "title"       varchar,
    "description" varchar
);

CREATE TABLE "emissions"
(
    "id"                 uuid PRIMARY KEY,
    "market_id"          uuid,
    "emitent_address_id" varchar,
    "amount"             integer,
    "created_at"         timestamp
);

CREATE TABLE "trades"
(
    "id"               uuid PRIMARY KEY,
    "saler_address_id" varchar,
    "buyer_address_id" varchar,
    "event_id"         varchar,
    "amount"           integer,
    "price"            decimal,
    "created_at"       timestamp
);

CREATE TABLE "resolutions"
(
    "id"         uuid PRIMARY KEY,
    "market_id"  varchar,
    "true_event" varchar,
    "created_at" timestamp
);

ALTER TABLE "emissions"
    ADD FOREIGN KEY ("market_id") REFERENCES "markets" ("id");

ALTER TABLE "events"
    ADD FOREIGN KEY ("market_id") REFERENCES "markets" ("id");

ALTER TABLE "trades"
    ADD FOREIGN KEY ("event_id") REFERENCES "events" ("id");

ALTER TABLE "resolutions"
    ADD FOREIGN KEY ("market_id") REFERENCES "markets" ("id");

ALTER TABLE "resolutions"
    ADD FOREIGN KEY ("true_event") REFERENCES "events" ("id");
