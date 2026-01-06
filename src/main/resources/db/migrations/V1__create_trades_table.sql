CREATE TABLE "trades"
(
    "maker_address"   varchar,
    "token_id"        varchar NOT NULL,
    "side"            varchar CHECK (side IN ('BUY', 'SELL')),
    "amount"          decimal,
    "total_price"     decimal,
    "polygon_tx_hash" varchar,
    "created_at"      varchar
);

CREATE INDEX idx_trades_token_id ON trades(token_id);
CREATE INDEX idx_trades_created_at ON trades(created_at);
CREATE INDEX idx_polygon_tx_hash ON trades(polygon_tx_hash);
