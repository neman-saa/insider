CREATE TABLE IF NOT EXISTS wallets2
(
    wallet_id Nullable(String),
    initial_balance Decimal(38, 9),
    final_balance Decimal(38, 9),
    active_from_block Int32,
    active_to_block Nullable(Int32),
    insert_time DateTime64(6)
)
ENGINE = MergeTree
ORDER BY (active_from_block);
