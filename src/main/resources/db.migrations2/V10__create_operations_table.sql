CREATE TABLE IF NOT EXISTS operations (
	wallet_id String,
	token_id String,
	side String,
	size Decimal(38, 9),
	total_price Decimal(38, 9),
	block_num Int
)
ENGINE = MergeTree
ORDER BY (wallet_id, block_num);
