CREATE TABLE IF NOT EXISTS trades_simulations
(
    block_timestamp   DateTime64(6),
    block_num         Int32,
    tx_index          Int32,
    maker_address     String,
    token_id          String,
    opposite_token_id String,
    side              String,
    amount            Decimal(38, 9),
    total_price       Decimal(38, 9),
    last_price        Int32,
    end_time          DateTime64(6),
    start_time        DateTime64(6),
    market_id         String
)
ENGINE = MergeTree
ORDER BY (block_num, tx_index, maker_address);

INSERT INTO trades_simulations
SELECT
    tr.block_timestamp,
    tr.block_num,
    tr.tx_index,
    tr.maker_address,
    tr.token_id,
    t_opposite.id AS opposite_token_id,
    tr.side,
    tr.amount,
    tr.total_price,
    t.last_price,
    m.endDate,
    m.startDate,
    m.id
FROM trades tr
JOIN tokens t
    ON t.id = tr.token_id
JOIN markets m
    ON m.id = t.market_id
JOIN tokens t_opposite
    ON t_opposite.market_id = t.market_id
   AND t_opposite.id != t.id
WHERE tr.block_num IS NOT NULL
  AND tr.tx_index IS NOT NULL;
