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

CREATE TABLE IF NOT EXISTS token_opposites
(
    token_id          String,
    opposite_token_id String
)
ENGINE = MergeTree
ORDER BY token_id;

INSERT INTO token_opposites
SELECT
    t.id AS token_id,
    if(arr[1] = t.id, arr[2], arr[1]) AS opposite_token_id
FROM tokens t
JOIN
(
    SELECT
        market_id,
        groupArray(id) AS arr
    FROM tokens
    GROUP BY market_id
    HAVING length(arr) = 2
) x
    ON x.market_id = t.market_id;

INSERT INTO trades_simulations
SELECT
    tr.block_timestamp,
    tr.block_num,
    tr.tx_index,
    tr.maker_address,
    tr.token_id,
    opp.opposite_token_id,
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
JOIN token_opposites opp
    ON opp.token_id = tr.token_id
WHERE tr.block_num IS NOT NULL
  AND tr.tx_index IS NOT NULL
  AND tr.block_timestamp IS NOT NULL
  AND cityHash64(tr.maker_address, tr.token_id) % 16 = 0;
