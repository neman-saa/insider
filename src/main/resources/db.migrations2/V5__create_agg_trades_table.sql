CREATE TABLE agg_trades
(
    maker_address String,
    token_id String,
    data SimpleAggregateFunction(
        groupArrayArray,
        Array(
            Tuple(
                Int64,           -- block number
                Int64,           -- tx index
                String,          -- side
                Int64,           -- amount
                Int64            -- total price
            )
        )
    ),
    last_activity_timestamp SimpleAggregateFunction(max, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (maker_address, token_id);
