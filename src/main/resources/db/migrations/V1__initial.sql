CREATE TABLE markets (
    conditionId TEXT PRIMARY KEY,
    id TEXT NOT NULL,
    question TEXT NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    tokens TEXT[] NOT NULL CHECK (cardinality(tokens) >= 2)
);

CREATE TABLE trades (
    conditionId TEXT REFERENCES markets(conditionId) ON DELETE CASCADE,
    wallet TEXT NOT NULL,
    outcome TEXT CHECK (outcome IN ('YES', 'NO', 'OTHER')) NOT NULL,
    outcomeMessage TEXT,
    tokenId TEXT NOT NULL,
    totalSize DOUBLE PRECISION NOT NULL,
    totalPrice DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (conditionId, tokenId)
);