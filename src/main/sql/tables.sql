CREATE TABLE IF NOT EXISTS transactions_java_indexed (
    txid VARCHAR(64) NOT NULL,
    block_number INT NOT NULL,
    data BYTEA,
    readable_data TEXT
);

CREATE INDEX IF NOT EXISTS idx_transactions_java_indexed_txid ON transactions_java_indexed(txid);
CREATE INDEX IF NOT EXISTS idx_transactions_java_indexed_block_number ON transactions_java_indexed(block_number);

CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_java_indexed_unique 
ON transactions_java_indexed(txid, block_number);

CREATE TABLE IF NOT EXISTS balance_java (
    address VARCHAR(130),
    txid VARCHAR(64),
    block_number INT,
    balance BIGINT
);

CREATE INDEX IF NOT EXISTS idx_balance_java_address ON balance_java(address);
CREATE INDEX IF NOT EXISTS idx_balance_java_txid ON balance_java(txid);
CREATE INDEX IF NOT EXISTS idx_balance_java_block_number ON balance_java(block_number);
ALTER TABLE balance_java
ADD CONSTRAINT unique_balance_entry UNIQUE (address, txid, block_number);


CREATE TABLE IF NOT EXISTS unprocessed_transactions_for_balance (
    txid VARCHAR(64) NOT NULL,
    block_number INT NOT NULL,
    UNIQUE (txid, block_number)
);

CREATE INDEX IF NOT EXISTS idx_unprocessed_transactions_for_balance_txid ON unprocessed_transactions_for_balance(txid);
CREATE INDEX IF NOT EXISTS idx_unprocessed_transactions_for_balance_block_number ON unprocessed_transactions_for_balance(block_number);

CREATE TABLE IF NOT EXISTS timeseries_hodl_1y (
    timestamp TIMESTAMP NOT NULL,
    block_number INT NOT NULL,
    value FLOAT
);

CREATE INDEX IF NOT EXISTS idx_timeseries_hodl_1y_timestamp ON timeseries_hodl_1y(timestamp);
CREATE INDEX IF NOT EXISTS idx_timeseries_hodl_1y_block_number ON timeseries_hodl_1y(block_number);

CREATE TABLE IF NOT EXISTS timeseries_hodl_2y (
    timestamp TIMESTAMP NOT NULL,
    block_number INT NOT NULL,
    value FLOAT
);

CREATE INDEX IF NOT EXISTS idx_timeseries_hodl_2y_timestamp ON timeseries_hodl_2y(timestamp);
CREATE INDEX IF NOT EXISTS idx_timeseries_hodl_2y_block_number ON timeseries_hodl_2y(block_number);

CREATE TABLE IF NOT EXISTS timeseries_hodl_3y (
    timestamp TIMESTAMP NOT NULL,
    block_number INT NOT NULL,
    value FLOAT
);

CREATE INDEX IF NOT EXISTS idx_timeseries_hodl_3y_timestamp ON timeseries_hodl_3y(timestamp);
CREATE INDEX IF NOT EXISTS idx_timeseries_hodl_3y_block_number ON timeseries_hodl_3y(block_number);


