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