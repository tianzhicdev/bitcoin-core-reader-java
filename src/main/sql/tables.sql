CREATE TABLE transactions_java (
    txid VARCHAR(64),
    block_number INT,
    data BYTEA,
    readable_data TEXT
);

-- todo: add index later