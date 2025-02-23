#!/bin/bash

# Define the database connection parameters
DB_HOST="localhost"
DB_PORT="3004"
DB_NAME="bitcoin"
DB_USER="abc"
DB_PASSWORD="12345"

# Function to reindex a given index with no timeout
reindex_index_no_timeout() {
  local index_name=$1
  PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
  BEGIN;
  SET statement_timeout = 0;
  REINDEX INDEX $index_name;
  COMMIT;" &
}

# Reindex all indices in parallel with no timeout
reindex_index_no_timeout "idx_unprocessed_transactions_for_balance_txid"
reindex_index_no_timeout "idx_unprocessed_transactions_for_balance_block_number"
reindex_index_no_timeout "idx_balance_java_address"
reindex_index_no_timeout "idx_balance_java_txid"
reindex_index_no_timeout "idx_balance_java_block_number"
reindex_index_no_timeout "idx_transactions_java_indexed_txid"
reindex_index_no_timeout "idx_transactions_java_indexed_block_number"
reindex_index_no_timeout "idx_transactions_java_indexed_unique"

# Wait for all background processes to complete
wait