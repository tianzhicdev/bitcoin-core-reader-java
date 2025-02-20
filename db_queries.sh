#!/bin/bash

# This script prints active queries in the local PostgreSQL database at port 3004

# Define the database connection parameters
DB_HOST="localhost"
DB_PORT="3004"
DB_NAME="bitcoin"
DB_USER="abc"

# Execute the SQL query to get active queries
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
SELECT pid, usename, datname, state, query_start, query
FROM pg_stat_activity
WHERE state = 'active';
"
