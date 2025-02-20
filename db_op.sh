#!/bin/bash

# This script takes a SQL query string as an argument and runs it against the local PostgreSQL database at port 3004

# Check if a query string is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <SQL_QUERY>"
  exit 1
fi

# Define the database connection parameters
DB_HOST="localhost"
DB_PORT="3004"
DB_NAME="bitcoin"
DB_USER="abc"

# Execute the provided SQL query
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "$1"
