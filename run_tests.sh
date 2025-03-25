#!/bin/bash

# Define base directory
BASE_DIR="samples"

# Define database directory
DB_DIR="$BASE_DIR/db"

# Define input and output directories
INPUT_DIR="$BASE_DIR/input"
OUTPUT_DIR="$BASE_DIR/test_output"

# Ensure the output directory exists
mkdir -p "$OUTPUT_DIR"

# Find all .sql query files in the input directory
for query_file in "$INPUT_DIR"/*.sql; do
    # Extract the filename without path
    query_name=$(basename "$query_file")
    
    # Extract query number (assuming filenames are like query1.sql, query2.sql)
    query_num=$(echo "$query_name" | grep -o '[0-9]\+')
    
    # Define output file name
    output_file="$OUTPUT_DIR/output$query_num.txt"

    echo "Running $query_name..."
    java BlazeDB "$DB_DIR" "$query_file" "$output_file"

    # Check if execution was successful
    if [ $? -eq 0 ]; then
        echo "Query $query_num executed successfully."
    else
        echo "Query $query_num failed."
    fi
done

echo "All queries executed."
