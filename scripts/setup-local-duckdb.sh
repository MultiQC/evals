#!/bin/bash
# Setup script for local DuckDB installation with Iceberg support

# Load environment variables
set -a
source .env
set +a

# Get DuckDB version
DUCKDB_VERSION=$(duckdb --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' || echo "unknown")
echo "DuckDB version: $DUCKDB_VERSION"

# Create temporary SQL file with environment variables expanded
cat > /tmp/bootstrap_expanded.sql << EOF
-- Load required extensions
INSTALL iceberg;
INSTALL httpfs;
LOAD iceberg;
LOAD httpfs;

-- Set Cloudflare credentials
SET s3_region='auto';
SET s3_endpoint='https://8234d07c9c28a6f6c380fe45731ba8e4.r2.cloudflarestorage.com';
SET s3_access_key_id='$CLOUDFLARE_ACCESS_KEY_ID';
SET s3_secret_access_key='$CLOUDFLARE_SECRET_ACCESS_KEY';

CREATE VIEW runs AS
SELECT *
FROM iceberg_scan(
  's3://megaqc-test/simulated/10runs_10mod_10samples_20metrics/parquet/metadata/v1.metadata.json'
);

-- Try to query the catalog
SELECT 'Attempting to query the Iceberg catalog';
SELECT * FROM iceberg_tables() LIMIT 10;

-- Or try listing from namespace directly
SELECT 'Attempting to query data directly';
SELECT * FROM '$NAMESPACE.$TABLE_NAME' LIMIT 5;
EOF

# Create the database directory if it doesn't exist
mkdir -p data

# Initialize the DuckDB database with the bootstrap SQL
duckdb data/megaqc.duckdb < /tmp/bootstrap_expanded.sql

echo "DuckDB setup complete. Database file created at data/megaqc.duckdb"
echo "To start DuckDB server: duckdb --listen 0.0.0.0 --port 9000 data/megaqc.duckdb"

# Remove the temporary file
rm /tmp/bootstrap_expanded.sql 