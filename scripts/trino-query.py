import os
from pathlib import Path

from dotenv import load_dotenv

from trino.auth import BasicAuthentication
from trino.dbapi import connect

# Load environment variables
load_dotenv()
CLOUDFLARE_TOKEN = os.environ["CLOUDFLARE_TOKEN"]

# Configuration parameters
NUM_RUNS = 10
NUM_MODULES = 10
NUM_SECTIONS_PER_MODULE = 5
NUM_SAMPLES_PER_MODULE = 10
NUM_METRICS_PER_MODULE = 20

# Dataset name
dataset = f"ns_{NUM_RUNS}runs_{NUM_MODULES}mod_{NUM_SAMPLES_PER_MODULE}samples_{NUM_METRICS_PER_MODULE}metrics"

# Trino connection parameters
TRINO_HOST = "localhost"  # Use appropriate hostname if not local
TRINO_PORT = 8080
TRINO_USER = "trino"
TRINO_CATALOG = "iceberg"  # This will be created dynamically
TRINO_SCHEMA = dataset  # This matches your namespace in Iceberg

# Connect to Trino
conn = connect(
    host=TRINO_HOST,
    port=TRINO_PORT,
    user=TRINO_USER,
    catalog=TRINO_CATALOG,
    schema=TRINO_SCHEMA,
)

# Create a cursor
cursor = conn.cursor()

# First, check if catalog exists
cursor.execute("SHOW CATALOGS")
catalogs = [row[0] for row in cursor.fetchall()]

# If iceberg catalog doesn't exist, create it
if "iceberg" not in catalogs:
    create_catalog_sql = """
    CREATE CATALOG iceberg USING iceberg WITH (
        "iceberg.catalog.type" = 'rest',
        "iceberg.rest-catalog.uri" = 'https://catalog.cloudflarestorage.com/8234d07c9c28a6f6c380fe45731ba8e4/megaqc-test',
        "iceberg.rest-catalog.oauth2.token" = '{}',
        "iceberg.file-format" = 'PARQUET',
        "iceberg.compression-codec" = 'GZIP'
    )
    """.format(CLOUDFLARE_TOKEN)

    cursor.execute(create_catalog_sql)
    print("Created Iceberg catalog")

# Query the table
cursor.execute(f"SELECT * FROM {TRINO_SCHEMA}.multiqc LIMIT 10")
rows = cursor.fetchall()
for row in rows:
    print(row)

# Close the connection
conn.close()
