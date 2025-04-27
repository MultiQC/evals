"""
This script is used to setup the Iceberg table for the MultiQC data.

It will create the table and load the data from the parquet files in the data directory.

The data is assumed to be already generated e.g. by gen-simulated.py
"""

import os
from pathlib import Path

import pyarrow.dataset as ds
from dotenv import load_dotenv
from pyiceberg.catalog.rest import RestCatalog

# Configuration parameters - will assumed data is already generated e.g. by gen-simulated.py
NUM_RUNS = 10
NUM_MODULES = 10
NUM_SECTIONS_PER_MODULE = 5
NUM_SAMPLES_PER_MODULE = 10
NUM_METRICS_PER_MODULE = 20

# Parquet setup
dataset = f"{NUM_RUNS}runs_{NUM_MODULES}mod_{NUM_SAMPLES_PER_MODULE}samples_{NUM_METRICS_PER_MODULE}metrics"
local_parquet_path = Path("data") / dataset / "parquet"

load_dotenv()
# Define catalog connection details (replace variables)
TOKEN = os.environ["CLOUDFLARE_TOKEN"]
WAREHOUSE = "8234d07c9c28a6f6c380fe45731ba8e4_megaqc-test"
CATALOG_URI = "https://catalog.cloudflarestorage.com/8234d07c9c28a6f6c380fe45731ba8e4/megaqc-test"

CATALOG_NAME = "megaqc-test"
NAMESPACE = dataset
TABLE_NAME = "multiqc"

# Upload the local data to Cloudflare R2
"""
PREFIX="simulated/100runs_10mod_100samples_20metrics/parquet"
BUCKET="megaqc-test"
find ./10runs_10mod_10samples_20metrics -type f -print0 |
  while IFS= read -r -d '' file; do
    key="$PREFIX/$(basename "$file")"
    npx wrangler r2 object put "$BUCKET/$key" --file "$file"
  done
"""

# Connect to R2 Data Catalog
catalog = RestCatalog(
    name=CATALOG_NAME,
    token=TOKEN,
    warehouse=WAREHOUSE,
    uri=CATALOG_URI,
)

# Create default namespace if it doesn't exist
if not catalog.namespace_exists(NAMESPACE):
    catalog.create_namespace(NAMESPACE)

# Drop table if it exists
if catalog.table_exists((NAMESPACE, TABLE_NAME)):
    catalog.drop_table((NAMESPACE, TABLE_NAME))


# Load from parquet with pyarrow
# First, create a dataset from all parquet files
dataset = ds.dataset(
    str(local_parquet_path),
    format="parquet",
)

# Convert to PyArrow table
pa_table = dataset.to_table()
print(f"Loaded PyArrow table with schema: {pa_table.schema}")

# Create or replace the Iceberg table
if catalog.table_exists((NAMESPACE, TABLE_NAME)):
    print(f"Dropping existing table: {TABLE_NAME}")
    catalog.drop_table((NAMESPACE, TABLE_NAME))

print(f"Creating Iceberg table: {TABLE_NAME}")
iceberg_table = catalog.create_table(
    (NAMESPACE, TABLE_NAME),
    schema=pa_table.schema,
)

# Load the table and append data
iceberg_table = catalog.load_table((NAMESPACE, TABLE_NAME))
print(f"Appending {pa_table.num_rows} rows to Iceberg table")
iceberg_table.append(pa_table)

print(f"Successfully loaded data into Iceberg table: {TABLE_NAME}")
