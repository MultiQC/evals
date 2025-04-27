from pathlib import Path

from trino.dbapi import connect

CATALOG_NAME = "megaqc-test"
NAMESPACE = "small"
TABLE_NAME = "multiqc"

# Configuration parameters - will assumed data is already generated e.g. by gen-simulated.py
NUM_RUNS = 10
NUM_MODULES = 10
NUM_SECTIONS_PER_MODULE = 5
NUM_SAMPLES_PER_MODULE = 10
NUM_METRICS_PER_MODULE = 20

# Parquet setup
dir_name = f"data/{NUM_RUNS}runs_{NUM_MODULES}mod_{NUM_SAMPLES_PER_MODULE}samples_{NUM_METRICS_PER_MODULE}metrics"
parquet_path = Path(dir_name) / "parquet"

# Trino connection parameters
TRINO_HOST = "trino-coordinator"
TRINO_PORT = 8080
TRINO_USER = "trino"
TRINO_CATALOG = "iceberg"

# Connect to Trino
conn = connect(
    host=TRINO_HOST,
    port=TRINO_PORT,
    user=TRINO_USER,
    catalog=TRINO_CATALOG,
)

# Query the table
cursor = conn.cursor()
cursor.execute(f"SELECT * FROM {NAMESPACE}.{TABLE_NAME}")


