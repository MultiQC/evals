"""
This script is used to setup an Iceberg table for the MultiQC data in either AWS Glue or Cloudflare.

It will create the table and load the data from the parquet files already stored in S3 or R2.

The data is assumed to be already generated and uploaded e.g. by gen-simulated.py

Options:
  --s3-prefix: Prefix of the S3 bucket
  --drop: Drop existing table before creating a new one
  --platform: Choose between AWS Glue or Cloudflare R2 (default: glue)
  --format: Format of the data to load (default: wide)
  --max-files: Maximum number of files to process from the input prefix (default: all files).
               When specified, the namespace and table names will be suffixed with "_subsetN"
               where N is the number of files.
  --files-per-chunk: Number of files to register with the Iceberg table per chunk (default: 1000)
"""

import argparse
import datetime
import os
import time

import boto3
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError
from cloudpathlib.cloudpath import CloudPath
from dotenv import load_dotenv
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.transforms import IdentityTransform, MonthTransform, Transform, TruncateTransform
from pyiceberg.types import BooleanType, DoubleType, IntegerType, LongType, NestedField, StringType, TimestampType

# Load environment variables
load_dotenv()

# Parse command line arguments
parser = argparse.ArgumentParser(description="Setup Iceberg table for MultiQC data.")
parser.add_argument(
    "--s3-prefix",
    type=str,
    required=True,
    help="Prefix of the S3 bucket",
)
parser.add_argument(
    "--drop",
    action="store_true",
    help="Drop existing table before creating a new one",
)
parser.add_argument(
    "--platform",
    choices=["cloudflare", "glue"],
    default="glue",
    help="Choose between AWS Glue or Cloudflare R2 (default: glue)",
)
parser.add_argument(
    "--format",
    choices=["wide", "long"],
    default="wide",
    help="Format of the data to load (default: wide)",
)
parser.add_argument(
    "--max-files",
    type=int,
    default=None,
    help="Maximum number of files to process from the input prefix (default: all files). "
    "When specified, the namespace and table names will be suffixed with '_subsetN' where N is the number of files.",
)
parser.add_argument(
    "--start-from",
    type=int,
    default=1,
    help="Start from a specific chunk (default: 1)",
)
parser.add_argument(
    "--files-per-chunk",
    type=int,
    default=1000,
    help="Number of files to register with the Iceberg table per chunk (default: 1000)",
)
args = parser.parse_args()


CATALOG_NAME = "megaqc-test"
args.s3_prefix = args.s3_prefix.strip("/")
is_reallife = args.s3_prefix.endswith("multiqc.parquet")
namespace = args.s3_prefix
if namespace.endswith("multiqc.parquet"):
    namespace = namespace.replace("/multiqc.parquet", "")
namespace = namespace.replace("/", "-")
print(f"S3 prefix: {args.s3_prefix}, Namespace: {namespace}")
table_name = namespace

# If max_files is specified, update the namespace and table_name to indicate this is a subset
if args.max_files is not None:
    subset_suffix = f"_subset{args.max_files}"
    namespace = f"{namespace}{subset_suffix}"
    table_name = f"{table_name}{subset_suffix}"
    print(f"Processing subset of {args.max_files} files, updated namespace: {namespace}, table_name: {table_name}")

# AWS Glue configuration
ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
region = os.environ["AWS_REGION"]
BUCKET_NAME = "vlad-megaqc"
s3_prefix = f"{args.s3_prefix}/"
S3_WAREHOUSE_LOCATION = f"s3://{BUCKET_NAME}/warehouse/{args.s3_prefix}/"

# Create appropriate filesystem for PyArrow to access storage
s3_fs = fs.S3FileSystem(
    access_key=ACCESS_KEY_ID,
    secret_key=SECRET_ACCESS_KEY,
    region=region,
    request_timeout=600,
    connect_timeout=60,
)

# Find first parquet file in the prefix to get the schema
storage_path = f"{BUCKET_NAME}/{s3_prefix}".strip("/")

if is_reallife:
    parquet_path = storage_path
    files = [parquet_path]
else:
    files = []
    files = s3_fs.get_file_info(fs.FileSelector(storage_path, recursive=True))
    files = [f.path for f in files if f.path.endswith(".parquet")]
    if not files:
        raise ValueError(f"No parquet files found in {storage_path}")
    parquet_path = files[0]

    if args.max_files and args.max_files < len(files):
        print(f"Will limit to first {args.max_files} of {len(files)} parquet files found (using namespace {namespace})")
        files = files[: args.max_files]

metadata = pq.read_metadata(parquet_path, filesystem=s3_fs)
schema = metadata.schema.to_arrow_schema()
print(f"Successfully read schema with {len(schema.names)} columns: {schema.names}")

# Create PyIceberg schema from PyArrow schema
iceberg_fields = []
for i, field in enumerate(schema):
    arrow_type = field.type
    if pa.types.is_string(arrow_type):
        iceberg_type = StringType()
    elif pa.types.is_timestamp(arrow_type):
        iceberg_type = TimestampType()
    elif pa.types.is_integer(arrow_type):
        iceberg_type = LongType()
    elif pa.types.is_floating(arrow_type):
        iceberg_type = DoubleType()
    elif pa.types.is_boolean(arrow_type):
        iceberg_type = BooleanType()
    else:  # Default to string for complex types
        iceberg_type = StringType()
    iceberg_fields.append(NestedField(field_id=i + 1, name=field.name, field_type=iceberg_type, required=False))

iceberg_schema = Schema(*iceberg_fields)
print("Successfully converted PyArrow schema to PyIceberg schema")

# else:
#     print("Use all files in the prefix to create the dataset")
#     dataset = ds.dataset(storage_path, format="parquet", filesystem=s3_fs)
#
# # Convert to PyArrow table
# pa_table = dataset.to_table()
# print(f"Loaded PyArrow table with schema: {pa_table.schema}")

# Build catalog
# Configure boto3 session
boto_config = Config(retries={"max_attempts": 10, "mode": "adaptive"}, connect_timeout=120, read_timeout=300)

boto3_session = boto3.Session(
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY,
    region_name=region,
)

# Apply the config to all clients created from this session
boto3_session._session.set_config_variable("boto_config", boto_config)

# Connect to AWS Glue Catalog
catalog = GlueCatalog(
    name="glue",
    boto3_session=boto3_session,
    warehouse=S3_WAREHOUSE_LOCATION,
)

# Verify auth is working by trying to list namespaces
try:
    namespaces = catalog.list_namespaces()
    print(f"Successfully authenticated with {args.platform.upper()}. Available namespaces: {namespaces}")
except Exception as e:
    print(f"Authentication error: {e}")
    raise

try:
    catalog.create_namespace(namespace)
    print(f"Created namespace: {namespace}")
except Exception as e:
    print(f"Namespace already exists or error creating: {e}")

# Check if table exists and delete it if requested
if args.drop:
    try:
        print(f"Dropping existing table: {namespace}.{table_name}")
        catalog.drop_table((namespace, table_name))
        print(f"Table {namespace}.{table_name} dropped successfully")
    except NoSuchTableError:
        print(f"Table {namespace}.{table_name} does not exist, nothing to drop")
    except Exception as e:
        print(f"Error dropping table: {e}")
        # Continue with the script even if drop fails

# Verify the schema after type conversion
print(f"Final schema before table creation: {iceberg_schema}")

# Check if table exists
table_exists = False
try:
    catalog.load_table((namespace, table_name))
    table_exists = True
    print(f"Table {namespace}.{table_name} already exists")
except NoSuchTableError:
    table_exists = False
    print(f"Table {namespace}.{table_name} doesn't exist, will create new")

# If table exists and we couldn't drop it, try to create a new table with a different name
if table_exists:
    if args.drop:
        print(f"Table {namespace}.{table_name} already exists in the catalog, dropping it")
        catalog.drop_table((namespace, table_name))
    else:
        raise ValueError(f"Table {namespace}.{table_name} already exists, skipping creation")

# Create the table
print(f"Creating Iceberg table in {args.platform}: {namespace}.{table_name}")

if args.format == "long":
    # For long format, create partitioning and sorting optimized for metric-based queries
    print("Creating table optimized for long format data with partitioning and sorting")

    creation_date_id = None
    sample_id = None
    metric_id = None

    for field in iceberg_schema.fields:
        if field.name == "creation_date":
            creation_date_id = field.field_id
        elif field.name == "sample":
            sample_id = field.field_id
        elif field.name == "metric":
            metric_id = field.field_id

    catalog.load_namespace_properties(namespace)
    print(f"Database {namespace} already exists")

    # Create partition spec - partition by month of creation_date
    # This helps with time-based queries like "past 3 months"
    partition_spec = None
    if creation_date_id is not None:
        print("Creating partition spec by month of creation_date")
        partition_spec = PartitionSpec(
            PartitionField(source_id=creation_date_id, field_id=1000, transform=MonthTransform(), name="month"),
        )
    else:
        print("creation_date field not found, using empty partition spec")
        partition_spec = PartitionSpec()

    # Create sort order - sort by sample and metric
    # This helps with queries that filter on specific samples and metrics
    sort_order_arg = None
    if sample_id is not None and metric_id is not None:
        print("Creating sort order by sample and metric")
        sort_order = SortOrder(
            SortField(
                source_id=sample_id,
                transform=TruncateTransform(10),
                direction=SortDirection.ASC,
                null_order=NullOrder.NULLS_LAST,
            ),
            SortField(
                source_id=metric_id,
                transform=TruncateTransform(10),
                direction=SortDirection.ASC,
                null_order=NullOrder.NULLS_LAST,
            ),
        )
        sort_order_arg = sort_order

    # Create properties for external table
    properties = {"format": "parquet", "external": "true"}

    # Create table arguments
    create_table_args = {
        "identifier": f"{namespace}.{table_name}",
        "schema": iceberg_schema,
        "location": f"s3://{BUCKET_NAME}/{s3_prefix}",
        "partition_spec": partition_spec,
        "properties": properties,
    }

    # Add sort_order only if it's defined
    if sort_order_arg is not None:
        create_table_args["sort_order"] = sort_order_arg

    # Create the table from the Parquet file
    iceberg_table = catalog.create_table(**create_table_args)
else:
    iceberg_table = catalog.create_table(
        identifier=(namespace, table_name),
        schema=iceberg_schema,
        location=f"{S3_WAREHOUSE_LOCATION}{namespace}/{table_name}",
    )
print(f"Successfully created table {namespace}.{table_name}")
print(f"Populating the table with data from {len(files)} files using chunks of {args.files_per_chunk}...")

# Filter reallife data to only include type=table_row and remove columns with all null values
if is_reallife:
    print("Real-life data detected. Filtering rows with type=table_row and removing columns with all null values")

    # Read in polars DataFrame
    df = pl.read_parquet(f"s3://{parquet_path}")
    print(f"Original DataFrame has {len(df)} rows and {len(df.columns)} columns")

    # Filter rows where type = table_row
    df = df.filter(pl.col("type") == "table_row")
    # Remove columns with all null values
    df = df.select([col for col in df.columns if not df[col].is_null().all()])

    # Persist the filtered data to a temporary Parquet file in S3 and register it with the Iceberg table
    filtered_path = f"s3://{BUCKET_NAME}/{s3_prefix}filtered.parquet"
    print(f"Writing filtered data {filtered_path}")
    df.write_parquet(filtered_path)

    # Register the new Parquet file with the Iceberg table
    iceberg_table.add_files([filtered_path])

else:
    # ---------------------------------------------------------------------------
    # Simulated data has a lot of outputs.
    # Register parquet files with the Iceberg table in chunks so we can resume
    # from a given chunk when the operation is interrupted.
    # ---------------------------------------------------------------------------
    FILES_PER_CHUNK = max(1, args.files_per_chunk)
    TOTAL_CHUNKS = (len(files) + FILES_PER_CHUNK - 1) // FILES_PER_CHUNK
    START_CHUNK = max(1, args.start_from)
    MAX_RETRIES = 5  # retry network-related failures when registering a chunk

    for chunk_num in range(1, TOTAL_CHUNKS + 1):
        start_idx = (chunk_num - 1) * FILES_PER_CHUNK
        end_idx = min(start_idx + FILES_PER_CHUNK, len(files))
        chunk_files = files[start_idx:end_idx]

        # Skip chunks that have already been processed when resuming
        if chunk_num < START_CHUNK:
            print(f"Skipping chunk {chunk_num}/{TOTAL_CHUNKS} (already processed) — {len(chunk_files)} files")
            continue

        print(f"Adding chunk {chunk_num}/{TOTAL_CHUNKS}: {len(chunk_files)} files (indexes {start_idx}–{end_idx - 1})")

        for attempt in range(MAX_RETRIES):
            try:
                iceberg_table.add_files([f"s3://{p}" for p in chunk_files])
                print(f"Successfully added chunk {chunk_num}/{TOTAL_CHUNKS}")
                break
            except (OSError, ClientError) as e:
                if attempt < MAX_RETRIES - 1:
                    wait_time = 2**attempt  # exponential back-off
                    print(
                        f"Error while adding chunk {chunk_num} (attempt {attempt + 1}/{MAX_RETRIES}): {e}. Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    print(
                        f"Failed to add chunk {chunk_num} after {MAX_RETRIES} attempts. "
                        f"You can resume with --start-from {chunk_num} once the issue is resolved."
                    )
                    raise

# Final verification
try:
    d = iceberg_table.scan().to_arrow()
    print(f"Loaded table {namespace}.{table_name}. Rows: {d.num_rows}, cols: {d.num_columns}")
except Exception as e:
    print(f"Table was created but verification failed: {str(e)}")
