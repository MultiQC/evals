"""
This script is used to setup an Iceberg table for the MultiQC data in either AWS Glue or Cloudflare.

It will create the table and load the data from the parquet files already stored in S3 or R2.

The data is assumed to be already generated and uploaded e.g. by gen-simulated.py
"""

import argparse
import datetime
import os

import boto3
import pyarrow.dataset as ds
import pyarrow.fs as fs
from dotenv import load_dotenv
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError

# Load environment variables
load_dotenv()

# Parse command line arguments
parser = argparse.ArgumentParser(description="Setup Iceberg table for MultiQC data.")
parser.add_argument(
    "--dataset",
    choices=["reallife", "simulated"],
    default="simulated",
    help="Choose between reallife or simulated dataset (default: simulated)",
)
parser.add_argument(
    "--experiment",
    type=str,
    default="",
    help="Name of the experiment (default: '')",
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
args = parser.parse_args()


# Platform-specific configuration
if args.platform == "cloudflare":
    # Cloudflare R2 configuration
    TOKEN = os.environ["CLOUDFLARE_TOKEN"]
    ACCESS_KEY_ID = os.environ["CLOUDFLARE_ACCESS_KEY_ID"]
    SECRET_ACCESS_KEY = os.environ["CLOUDFLARE_SECRET_ACCESS_KEY"]
    WAREHOUSE = "8234d07c9c28a6f6c380fe45731ba8e4_megaqc-test"
    CATALOG_URI = "https://catalog.cloudflarestorage.com/8234d07c9c28a6f6c380fe45731ba8e4/megaqc-test"
    R2_ENDPOINT_URL = "https://8234d07c9c28a6f6c380fe45731ba8e4.r2.cloudflarestorage.com"
    CATALOG_NAME = "megaqc-test"
    NAMESPACE = args.experiment
    TABLE_NAME = "multiqc"
    BUCKET_NAME = "megaqc-test"
    s3_prefix = f"{args.experiment}/parquet/"
    region = "auto"
    DATABASE_NAME = NAMESPACE
    S3_WAREHOUSE_LOCATION = None
else:
    # AWS Glue configuration
    ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
    SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
    region = os.environ["AWS_REGION"]
    DATABASE_NAME = "megaqc-test"
    NAMESPACE = args.experiment
    TABLE_NAME = "multiqc"
    BUCKET_NAME = "vlad-megaqc"
    s3_prefix = f"{args.dataset}/{args.experiment}/"
    S3_WAREHOUSE_LOCATION = f"s3://{BUCKET_NAME}/warehouse/{args.dataset}/{args.experiment}/"

# Connect to the appropriate catalog
if args.platform == "cloudflare":
    # Connect to Cloudflare R2 Data Catalog
    catalog = RestCatalog(
        name=CATALOG_NAME,
        token=TOKEN,
        warehouse=WAREHOUSE,
        uri=CATALOG_URI,
    )
else:
    # Connect to AWS Glue Catalog
    catalog = GlueCatalog(
        name="glue",
        boto3_session=boto3.Session(
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=SECRET_ACCESS_KEY,
            region_name=region,
        ),
        warehouse=S3_WAREHOUSE_LOCATION,
    )

# Verify auth is working by trying to list namespaces
try:
    namespaces = catalog.list_namespaces()
    print(f"Successfully authenticated with {args.platform.upper()}. Available namespaces: {namespaces}")
except Exception as e:
    print(f"Authentication error: {e}")
    raise

# Create namespace if it doesn't exist
if args.platform == "cloudflare":
    catalog.create_namespace_if_not_exists(NAMESPACE)
    print(f"Ensured namespace exists: {NAMESPACE}")
else:
    try:
        catalog.create_namespace(DATABASE_NAME)
        print(f"Created namespace: {DATABASE_NAME}")
    except Exception as e:
        print(f"Namespace already exists or error creating: {e}")

# Check if table exists and delete it if requested
if args.drop:
    try:
        print(f"Dropping existing table: {DATABASE_NAME}.{TABLE_NAME}")
        catalog.drop_table((DATABASE_NAME, TABLE_NAME))
        print(f"Table {DATABASE_NAME}.{TABLE_NAME} dropped successfully")
    except NoSuchTableError:
        print(f"Table {DATABASE_NAME}.{TABLE_NAME} does not exist, nothing to drop")
    except Exception as e:
        print(f"Error dropping table: {e}")
        # Continue with the script even if drop fails

# Create appropriate filesystem for PyArrow to access storage
if args.platform == "cloudflare":
    s3_fs = fs.S3FileSystem(
        endpoint_override=R2_ENDPOINT_URL,
        access_key=ACCESS_KEY_ID,
        secret_key=SECRET_ACCESS_KEY,
        region=region,
    )
    storage_path = f"{BUCKET_NAME}/{s3_prefix}"
else:
    s3_fs = fs.S3FileSystem(
        access_key=ACCESS_KEY_ID,
        secret_key=SECRET_ACCESS_KEY,
        region=region,
    )
    storage_path = f"{BUCKET_NAME}/{s3_prefix}"

# Load from bucket using PyArrow
print(f"Creating PyArrow dataset from {args.platform} storage at {storage_path}")

# Create dataset from S3/R2 bucket
dataset = ds.dataset(storage_path, format="parquet", filesystem=s3_fs)

# Convert to PyArrow table
pa_table = dataset.to_table()
print(f"Loaded PyArrow table with schema: {pa_table.schema}")

# Check if table exists
table_exists = False
try:
    catalog.load_table((DATABASE_NAME, TABLE_NAME))
    table_exists = True
    print(f"Table {DATABASE_NAME}.{TABLE_NAME} already exists")
except NoSuchTableError:
    table_exists = False
    print(f"Table {DATABASE_NAME}.{TABLE_NAME} doesn't exist, will create new")

# If table exists and we couldn't drop it, try to create a new table with a different name
if table_exists and args.drop:
    # Generate a unique name with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    new_table_name = f"{TABLE_NAME}_{timestamp}"
    print(f"Creating new table with unique name: {new_table_name}")
    TABLE_NAME = new_table_name

# Create the table
print(f"Creating Iceberg table in {args.platform}: {DATABASE_NAME}.{TABLE_NAME}")
try:
    # Create table with platform-specific parameters
    if args.platform == "cloudflare":
        iceberg_table = catalog.create_table(
            identifier=(DATABASE_NAME, TABLE_NAME),
            schema=pa_table.schema,
        )
    else:
        iceberg_table = catalog.create_table(
            identifier=(DATABASE_NAME, TABLE_NAME),
            schema=pa_table.schema,
            location=f"{S3_WAREHOUSE_LOCATION}{NAMESPACE}/{TABLE_NAME}",
        )
    print(f"Successfully created table {DATABASE_NAME}.{TABLE_NAME}")
except Exception as e:
    print(f"Error creating table: {e}")
    # Try fallback approach
    print("Trying alternate approach with create_table_if_not_exists...")
    if args.platform == "cloudflare":
        iceberg_table = catalog.create_table_if_not_exists(
            identifier=(DATABASE_NAME, TABLE_NAME),
            schema=pa_table.schema,
        )
    else:
        iceberg_table = catalog.create_table_if_not_exists(
            identifier=(DATABASE_NAME, TABLE_NAME),
            schema=pa_table.schema,
            location=f"{S3_WAREHOUSE_LOCATION}{NAMESPACE}/{TABLE_NAME}",
        )

# Load the table and append data
iceberg_table = catalog.load_table((DATABASE_NAME, TABLE_NAME))
print(f"Appending {pa_table.num_rows} rows to Iceberg table")
iceberg_table.append(pa_table)

print(f"Successfully loaded data into Iceberg table on {args.platform}: {DATABASE_NAME}.{TABLE_NAME}")
print(iceberg_table.scan().to_arrow().to_pandas().head())
