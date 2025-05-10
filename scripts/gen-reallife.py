import os
import subprocess
from pathlib import Path
import sys
import dotenv
import argparse

import boto3

dotenv.load_dotenv()

parser = argparse.ArgumentParser()
parser.add_argument("--rerun", action="store_true", help="Rerun MultiQC if the parquet file already exists")
args = parser.parse_args()

DATASET_NAME = "reallife"
S3_BUCKET = "vlad-megaqc"
PROJECTS_DIR = Path("/Users/vlad/Seqera/multiqc_heavy_examples")

# List of projects to process
projects = ["Xing2020", "BSstuff", "Petropoulus_2016"]

# Run multiqc on each project
output_dir = "parquet_runs"
for project in projects:
    parquet_file = Path(f"{PROJECTS_DIR}/{output_dir}/{project}_multiqc_report_data/multiqc.parquet")
    if args.rerun and not parquet_file.exists():
        print(f"File {parquet_file} does not exist, running MultiQC")
        cmd = f"multiqc -fv {project}/ -o {output_dir} --title {project}"
        print(f"Running: {cmd}")
        subprocess.run(cmd, shell=True, check=True)
        print(f"Completed MultiQC for {project}")
    else:
        print(f"Skipping {project} because it already exists")

# List of expected parquet files
s3_client = boto3.client("s3")

for project in projects:
    parquet_file = Path(f"{PROJECTS_DIR}/{output_dir}/{project}_multiqc_report_data/multiqc.parquet")

    if parquet_file.exists():
        # Upload to S3 with the same path structure
        s3_key = f"{DATASET_NAME}/{project}/multiqc.parquet"

        print(f"Uploading {parquet_file} to s3://{S3_BUCKET}/{s3_key}")
        s3_client.upload_file(str(parquet_file), S3_BUCKET, s3_key)
        print(f"Upload completed for {parquet_file}")
    else:
        print(f"Warning: Parquet file not found: {parquet_file}")
