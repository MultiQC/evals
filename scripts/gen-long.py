import argparse
import json
import multiprocessing as mp
import os
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

import numpy as np
import polars as pl
from cloudpathlib import S3Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DATASET_NAME = "simulated"
PARQUET_BUCKET = S3Path(f"s3://vlad-megaqc-demo/{DATASET_NAME}/")

# Cloud-specific configurations
CHECKPOINT_FILE = "generation_checkpoint.json"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


def load_checkpoint() -> Dict[str, Any]:
    """Load generation checkpoint if it exists"""
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"last_completed_run": 0, "failed_runs": []}


def save_checkpoint(checkpoint: Dict[str, Any]):
    """Save generation checkpoint"""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f)


parser = argparse.ArgumentParser()
parser.add_argument("--num-runs", type=int, default=10)
parser.add_argument("--num-modules", type=int, default=10)
parser.add_argument("--num-sections", type=int, default=5)
parser.add_argument("--num-samples-per-module", type=int, default=10)
parser.add_argument("--num-metrics-per-module", type=int, default=50)
parser.add_argument("--start-from", type=int, default=1, help="Start processing from this run number")
parser.add_argument("--end-at", type=int, help="End processing at this run number (inclusive)")
parser.add_argument("--workers", type=int, default=min(4, mp.cpu_count()), help="Number of parallel workers")
args = parser.parse_args()

# Parquet setup
EXPERIMENT_NAME = f"long_{args.num_runs}runs"
out_path = PARQUET_BUCKET
target_path = out_path / EXPERIMENT_NAME


def generate_random_string(length=10):
    """Generate a random string of fixed length"""
    return "".join(random.choices(string.ascii_letters, k=length))


def generate_metric_metadata():
    """Generate metadata for a metric"""
    return {
        "min": random.uniform(0, 10),
        "max": random.uniform(90, 100),
        "dmin": random.uniform(0, 3),
        "dmax": random.uniform(9, 10),
        "scale": random.choice(["Set2", "Accent", "Set1", "Set3", "Dark2", "Paired", "Pastel2", "Pastel1"]),
        "color": f"#{random.randint(0, 0xFFFFFF):06x}",
        "type": random.choice(["numeric", "categorical", "percentage"]),
        "namespace": generate_random_string(10),
        "placement": random.randint(0, 1000),
        "shared_key": random.choice(["read_count", "base_count"]),
    }


def generate_single_run(run_number):
    """Generate data for a single run - optimized for single-pass generation"""
    for attempt in range(MAX_RETRIES):
        try:
            print(f"Starting generation for run {run_number} (attempt {attempt + 1}/{MAX_RETRIES})")

            # Generate date as datetime object
            creation_date = datetime.now() - timedelta(days=random.randint(0, 100))
            run_id = f"run_{run_number}"

            # Pre-generate all metrics metadata to avoid repeated generation
            metrics_metadata = {}
            for metric_number in range(args.num_metrics_per_module):
                metrics_metadata[f"metric_{metric_number}"] = generate_metric_metadata()

            # Calculate total number of rows
            total_rows = args.num_modules * args.num_samples_per_module * args.num_metrics_per_module
            print(f"  Run {run_number}: Generating {total_rows:,} rows in one pass")

            # Generate all data for this run at once
            rows = []
            for module_number in range(args.num_modules):
                for sample_number in range(args.num_samples_per_module):
                    for metric_number in range(args.num_metrics_per_module):
                        mm = metrics_metadata[f"metric_{metric_number}"]
                        center = random.uniform(mm["min"], mm["max"])
                        value = random.gauss(mu=center, sigma=16.67)
                        value = min(max(value, mm["min"]), mm["max"])

                        row = {
                            "run_id": run_id,
                            "creation_date": creation_date,
                            "module_name": f"module_{module_number}",
                            "sample_name": f"sample_{sample_number}",
                            "metric_name": f"metric_{metric_number}",
                            "metric_min": np.float32(mm["min"]),
                            "metric_max": np.float32(mm["max"]),
                            "metric_dmin": np.float32(mm["dmin"]),
                            "metric_dmax": np.float32(mm["dmax"]),
                            "metric_scale": mm["scale"],
                            "metric_color": mm["color"],
                            "val_raw": np.float32(value),
                            "val_raw_type": "float",
                            # Add derived columns for better query performance
                            "module_id": np.int16(module_number),
                            "sample_id": np.int16(sample_number),
                            "metric_id": np.int16(metric_number),
                            "date_partition": creation_date.strftime("%Y-%m-%d"),
                            "hour_partition": creation_date.hour,
                        }
                        rows.append(row)

            print(f"  Run {run_number}: Generated {len(rows):,} rows, creating DataFrame")

            # Create DataFrame from all rows at once
            df = pl.DataFrame(rows)

            # Optimize data types
            df = df.with_columns(
                [
                    pl.col("creation_date").cast(pl.Datetime("us")),
                    pl.col("metric_scale").cast(pl.Utf8),
                    pl.col("val_raw_type").cast(pl.Utf8),
                    pl.col("date_partition").cast(pl.Utf8),
                ]
            )

            # Sort for better compression and query performance
            df = df.sort(["creation_date", "module_id", "sample_id", "metric_id"])

            print(f"  Run {run_number}: DataFrame created and sorted, writing to file")

            # Create date-partitioned structure for Iceberg
            date_str = creation_date.strftime("%Y-%m-%d")
            output_dir = target_path / f"date_partition={date_str}"
            output_dir.mkdir(parents=True, exist_ok=True)
            out_file = output_dir / f"run_{run_number}.parquet"

            # Write to parquet with optimized settings
            df.write_parquet(
                str(out_file),
                compression="snappy",
                row_group_size=50000,
                use_pyarrow=True,
            )

            # Calculate file size for reporting
            file_size_mb = out_file.stat().st_size / (1024 * 1024)

            print(f"✓ Completed run {run_number}: {len(df):,} rows, {file_size_mb:.1f}MB -> {out_file}")

            # Clean up memory explicitly
            del rows
            del df

            return {"file": str(out_file), "rows": total_rows, "size_mb": file_size_mb, "date": date_str}
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"Attempt {attempt + 1} failed for run {run_number}: {str(e)}")
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                raise Exception(f"Failed to generate run {run_number} after {MAX_RETRIES} attempts: {str(e)}")


def generate_long_format_files():
    """Generate long format files with run-level parallelization"""
    target_path.mkdir(parents=True, exist_ok=True)

    # Load checkpoint
    checkpoint = load_checkpoint()
    last_completed = checkpoint["last_completed_run"]
    failed_runs = checkpoint["failed_runs"]

    # Determine which runs to generate
    end_run = args.end_at if args.end_at is not None else args.num_runs
    runs_to_generate = [
        run_number
        for run_number in range(1, end_run + 1)
        if run_number > last_completed and run_number >= args.start_from
    ]

    # Add previously failed runs
    runs_to_generate.extend([r for r in failed_runs if r >= args.start_from])
    runs_to_generate = sorted(list(set(runs_to_generate)))

    if not runs_to_generate:
        print("No runs to generate based on checkpoint and --start-from parameter")
        return

    total_rows_per_run = args.num_modules * args.num_samples_per_module * args.num_metrics_per_module

    print(f"🚀 Generating {len(runs_to_generate)} runs using {args.workers} workers")
    print(f"   Rows per run: {total_rows_per_run:,}")
    print(f"   Total estimated rows: {len(runs_to_generate) * total_rows_per_run:,}")
    print("   Parallelization: Each worker generates complete runs")
    print(f"   Checkpoint: Last completed run {last_completed}")
    print(f"   Failed runs to retry: {len(failed_runs)}")

    # Use ThreadPoolExecutor to parallelize across runs
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_run = {
            executor.submit(generate_single_run, run_number): run_number for run_number in runs_to_generate
        }

        completed_results = []
        failed_runs = []

        for future in as_completed(future_to_run):
            run_number = future_to_run[future]
            try:
                result = future.result()
                completed_results.append(result)
                checkpoint["last_completed_run"] = max(checkpoint["last_completed_run"], run_number)
                if run_number in checkpoint["failed_runs"]:
                    checkpoint["failed_runs"].remove(run_number)
                save_checkpoint(checkpoint)
                print(f"🎉 Worker completed run {run_number}")
            except Exception as exc:
                print(f"❌ Run {run_number} failed: {exc}")
                failed_runs.append(run_number)
                checkpoint["failed_runs"] = failed_runs
                save_checkpoint(checkpoint)

    # Print comprehensive summary
    if completed_results:
        total_rows = sum(r["rows"] for r in completed_results)
        total_size_mb = sum(r["size_mb"] for r in completed_results)
        unique_dates = len(set(r["date"] for r in completed_results))

        print("\n📊 Generation Summary:")
        print(f"   ✅ Files created: {len(completed_results)}")
        print(f"   📈 Total rows: {total_rows:,}")
        print(f"   💾 Total size: {total_size_mb:.1f} MB ({total_size_mb/1024:.2f} GB)")
        print(f"   📅 Unique dates: {unique_dates}")
        print(f"   📊 Avg rows/file: {total_rows/len(completed_results):,.0f}")
        print(f"   📦 Avg size/file: {total_size_mb/len(completed_results):.1f} MB")
        print(f"   ❌ Failed runs: {len(failed_runs)}")

        if failed_runs:
            print(f"   🔄 Failed run numbers: {failed_runs}")

        # Sample a file for schema verification
        sample_result = completed_results[0]
        sample_df = pl.read_parquet(sample_result["file"])

        print("\n🔍 Schema Preview:")
        for name, dtype in sample_df.schema.items():
            print(f"   {name}: {dtype}")

        print("\n🎯 Optimization Benefits:")
        print("   - Single-pass generation per run (no batching overhead)")
        print(f"   - Parallel processing across {args.workers} workers")
        print("   - Memory efficient (cleanup after each run)")
        print("   - Date-partitioned for Iceberg optimization")


if __name__ == "__main__":
    generate_long_format_files()
