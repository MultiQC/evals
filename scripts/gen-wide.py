import argparse
import json
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import numpy as np
from cloudpathlib import S3Path
from dotenv import load_dotenv

DATASET_NAME = "simulated"

PARQUET_BUCKET = S3Path(f"s3://vlad-megaqc/{DATASET_NAME}/")
LOCAL_DIR = Path(f"./data/{DATASET_NAME}/")

parser = argparse.ArgumentParser()
parser.add_argument("--num_runs", type=int, default=10)
parser.add_argument("--num_modules", type=int, default=10)
parser.add_argument("--num_sections", type=int, default=5)
parser.add_argument("--num_samples_per_module", type=int, default=10)
parser.add_argument("--num_metrics_per_module", type=int, default=50)
parser.add_argument("--upload", action="store_true")
parser.add_argument("--start-from", type=int, default=1, help="Start processing from this run number")
args = parser.parse_args()

# Parquet setup
EXPERIMENT_NAME = f"wide_{args.num_runs}runs_{args.num_modules}mod_{args.num_sections}sec_{args.num_samples_per_module}samples_{args.num_metrics_per_module}metrics"
out_path = PARQUET_BUCKET if args.upload else LOCAL_DIR
target_path = out_path / EXPERIMENT_NAME


def generate_random_string(length=10):
    """Generate a random string of fixed length"""
    return "".join(random.choices(string.ascii_letters, k=length))


def generate_random_plot_json():
    """Generate a random plot JSON structure."""
    plot_types = ["bar plot", "x/y line", "violin plot", "heatmap", "scatter plot", "box plot"]

    # Create a base structure
    plot_data = {
        "anchor": f"{generate_random_string(10)}_plot",
        "plot_type": random.choice(plot_types),
        "pconfig": {
            "id": f"{generate_random_string(8)}",
            "title": f"Module: {generate_random_string(15)}",
            "anchor": f"{generate_random_string(10)}_plot",
            "square": random.choice([True, False]),
            "logswitch": random.choice([True, False]),
            "logswitch_active": random.choice([True, False]),
            "logswitch_label": "Log10",
            "cpswitch": random.choice([True, False]),
            "cpswitch_c_active": random.choice([True, False]),
            "cpswitch_counts_label": "Counts",
        },
        "datasets": [
            {
                "name": f"Sample_{generate_random_string(8)}",
                "data": {f"point_{i}": random.uniform(0, 100) for i in range(random.randint(10, 30))},
            }
            for _ in range(random.randint(3, 8))
        ],
        "categories": [f"category_{generate_random_string(5)}" for _ in range(random.randint(5, 15))],
        "extra_data": {
            f"param_{generate_random_string(8)}": generate_random_string(15) for _ in range(random.randint(3, 10))
        },
    }

    return json.dumps(plot_data)


def generate_wide_format_parquet(num_runs, num_plots, num_samples, num_metrics, output_dir):
    """Generate a wide format parquet file with plot_input and table_row rows.

    Args:
        num_runs: Number of runs to generate
        num_plots: Number of unique plot types to generate
        num_samples: Number of sample rows per plot type
        num_metrics: Number of metric columns to generate
        output_dir: Directory to save the parquet files

    Returns:
        list: List of generated file paths
    """
    # Define column structure
    metric_columns = [f"metric_{i}" for i in range(num_metrics)]
    plot_anchors = [f"plot_{i}" for i in range(num_plots)]
    sample_names = [f"sample_{i}" for i in range(num_samples)]

    generated_files = []

    for run_id in range(1, num_runs + 1):
        # Skip runs before the start-from value
        if run_id < args.start_from:
            continue

        # Write to parquet immediately to save memory
        output_file = output_dir / f"run_{run_id}.parquet"
        should_generate = True

        if output_file.exists():
            # Verify the file can be read properly
            try:
                # Attempt to read the parquet file
                _ = pl.read_parquet(str(output_file))
                # If successful, skip generation
                print(f"Skipping run {run_id} because it already exists and can be read properly")
                should_generate = False
            except Exception as e:
                print(f"Found corrupted parquet file for run {run_id}, regenerating: {str(e)}")
                # Will proceed to regenerate the file

        if not should_generate:
            continue

        # Generate current timestamp
        current_date = datetime.now()

        plot_input_rows = []

        # Create table_row rows
        table_rows = []
        for s_name in sample_names:
            # Generate metric values
            row_data = {
                "run_id": run_id,
                "creation_date": current_date,
                "type": "table_row",
                "sample_name": s_name,
            }

            # Add metric columns with both val and str variants
            for metric in metric_columns:
                # Generate value for val column
                value = random.uniform(0, 5)
                row_data[metric] = np.float32(value)  # Use float32 for memory efficiency

            table_rows.append(row_data)

        # Combine plot_input and table_row rows for this run
        run_rows = plot_input_rows + table_rows

        # Convert to DataFrame using polars
        df = pl.DataFrame(run_rows)
        
        # Optimize data types
        df = df.with_columns([
            pl.col("creation_date").cast(pl.Datetime("us")),
            pl.col("type").cast(pl.Utf8),
            pl.col("sample_name").cast(pl.Utf8)
        ])

        # Write to parquet immediately to save memory
        df.write_parquet(
            str(output_file),
            compression="snappy",
            row_group_size=50000,
            use_pyarrow=True
        )
        generated_files.append(str(output_file))

        print(f"Generated and saved run {run_id} to {output_file}")

        # Free memory (polars handles this automatically, but explicit is good)
        del df
        del run_rows
        del plot_input_rows
        del table_rows

    return generated_files


def generate_wide_format_files(
    num_runs=args.num_runs,
    num_modules=args.num_modules,
    num_sections=args.num_sections,
    num_samples_per_module=args.num_samples_per_module,
    num_metrics_per_module=args.num_metrics_per_module,
):
    num_plots = num_sections * num_modules
    num_samples = num_samples_per_module
    num_metrics = num_modules * num_metrics_per_module

    """Generate and save wide format parquet files."""
    print(
        f"Generating wide format parquet files. Num runs: {num_runs}, num modules: {num_modules}, num sections: {num_sections}, num plots: {num_plots}, num samples: {num_samples}, num metrics: {num_metrics}"
    )

    # Generate data and write files immediately
    generated_files = generate_wide_format_parquet(num_runs, num_plots, num_samples, num_metrics, target_path)

    print(f"Generated {len(generated_files)} wide format parquet files")
    
    # Print summary statistics using polars
    if generated_files:
        sample_df = pl.read_parquet(generated_files[0])
        row_count, col_count = sample_df.shape
        estimated_memory_mb = sample_df.estimated_size("mb")
        
        print(f"Sample data shape: ({row_count:,}, {col_count})")
        print(f"Memory usage per file: ~{estimated_memory_mb:.1f} MB")
        print(f"Total estimated data size: ~{len(generated_files) * estimated_memory_mb / 1024:.2f} GB")


if __name__ == "__main__":
    generate_wide_format_files() 