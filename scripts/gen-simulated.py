import argparse
import json
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from cloudpathlib import S3Path
from dotenv import load_dotenv

DATASET_NAME = "simulated"


PARQUET_BUCKET = S3Path(f"s3://vlad-megaqc/{DATASET_NAME}/")
LOCAL_DIR = Path(f"./data/{DATASET_NAME}/")


parser = argparse.ArgumentParser()
parser.add_argument("--format", type=str, default="wide", choices=["wide", "long"])
parser.add_argument("--num_runs", type=int, default=10)
parser.add_argument("--num_modules", type=int, default=10)
parser.add_argument("--num_sections", type=int, default=5)
parser.add_argument("--num_samples_per_module", type=int, default=10)
parser.add_argument("--num_metrics_per_module", type=int, default=50)
parser.add_argument("--upload", action="store_true")
parser.add_argument("--start-from", type=int, default=1, help="Start processing from this run number")
args = parser.parse_args()

# Parquet setup
EXPERIMENT_NAME = f"{args.format}_{args.num_runs}runs_{args.num_modules}mod_{args.num_sections}sec_{args.num_samples_per_module}samples_{args.num_metrics_per_module}metrics"
out_path = PARQUET_BUCKET if args.upload else LOCAL_DIR
target_path = out_path / EXPERIMENT_NAME


# Generate random sample names
def generate_sample_names(num_samples):
    return [f"sample_{i:03d}" for i in range(1, num_samples + 1)]


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


# Generate random module names
def generate_module_names(num_modules):
    prefixes = ["fastqc", "picard", "samtools", "bcftools", "gatk", "star", "kallisto", "salmon", "kraken", "quast"]
    return [f"{random.choice(prefixes)}_{i:02d}" for i in range(1, num_modules + 1)]


def generate_value_metadata(value):
    """Generate metadata for a value"""
    return {
        "val_raw": value,
        "val_raw_type": random.choice(["int", "float", "str"]),
        "val_mod": value,
        "val_mod_type": random.choice(["int", "float", "str"]),
        "val_fmt": f"{value:.2f}" if isinstance(value, float) else str(value),
    }


# Generate random metric names for a module
def generate_metric_names(module_name, num_metrics):
    metric_types = ["quality", "coverage", "count", "percentage", "score", "length", "gc", "reads", "mapped", "error"]
    return [f"{module_name}_{random.choice(metric_types)}_{i:02d}" for i in range(1, num_metrics + 1)]


def generate_sample_data(sample_index: int, metric_metadatas: dict[str, dict]):
    """Generate data for a single sample"""
    metrics = {}

    for mn, mm in metric_metadatas.items():
        min_val = mm["min"]
        max_val = mm["max"]
        value = random.gauss(mu=50, sigma=16.67)  # sigma chosen so ~99.7% of values fall within 0-100
        value = min(max(value, min_val), max_val)  # Clamp value between min and max
        metrics[mn] = (generate_value_metadata(value),)

    return {"sample_id": f"sample_{sample_index}", "metrics": metrics}


def generate_module_data(module_index, num_samples, num_metrics, num_sections):
    """Generate data for a single module"""
    metrics_metadata = {}
    for i in range(num_metrics):
        metric_name = f"metric_{i}"
        metrics_metadata[metric_name] = generate_metric_metadata()

    samples = [generate_sample_data(sample_i, metrics_metadata) for sample_i in range(num_samples)]
    sections = [generate_section_data() for _ in range(num_sections)]

    return {
        "module_id": f"module_{module_index}",
        "name": f"Module {module_index}",
        "url": f"http://example.com/module/{module_index}",
        "comment": f"This is module {module_index}",
        "metrics_metadata": metrics_metadata,
        "samples": samples,
        "sections": sections,
        "anchor": f"anchor_{module_index}",
        "doi": generate_random_string(20),
    }


def generate_section_data():
    """Generate data for a section"""
    section_id = generate_random_string()
    anchor = f"section_{section_id}"

    return {
        "name": f"Section {section_id}",
        "anchor": anchor,
        "id": section_id,
        "description": f"Description for section {section_id}",
        "module": f"module_{random.randint(0, 100)}",
        "module_anchor": f"module_anchor_{random.randint(0, 100)}",
        "module_info": f"Info about module for section {section_id}",
        "comment": f"Comment for section {section_id}",
        "helptext": f"Help text for section {section_id}",
        "content_before_plot": f"Content before plot for section {section_id}",
        "content": f"Main content for section {section_id}",
        "plot": f"Plot content for section {section_id}",
        "print_section": random.choice([True, False]),
        "plot_anchor": f"plot_{section_id}" if random.choice([True, False]) else None,
        "ai_summary": f"AI generated summary for section {section_id}",
    }


def generate_run_data(run_index, num_modules, num_samples_per_module, num_metrics_per_module, num_sections_per_module):
    """Generate data for a single run"""
    modules = [
        generate_module_data(i, num_samples_per_module, num_metrics_per_module, num_sections_per_module)
        for i in range(num_modules)
    ]

    return {
        "run_id": f"run_{run_index}",
        "timestamp": (datetime.now() - timedelta(days=random.randint(0, 100))).isoformat(),
        "modules": modules,
    }


def flatten_hierarchical_data(run_data):
    """Convert hierarchical data to flat format for Parquet

    Args:
        data: Nested JSON-like data with runs, modules, samples, and metrics

    Returns:
        pd.DataFrame: Flattened dataframe with one row per metric value
    """
    flat_records = []

    run_id = run_data["run_id"]
    creation_date = run_data["creation_date"]
    for data in run_data["modules"]:
        module_id = data["module_id"]
        module_name = data["name"]
        module_url = data["url"]
        module_comment = data["comment"]
        module_anchor = data.get("anchor", "")
        module_doi = data.get("doi", "")

        # Extract metrics metadata for later use
        metrics_metadata = data["metrics_metadata"]

        # Process samples data
        for sample in data["samples"]:
            sample_id = sample["sample_id"]

            for metric_name, metric_data in sample["metrics"].items():
                # The metric_data is a tuple with one element in your generator
                metric_values = metric_data[0] if isinstance(metric_data, tuple) else metric_data

                flat_records.append(
                    {
                        # Run information
                        "run_id": run_id,
                        "creation_date": creation_date,
                        # Module information
                        "module_id": module_id,
                        "module_name": module_name,
                        "module_url": module_url,
                        "module_comment": module_comment,
                        "module_anchor": module_anchor,
                        "module_doi": module_doi,
                        # Sample information
                        "sample_id": sample_id,
                        # Metric information
                        "metric_name": metric_name,
                        # Value metadata
                        "val_raw": metric_values.get("val_raw"),
                        "val_raw_type": metric_values.get("val_raw_type"),
                        "val_mod": metric_values.get("val_mod"),
                        "val_mod_type": metric_values.get("val_mod_type"),
                        "val_fmt": metric_values.get("val_fmt"),
                        # Metric metadata (from module)
                        "metric_min": metrics_metadata.get(metric_name, {}).get("min"),
                        "metric_max": metrics_metadata.get(metric_name, {}).get("max"),
                        "metric_dmin": metrics_metadata.get(metric_name, {}).get("dmin"),
                        "metric_dmax": metrics_metadata.get(metric_name, {}).get("dmax"),
                        "metric_scale": metrics_metadata.get(metric_name, {}).get("scale"),
                        "metric_color": metrics_metadata.get(metric_name, {}).get("color"),
                        "metric_type": metrics_metadata.get(metric_name, {}).get("type"),
                        "metric_namespace": metrics_metadata.get(metric_name, {}).get("namespace"),
                        "metric_placement": metrics_metadata.get(metric_name, {}).get("placement"),
                        "metric_shared_key": metrics_metadata.get(metric_name, {}).get("shared_key"),
                    }
                )

        # Process sections data
        for section in data.get("sections", []):
            section_id = section.get("id", "")
            section_name = section.get("name", "")
            section_anchor = section.get("anchor", "")

            flat_records.append(
                {
                    # Run information
                    "run_id": run_id,
                    "creation_date": creation_date,
                    # Module information
                    "module_id": module_id,
                    "module_name": module_name,
                    # Section information (no sample or metric)
                    "entity_type": "section",
                    "section_id": section_id,
                    "section_name": section_name,
                    "section_anchor": section_anchor,
                    "section_description": section.get("description", ""),
                    "section_module": section.get("module", ""),
                    "section_module_anchor": section.get("module_anchor", ""),
                    "section_module_info": section.get("module_info", ""),
                    "section_comment": section.get("comment", ""),
                    "section_helptext": section.get("helptext", ""),
                    "section_content_before_plot": section.get("content_before_plot", ""),
                    "section_content": section.get("content", ""),
                    "section_plot": section.get("plot", ""),
                    "section_print_section": section.get("print_section", False),
                    "section_plot_anchor": section.get("plot_anchor", ""),
                    "section_ai_summary": section.get("ai_summary", ""),
                }
            )

        # Convert to DataFrame
        df = pd.DataFrame(flat_records)

        # Add entity_type for sample metrics if not already set
        if "entity_type" in df.columns:
            df.loc[df["entity_type"].isna(), "entity_type"] = "sample_metric"
        else:
            df["entity_type"] = "sample_metric"

    return df


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
                _ = pd.read_parquet(str(output_file))
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
                row_data[metric] = value

            table_rows.append(row_data)

        # Combine plot_input and table_row rows for this run
        run_rows = plot_input_rows + table_rows

        # Convert to DataFrame
        df = pd.DataFrame(run_rows)
        df["creation_date"] = (
            pd.to_datetime(df["creation_date"], utc=True)
            .dt.floor("us")  # tz-aware (+02:00)
            .dt.tz_localize(None)  # …but drop the zone
            .astype("datetime64[us]")  # make it explicit
        )

        # Write to parquet immediately to save memory
        df.to_parquet(str(output_file))
        generated_files.append(str(output_file))

        print(f"Generated and saved run {run_id} to {output_file}")

        # Free memory
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


def generate_long_format_files():
    target_path.mkdir(parents=True, exist_ok=True)

    for run_number in range(1, args.num_runs + 1):
        # Skip runs before the start-from value
        if run_number < args.start_from:
            print(f"Skipping run {run_number} because it's before start-from value ({args.start_from})")
            continue

        out_path = target_path / f"run_{run_number}.parquet"
        should_generate = True

        if out_path.exists():
            # Verify the file can be read properly
            try:
                # Attempt to read the parquet file
                test_df = pd.read_parquet(str(out_path))
                # If successful, skip generation
                print(f"Skipping run {run_number} because it already exists and can be read properly")
                should_generate = False
            except Exception as e:
                print(f"Found corrupted parquet file for run {run_number}, regenerating: {str(e)}")
                # Will proceed to regenerate the file

        if not should_generate:
            continue

        # Generate date as datetime object instead of string
        creation_date = datetime.now() - timedelta(days=random.randint(0, 100))
        run_id = f"run_{run_number}"
        print(f"Generating {run_id}, creation_date: {creation_date}, writing to {out_path}")

        metrics_metadata = {}
        for module_number in range(args.num_modules):
            for metric_number in range(args.num_metrics_per_module):
                metrics_metadata[f"metric_{metric_number}"] = generate_metric_metadata()

        rows = []
        for module_number in range(args.num_modules):
            for sample_number in range(args.num_samples_per_module):
                for metric_number in range(args.num_metrics_per_module):
                    mm = metrics_metadata[f"metric_{metric_number}"]
                    value = random.gauss(mu=50, sigma=16.67)  # sigma chosen so ~99.7% of values fall within 0-100
                    value = min(max(value, mm["min"]), mm["max"])
                    row = {
                        "run_id": run_id,
                        "creation_date": creation_date,  # Store as datetime object directly
                        "module_name": f"module_{module_number}",
                        "sample_name": f"sample_{sample_number}",
                        "metric_name": f"metric_{metric_number}",
                        "metric_min": mm["min"],
                        "metric_max": mm["max"],
                        "metric_dmin": mm["dmin"],
                        "metric_dmax": mm["dmax"],
                        "metric_scale": mm["scale"],
                        "metric_color": mm["color"],
                        "val_raw": value,
                        "val_raw_type": "float",
                    }
                    rows.append(row)

        df = pd.DataFrame(rows)
        df["creation_date"] = (
            pd.to_datetime(df["creation_date"], utc=True)
            .dt.floor("us")  # tz-aware (+02:00)
            .dt.tz_localize(None)  # …but drop the zone
            .astype("datetime64[us]")  # make it explicit
        )
        out_path.mkdir(parents=True, exist_ok=True)
        df.to_parquet(str(out_path))


# def generate_long_format_files_old():
#     target_path.mkdir(parents=True, exist_ok=True)

#     for run_number in range(1, args.num_runs + 1):
#         out_path = target_path / f"run_{run_number}.parquet"
#         if out_path.exists():
#             print(f"Skipping run {run_number} because it already exists")
#             continue

#         print(f"Generating run {run_number}, writing to {out_path}")
#         timestamp = (datetime.now() - timedelta(days=random.randint(0, 100))).isoformat()
#         run_id = f"run_{run_number}"
#         data = {
#             "run_id": run_id,
#             "creation_date": timestamp,
#             "modules": [],
#         }

#         for module_number in range(args.num_modules):
#             module_data = generate_module_data(
#                 module_number, args.num_samples_per_module, args.num_metrics_per_module, 5
#             )
#             data["modules"].append(module_data)

#         out_path.mkdir(parents=True, exist_ok=True)
#         df = flatten_hierarchical_data(data)
#         df.to_parquet(str(out_path))


if args.format == "wide":
    # Generate wide format data (with fewer samples for demonstration)
    generate_wide_format_files()

else:
    # Generate long format data
    generate_long_format_files()
