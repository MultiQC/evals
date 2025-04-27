import json
import os
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from cloudpathlib import S3Path

NUM_RUNS = 10
NUM_MODULES = 10
NUM_SECTIONS_PER_MODULE = 5
NUM_SAMPLES_PER_MODULE = 10
NUM_METRICS_PER_MODULE = 20

# Paths
# PARQUET_BUCKET = S3Path("s3://megaqc-test/simulated/")
PARQUET_BUCKET = S3Path("s3://vlad-megaqc/simulated/")
# PARQUET_BUCKET = Path("./data")

# Parquet setup
dir_name = f"{NUM_RUNS}runs_{NUM_MODULES}mod_{NUM_SAMPLES_PER_MODULE}samples_{NUM_METRICS_PER_MODULE}metrics"
target_path = PARQUET_BUCKET / dir_name


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


def generate_value_metadata(value):
    """Generate metadata for a value"""
    return {
        "val_raw": value,
        "val_raw_type": random.choice(["int", "float", "str"]),
        "val_mod": value,
        "val_mod_type": random.choice(["int", "float", "str"]),
        "val_fmt": f"{value:.2f}" if isinstance(value, float) else str(value),
    }


def generate_sample_data(num_metrics):
    """Generate data for a single sample"""
    sample_id = generate_random_string()
    metrics = {}

    for i in range(num_metrics):
        metric_name = f"metric_{i}"
        value = random.uniform(0, 100)
        metrics[metric_name] = (generate_value_metadata(value),)

    return {"sample_id": sample_id, "metrics": metrics}


def generate_module_data(module_index, num_samples, num_metrics, num_sections):
    """Generate data for a single module"""
    samples = [generate_sample_data(num_metrics) for _ in range(num_samples)]
    sections = [generate_section_data() for _ in range(num_sections)]

    metrics_metadata = {}
    for i in range(num_metrics):
        metric_name = f"metric_{i}"
        metrics_metadata[metric_name] = generate_metric_metadata()

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


def flatten_hierarchical_data(data):
    """Convert hierarchical data to flat format for Parquet

    Args:
        data: Nested JSON-like data with runs, modules, samples, and metrics

    Returns:
        pd.DataFrame: Flattened dataframe with one row per metric value
    """
    flat_records = []

    run_id = data["run_id"]
    timestamp = data["timestamp"]
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
                    "timestamp": timestamp,
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
                "timestamp": timestamp,
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


json_dir = target_path / "json"
parquet_dir = target_path / "parquet"
json_dir.mkdir(parents=True, exist_ok=True)
parquet_dir.mkdir(parents=True, exist_ok=True)

for run_number in range(NUM_RUNS):
    print(f"Generating run {run_number}")
    timestamp = (datetime.now() - timedelta(days=random.randint(0, 100))).isoformat()
    run_id = f"run_{run_number}"
    for module_number in range(NUM_MODULES):
        data = generate_module_data(
            module_number, NUM_SAMPLES_PER_MODULE, NUM_METRICS_PER_MODULE, NUM_SECTIONS_PER_MODULE
        )
        data.update(
            {
                "run_id": run_id,
                "timestamp": timestamp,
            }
        )

        df = flatten_hierarchical_data(data)
        df.to_parquet(str(parquet_dir / f"run_{run_number}_module_{module_number}.parquet"))
