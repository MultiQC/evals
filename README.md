# MultiQC evaluation framework

Evaluation framework of meta-analysis of MultiQC outputs at scale. This project prepares synthetic and real-world MultiQC datasets, stores them in Parquet format (wide or long tables), ingests into Iceberg (Cloudflare REST or AWS Glue), and benchmarks query performance using different engines (Trino, DuckDB, PyArrow).

Benchamrking results can be explored of wide vs long formats are found [here](https://docs.google.com/spreadsheets/d/1bULcsneRp7N_m9JspwxpWBYTJ9qhYrdlqpIC4PWn49A/edit?usp=sharing)

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Data Generation](#data-generation)
- [Data Ingestion](#data-ingestion)
- [Benchmarking](#benchmarking)
- [Setup and Installation](#setup-and-installation)
- [Usage](#usage)
- [Storage Backends](#storage-backends)
- [Query Engines](#query-engines)
- [Performance Analysis](#performance-analysis)

## Overview

This project evaluates different approaches for storing and querying MultiQC data at scale. It supports:

- **Data Generation**: Both simulated and real-world MultiQC datasets
- **Storage Formats**: Parquet (local/S3), Apache Iceberg, MongoDB
- **Query Engines**: Trino, DuckDB, PyArrow, direct database queries
- **Benchmarking**: Performance comparison across different storage and query combinations

The framework is designed to help determine optimal storage strategies for MultiQC data based on query patterns, data volume, and performance requirements.

## Project Structure

```
evals/
├── scripts/                      # Data generation and setup scripts
│   ├── gen-simulated.py          # Generate synthetic MultiQC data
│   ├── gen-reallife.py           # Process real MultiQC datasets
│   ├── init-catalog.py           # Initialize Iceberg catalogs and ingest schemas and tables
├── notebooks/                    # Analysis and benchmarking notebooks
│   ├── query.ipynb               # Trino query performance analysis of wide vs. long formats
│   ├── mongo_vs_parquet.ipynb    # MongoDB vs Parquet comparison
│   └── paquert_compression.ipynb # Parquet compression analysis
├── trino/                        # Trino configuration
└── docker-compose files          # Container orchestration for local set up
```

## Data Generation

### Simulated Data Generation

The `scripts/gen-simulated.py` script generates synthetic MultiQC datasets with configurable parameters:

```bash
python scripts/gen-simulated.py \
    --format wide \
    --num_runs 1000 \
    --num_modules 10 \
    --num_sections 5 \
    --num_samples_per_module 100 \
    --num_metrics_per_module 50 \
    --upload
```

**Parameters:**

- `--format`: Data format (`wide` or `long`)
- `--num_runs`: Number of MultiQC runs to generate
- `--num_modules`: Number of modules per run
- `--num_sections`: Number of sections per module
- `--num_samples_per_module`: Number of samples per module
- `--num_metrics_per_module`: Number of metrics per module
- `--upload`: Upload to S3 (requires AWS credentials)
- `--start-from`: Resume generation from specific run number

**Data Formats:**

1. **Wide Format**: Each row represents a sample with metrics as columns

   - Optimized for sample-centric queries
   - Includes both `plot_input` and `table_row` data types
   - Better for retrieving all metrics for specific samples

2. **Long Format**: Each row represents a single metric value

   - Optimized for metric-centric queries and analytics
   - Better for filtering by specific metrics across many samples
   - Includes comprehensive metadata for each metric

**Generated Data Structure:**

- **Run-level**: Unique run IDs, creation timestamps
- **Module-level**: Module metadata, anchors, DOIs
- **Sample-level**: Sample identifiers and metric values
- **Metric-level**: Value metadata, formatting, constraints
- **Section-level**: Plot configurations, content metadata

### Real World Data Processing

The `scripts/gen-reallife.py` script processes existing MultiQC reports:

```bash
python scripts/gen-reallife.py --rerun
```

This script:

1. Runs MultiQC on real datasets in specified directories
2. Converts output to Parquet format
3. Uploads processed data to S3
4. Handles projects: Xing2020, BSstuff, Petropoulus_2016

## Data Ingestion

### Iceberg Catalog Setup

The `scripts/init-catalog.py` script initializes Apache Iceberg tables:

```bash
python scripts/init-catalog.py \
    --s3-prefix simulated/wide_1000runs_10mod_5sec_100samples_50metrics \
    --platform glue \
    --format wide \
    --max-files 1000 \
    --files-per-chunk 100
```

**Features:**

- **Platform Support**: AWS Glue and Cloudflare R2
- **Schema Detection**: Automatic schema inference from Parquet files
- **Partitioning**: Optimized partitioning strategies for long data format
- **Chunked Loading**: Handles large datasets by processing files in chunks
- **Resume Capability**: Can resume interrupted ingestion processes

**Partitioning Strategies:**

- **Long Format**: Partitioned by month of creation_date for time-based queries
- **Wide Format**: Custom partitioning based on query patterns
- **Sorting**: Optimized sort orders for common query patterns

## Benchmarking

### Query Performance Analysis

The framework includes comprehensive benchmarking across multiple dimensions:

#### 1. Storage Format Comparison (`notebooks/mongo_vs_parquet.ipynb`)

Compares MongoDB vs Parquet for:

- **Storage Efficiency**: File sizes and compression ratios
- **Query Performance**: Single metric and module queries
- **Scalability**: Performance with increasing data volumes

**Key Findings:**

- Parquet: Better for analytical queries, columnar efficiency
- MongoDB: Better for document-oriented access patterns
- Compression: Parquet typically 2-4x smaller than JSON

#### 2. Parquet Optimization (`notebooks/parquet_stess_test.ipynb`)

Tests Parquet performance with:

- **Partitioning Strategies**: Run-based partitioning effects
- **Query Engines**: DuckDB vs PyArrow performance
- **Data Patterns**: Impact of data distribution on query speed

#### 3. Compression Analysis (`notebooks/paquert_compression.ipynb`)

Evaluates compression efficiency across:

- **Data Patterns**: Unique, repeated, cyclic, sorted values
- **Compression Algorithms**: Brotli, Snappy, Gzip
- **Cardinality Effects**: Impact of unique value counts

**Compression Results:**

- Repeated values: Up to 99.99% compression ratio
- Unique values: ~40% compression ratio
- Sorted data: ~27% compression ratio

#### 4. Multi-Engine Comparison (`notebooks/query.ipynb`)

Benchmarks query performance across:

- **Trino**: Distributed SQL engine
- **DuckDB**: Embedded analytical database
- **Direct Parquet**: PyArrow and DuckDB file access

**Query Patterns Tested:**

- Metric filtering across runs
- Date range queries
- Sample-specific lookups
- Cross-metric analysis
- Aggregation queries

Benchamrking results can be explored [here](https://docs.google.com/spreadsheets/d/1bULcsneRp7N_m9JspwxpWBYTJ9qhYrdlqpIC4PWn49A/edit?usp=sharing)

### Sample Benchmark Queries

The `scripts/trino-queries.sql` file contains representative queries:

```sql
-- Metric-specific filtering with partitioning
SELECT run_id, sample_name, val_raw
FROM "megaqc-test"."simulated"."long_format_table"
WHERE metric_name = 'metric_0'
LIMIT 100;

-- Time-based analysis
SELECT date_trunc('month', creation_date) as month,
       AVG(val_raw) as average_value
FROM "megaqc-test"."simulated"."long_format_table"
WHERE metric_name = 'metric_0'
GROUP BY date_trunc('month', creation_date)
ORDER BY month;

-- Performance analysis with EXPLAIN
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM "megaqc-test"."simulated"."long_format_table"
WHERE metric_name = 'metric_0';
```

## Setup and Installation

### Prerequisites

- Python 3.12+
- Docker and Docker Compose
- AWS CLI (for S3 access)
- DuckDB (for local testing)

### Installation

1. **Clone the repository:**

   ```bash
   git clone <repository-url>
   cd evals
   ```

2. **Install dependencies:**

   ```bash
   pip install -e .
   ```

3. **Configure environment:**

   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

4. **Start services:**

   ```bash
   # Trino + Iceberg
   docker-compose -f iceberg-docker-compose.yml up -d

   # Superset (optional)
   docker-compose -f superset-docker-compose.yml up -d
   ```

### Environment Variables

Required in `.env` file:

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=us-east-1

# Cloudflare R2 (optional)
CLOUDFLARE_TOKEN=your_token
CLOUDFLARE_ACCESS_KEY_ID=your_key
CLOUDFLARE_SECRET_ACCESS_KEY=your_secret
```

## Usage

### Basic Workflow

1. **Generate test data:**

   ```bash
   python scripts/gen-simulated.py --format long --num_runs 100
   ```

2. **Set up Iceberg catalog:**

   ```bash
   python scripts/init-catalog.py --s3-prefix simulated/your_dataset
   ```

3. **Run benchmarks:**
   ```bash
   jupyter notebook notebooks/query.ipynb
   ```

### Advanced Usage

**Large-scale data generation:**

```bash
python scripts/gen-simulated.py \
    --format long \
    --num_runs 10000 \
    --num_modules 20 \
    --num_samples_per_module 1000 \
    --num_metrics_per_module 100 \
    --upload \
    --start-from 5000
```

**Chunked Iceberg ingestion:**

```bash
python scripts/init-catalog.py \
    --s3-prefix simulated/large_dataset \
    --max-files 10000 \
    --files-per-chunk 500 \
    --start-from 10
```

## Storage Backends

### 1. Apache Iceberg

- **Use Case**: Large-scale analytical workloads
- **Advantages**: ACID transactions, time travel, schema evolution
- **Platforms**: AWS Glue, Cloudflare R2
- **Optimization**: Partitioning and sorting for query patterns

### 2. Parquet Files

- **Use Case**: Direct file-based analytics
- **Advantages**: Columnar efficiency, broad tool support
- **Storage**: Local filesystem, S3
- **Optimization**: Partitioning by run_id, compression tuning

### 3. MongoDB

- **Use Case**: Document-oriented access patterns
- **Advantages**: Flexible schema, nested data support
- **Optimization**: Indexing on common query fields

## Query Engines

### 1. Trino

- **Strengths**: Distributed queries, multiple data sources
- **Use Case**: Large-scale analytics across catalogs
- **Configuration**: Iceberg connector, S3 access

### 2. DuckDB

- **Strengths**: Embedded analytics, fast local queries
- **Use Case**: Development, single-machine analytics
- **Features**: Direct Parquet access, Iceberg support

### 3. PyArrow

- **Strengths**: Direct file access, Python integration
- **Use Case**: Data science workflows, ETL processes
- **Features**: Predicate pushdown, columnar operations

## Performance Analysis

### Key Metrics

1. **Query Latency**: Time to execute common query patterns
2. **Storage Efficiency**: Compression ratios and file sizes
3. **Scalability**: Performance with increasing data volumes
4. **Resource Usage**: Memory and CPU consumption

### Optimization Strategies

1. **Partitioning**: Align with query patterns (time, metric, sample)
2. **Compression**: Choose algorithms based on data characteristics
3. **Indexing**: Create appropriate indexes for access patterns
4. **Caching**: Leverage query result caching where available

### Benchmark Results Summary

Based on testing with various dataset sizes:

- **Small datasets (< 1GB)**: DuckDB and direct Parquet access perform best
- **Medium datasets (1-100GB)**: Iceberg with proper partitioning optimal
- **Large datasets (> 100GB)**: Trino with distributed processing required
- **Compression**: Brotli provides best compression for most MultiQC data patterns
- **Query patterns**: Long format better for metric-centric analysis, wide format better for sample-centric queries

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

MIT License - see LICENSE file for details.
