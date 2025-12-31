# IDR Scale Testing

Tools for generating synthetic data and benchmarking identity resolution at scale.

## Quick Start

### 1. Generate Test Data

```bash
# Install dependencies
pip install pyarrow

# Generate 20M rows
python tools/scale_test/data_generator.py --rows=20000000 --seed=42 --output=data/

# Generate 50M rows
python tools/scale_test/data_generator.py --rows=50000000 --seed=42 --output=data/

# Generate 100M rows
python tools/scale_test/data_generator.py --rows=100000000 --seed=42 --output=data/
```

### 2. Load Data into Platform

```bash
# DuckDB
python tools/scale_test/loaders.py --platform=duckdb --data=data/retail_customers_20m.parquet --db=idr.duckdb

# Snowflake (set env vars first)
export SNOWFLAKE_ACCOUNT=xxx SNOWFLAKE_USER=xxx SNOWFLAKE_PASSWORD=xxx SNOWFLAKE_DATABASE=xxx SNOWFLAKE_WAREHOUSE=xxx
python tools/scale_test/loaders.py --platform=snowflake --data=data/retail_customers_20m.parquet

# BigQuery
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
python tools/scale_test/loaders.py --platform=bigquery --data=data/retail_customers_20m.parquet --project=your-project
```

### 3. Run Benchmark

```bash
# DuckDB benchmark
python tools/scale_test/benchmark.py \
    --platform=duckdb \
    --data=data/retail_customers_20m.parquet \
    --rows=20000000 \
    --db=idr_benchmark.duckdb \
    --output=benchmarks/
```

## Data Schema

Retail customer profile matching Nike/Lululemon industry patterns:

| Column | Type | Description |
|--------|------|-------------|
| entity_id | UUID | Unique entity identifier |
| first_name | VARCHAR | Customer first name |
| last_name | VARCHAR | Customer last name |
| email | VARCHAR | Email address |
| phone | VARCHAR | Phone number (10 digits) |
| loyalty_id | VARCHAR | Loyalty program ID |
| address_street | VARCHAR | Street address |
| address_city | VARCHAR | City |
| address_state | VARCHAR | State (2-letter) |
| address_zip | VARCHAR | ZIP code |
| loyalty_tier | VARCHAR | bronze/silver/gold/platinum/diamond |
| created_at | TIMESTAMP | Account creation date |
| source_system | VARCHAR | web/mobile_app/store_pos/call_center/partner |

## Cluster Distribution

Matches real-world retail patterns:

| Cluster Size | Percentage | Description |
|--------------|------------|-------------|
| 1 | 35% | New/one-time customers |
| 2 | 25% | Customer + one alt identity |
| 3-5 | 20% | Family accounts |
| 6-15 | 12% | Frequent buyers |
| 16-50 | 5% | Business accounts |
| 51-200 | 2% | Corporate accounts |
| 201-1000 | 1% | Data quality issues |

## Reproducibility

Same seed → identical data on any platform:

```bash
# These produce the EXACT same data:
python data_generator.py --rows=20000000 --seed=42  # Run anywhere
python data_generator.py --rows=20000000 --seed=42  # Run on different machine
```

This ensures fair benchmarking across DuckDB, Snowflake, BigQuery, and Databricks.

## Benchmark Results

See [benchmark-results.md](../../docs/performance/benchmark-results.md) for results.
