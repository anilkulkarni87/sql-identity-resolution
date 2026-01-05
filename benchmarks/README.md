# Benchmarks

Performance testing and benchmarking suite for the SQL Identity Resolution framework.

## Directory Structure

```
benchmarks/
├── duckdb/          # DuckDB scale testing scripts
├── bigquery/        # BigQuery scale testing scripts
├── snowflake/       # Snowflake scale testing scripts
├── databricks/      # Databricks scale testing scripts
├── tools/           # Cross-platform data loader
├── results/         # Benchmark results documentation
└── data/            # Generated test data (gitignored)
```

## Running Benchmarks

### DuckDB (Local)
```bash
cd benchmarks/duckdb
python idr_scale_test.py --scale 10000000
```

### BigQuery
```bash
# Setup data
bq query --use_legacy_sql=false < benchmarks/bigquery/scale_test_setup.sql

# Load metadata
bq query --use_legacy_sql=false < benchmarks/bigquery/scale_test_metadata.sql

# Run IDR
python sql/bigquery/core/idr_run.py --project=YOUR_PROJECT --run-mode=FULL
```

### Snowflake
```sql
-- In Snowflake worksheet
!source benchmarks/snowflake/IDR_ScaleTest.sql;
CALL IDR_RUN('FULL', 50, FALSE);
```

### Databricks
Upload `benchmarks/databricks/IDR_ScaleTest.py` to a Databricks notebook and run.

## Latest Results (10M rows)

| Platform | Duration | Cost |
|----------|----------|------|
| DuckDB | 143s | Free |
| Snowflake | 168s | ~$0.25 |
| BigQuery | 295s | ~$0.50 |
| Databricks | 317s | TBD |

See [full results](results/benchmark-results.md) for details.
