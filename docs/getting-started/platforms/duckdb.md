# DuckDB Setup

DuckDB is the recommended platform for **development, testing, and small-to-medium datasets**.

---

## Prerequisites

- Python 3.9+
- DuckDB (`pip install duckdb`)

---

## Installation

```bash
# Clone repository
git clone https://github.com/anilkulkarni87/sql-identity-resolution.git
cd sql-identity-resolution

# Install dependencies
pip install duckdb
```

---

## Create Database

```bash
# Create database with all schemas and tables
duckdb idr.duckdb < sql/duckdb/00_ddl_all.sql
```

This creates three schemas:

| Schema | Purpose |
|--------|---------|
| `idr_meta` | Configuration (sources, rules, mappings) |
| `idr_work` | Temporary processing tables |
| `idr_out` | Output tables (membership, clusters, metrics) |

---

## Configure Sources

Connect to DuckDB and insert your configuration:

```bash
duckdb idr.duckdb
```

```sql
-- Register your source table
INSERT INTO idr_meta.source_table VALUES
  ('customers', 'main.customers', 'PERSON', 'customer_id', 'updated_at', 0, TRUE);

-- Define matching rules
INSERT INTO idr_meta.rule VALUES
  ('email_exact', 'EMAIL', 1, TRUE, 10000),
  ('phone_exact', 'PHONE', 2, TRUE, 5000);

-- Map identifiers to columns
INSERT INTO idr_meta.identifier_mapping VALUES
  ('customers', 'EMAIL', 'email', TRUE),
  ('customers', 'PHONE', 'phone', TRUE);
```

---

## Run IDR

### Dry Run (Preview)

```bash
python sql/duckdb/idr_run.py \
  --db=idr.duckdb \
  --run-mode=FULL \
  --dry-run
```

### Live Run

```bash
python sql/duckdb/idr_run.py \
  --db=idr.duckdb \
  --run-mode=FULL
```

### Incremental Run

```bash
python sql/duckdb/idr_run.py \
  --db=idr.duckdb \
  --run-mode=INCR
```

---

## CLI Options

| Option | Description | Default |
|--------|-------------|---------|
| `--db` | Path to DuckDB database file | Required |
| `--run-mode` | `FULL` or `INCR` | `INCR` |
| `--max-iters` | Max label propagation iterations | 30 |
| `--dry-run` | Preview mode (no commits) | False |

---

## Verify Results

```sql
-- Check run history
SELECT run_id, status, entities_processed, duration_seconds 
FROM idr_out.run_history 
ORDER BY started_at DESC;

-- View cluster distribution
SELECT cluster_size, COUNT(*) as count
FROM idr_out.identity_clusters_current
GROUP BY cluster_size
ORDER BY cluster_size;

-- Lookup specific entity
SELECT * FROM idr_out.identity_resolved_membership_current
WHERE entity_key = 'customers:12345';
```

---

## Scheduling

### Cron

```bash
# Run every hour
0 * * * * cd /path/to/repo && python sql/duckdb/idr_run.py --db=idr.duckdb --run-mode=INCR
```

### Airflow

See [Scheduling Guide](../../deployment/scheduling.md) for DAG templates.

---

## Performance Tips

1. **Use Incremental Mode**: After initial FULL run, use INCR for better performance
2. **Set max_group_size**: Limit overly generic identifiers (e.g., `test@test.com`)
3. **Index source tables**: Add indexes on entity key and watermark columns

---

## Next Steps

- [Configuration Guide](../../guides/configuration.md)
- [Dry Run Mode](../../guides/dry-run-mode.md)
- [Troubleshooting](../../guides/troubleshooting.md)
