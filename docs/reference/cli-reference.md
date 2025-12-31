# CLI Reference

Command-line interface reference for SQL Identity Resolution runners.

---

## DuckDB CLI

### Usage

```bash
python sql/duckdb/idr_run.py [OPTIONS]
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--db` | STRING | **Required** | Path to DuckDB database file |
| `--run-mode` | ENUM | `INCR` | `FULL` or `INCR` |
| `--max-iters` | INT | `30` | Max label propagation iterations |
| `--dry-run` | FLAG | `False` | Preview mode (no commits) |

### Examples

```bash
# Full run
python sql/duckdb/idr_run.py --db=idr.duckdb --run-mode=FULL

# Incremental run
python sql/duckdb/idr_run.py --db=idr.duckdb --run-mode=INCR

# Dry run
python sql/duckdb/idr_run.py --db=idr.duckdb --run-mode=FULL --dry-run

# Custom max iterations
python sql/duckdb/idr_run.py --db=idr.duckdb --max-iters=50
```

### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | Error |

---

## BigQuery CLI

### Usage

```bash
python sql/bigquery/idr_run.py [OPTIONS]
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--project` | STRING | **Required** | GCP project ID |
| `--run-mode` | ENUM | `INCR` | `FULL` or `INCR` |
| `--max-iters` | INT | `30` | Max label propagation iterations |
| `--dry-run` | FLAG | `False` | Preview mode (no commits) |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account JSON |

### Examples

```bash
# Set credentials
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Full run
python sql/bigquery/idr_run.py --project=my-project --run-mode=FULL

# Dry run
python sql/bigquery/idr_run.py --project=my-project --dry-run
```

---

## Snowflake Stored Procedure

### Signature

```sql
CALL idr_run(
    RUN_MODE VARCHAR,      -- 'FULL' or 'INCR'
    MAX_ITERS INT,         -- Max iterations (e.g., 30)
    DRY_RUN BOOLEAN        -- TRUE = preview only
);
```

### Examples

```sql
-- Full run
CALL idr_run('FULL', 30, FALSE);

-- Incremental run
CALL idr_run('INCR', 30, FALSE);

-- Dry run
CALL idr_run('FULL', 30, TRUE);
```

### Return Value

Returns a VARCHAR with run summary:

```
SUCCESS: run_id=run_abc123, entities=1234, edges=5678, iterations=5, duration=12s
```

Or for dry runs:

```
DRY_RUN_COMPLETE: run_id=dry_run_abc123, new_entities=100, moved_entities=50, duration=8s | DRY RUN - NO CHANGES COMMITTED
```

---

## Databricks Widgets

### Widget Parameters

| Widget | Type | Options | Default | Description |
|--------|------|---------|---------|-------------|
| `RUN_MODE` | dropdown | `INCR`, `FULL` | `INCR` | Processing mode |
| `MAX_ITERS` | text | Integer | `30` | Max iterations |
| `DRY_RUN` | dropdown | `true`, `false` | `false` | Preview mode |
| `RUN_ID` | text | String | Auto-generated | Custom run ID |

### Programmatic Access

```python
# Read widget values
run_mode = dbutils.widgets.get("RUN_MODE")
dry_run = dbutils.widgets.get("DRY_RUN") == "true"

# Set widget defaults
dbutils.widgets.dropdown("RUN_MODE", "INCR", ["INCR", "FULL"])
dbutils.widgets.dropdown("DRY_RUN", "false", ["true", "false"])
```

### Running via Jobs API

```json
{
  "notebook_task": {
    "notebook_path": "/Repos/org/repo/sql/databricks/notebooks/IDR_Run",
    "base_parameters": {
      "RUN_MODE": "INCR",
      "DRY_RUN": "false",
      "MAX_ITERS": "30"
    }
  }
}
```

---

## Metrics Exporter CLI

### Usage

```bash
python tools/metrics_exporter.py [OPTIONS]
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--platform` | ENUM | **Required** | `duckdb`, `snowflake`, `bigquery` |
| `--connection` | STRING | | DuckDB: path; Others: connection string |
| `--exporter` | ENUM | `stdout` | `stdout`, `prometheus`, `datadog`, `webhook` |
| `--prometheus-port` | INT | `9090` | Port for Prometheus metrics |
| `--datadog-api-key` | STRING | | DataDog API key |
| `--webhook-url` | STRING | | Webhook endpoint URL |
| `--run-id` | STRING | | Export specific run only |

### Examples

```bash
# Print to stdout
python tools/metrics_exporter.py --platform=duckdb --connection=idr.duckdb

# Prometheus endpoint
python tools/metrics_exporter.py --platform=duckdb --connection=idr.duckdb \
    --exporter=prometheus --prometheus-port=9090

# DataDog
python tools/metrics_exporter.py --platform=snowflake \
    --connection="account=xxx;user=xxx;password=xxx" \
    --exporter=datadog --datadog-api-key=$DD_API_KEY

# Webhook
python tools/metrics_exporter.py --platform=bigquery \
    --connection="project=my-project" \
    --exporter=webhook --webhook-url=https://hooks.slack.com/xxx
```

---

## Dashboard Generator CLI

### Usage

```bash
python tools/dashboard/generator.py [OPTIONS]
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--platform` | ENUM | **Required** | `duckdb`, `snowflake`, `bigquery`, `databricks` |
| `--connection` | STRING | **Required** | Platform-specific connection string |
| `--output` | STRING | `dashboard.html` | Output file path |
| `--run-id` | STRING | | Focus on specific run |

### Examples

```bash
# Generate from DuckDB
python tools/dashboard/generator.py \
    --platform=duckdb \
    --connection=idr.duckdb \
    --output=dashboard.html

# Open in browser
open dashboard.html
```

---

## Common Patterns

### CI/CD Integration

```bash
#!/bin/bash
# ci-run.sh

# Dry run first
python sql/duckdb/idr_run.py --db=idr.duckdb --dry-run
if [ $? -ne 0 ]; then
    echo "Dry run failed"
    exit 1
fi

# Check for unexpected changes
MOVED=$(duckdb idr.duckdb -c "SELECT COUNT(*) FROM idr_out.dry_run_results WHERE change_type='MOVED'" | tail -1)
if [ "$MOVED" -gt 1000 ]; then
    echo "Too many moved entities: $MOVED"
    exit 1
fi

# Live run
python sql/duckdb/idr_run.py --db=idr.duckdb
```

### Scheduled Run with Logging

```bash
#!/bin/bash
# scheduled-run.sh

LOG_FILE="/var/log/idr/$(date +%Y%m%d_%H%M%S).log"

python sql/duckdb/idr_run.py \
    --db=/data/idr.duckdb \
    --run-mode=INCR \
    2>&1 | tee "$LOG_FILE"

# Check exit code
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    # Send alert
    curl -X POST https://hooks.slack.com/xxx \
        -d "{\"text\": \"IDR run failed. See $LOG_FILE\"}"
fi
```

---

## Next Steps

- [Schema Reference](schema-reference.md)
- [Metrics Reference](metrics-reference.md)
- [Troubleshooting](../guides/troubleshooting.md)
