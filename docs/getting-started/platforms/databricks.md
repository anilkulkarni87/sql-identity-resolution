# Databricks Setup

Databricks is ideal for **large-scale processing** and organizations with existing Spark infrastructure.

---

## Prerequisites

- Databricks workspace (AWS, Azure, or GCP)
- Cluster with Spark 3.x
- Unity Catalog or Hive metastore

---

## Installation

### 1. Import Notebooks

Clone the repository to your Databricks Repos:

```
Workspace → Repos → Add Repo → 
URL: https://github.com/anilkulkarni87/sql-identity-resolution.git
```

Or manually import notebooks from `sql/databricks/notebooks/`:

- `IDR_QuickStart.py` - Setup and demo
- `IDR_Run.py` - Main runner

### 2. Run Quick Start

Open and run `IDR_QuickStart.py`. This will:

1. Create schemas (`idr_meta`, `idr_work`, `idr_out`)
2. Create all required tables
3. Insert sample data
4. Run a demo IDR process

---

## Configure Sources

```python
# In a notebook cell
spark.sql("""
INSERT INTO idr_meta.source_table VALUES
  ('customers', 'crm.customers', 'PERSON', 'customer_id', 'updated_at', 0, TRUE)
""")

spark.sql("""
INSERT INTO idr_meta.rule VALUES
  ('email_exact', 'EMAIL', 1, TRUE, 10000),
  ('phone_exact', 'PHONE', 2, TRUE, 5000)
""")

spark.sql("""
INSERT INTO idr_meta.identifier_mapping VALUES
  ('customers', 'EMAIL', 'email', TRUE),
  ('customers', 'PHONE', 'phone', TRUE)
""")
```

---

## Run IDR

### Widget Parameters

The `IDR_Run.py` notebook uses widgets for configuration:

| Widget | Description | Options |
|--------|-------------|---------|
| `RUN_MODE` | Processing mode | `INCR`, `FULL` |
| `MAX_ITERS` | Max LP iterations | Integer (default: 30) |
| `DRY_RUN` | Preview mode | `true`, `false` |
| `RUN_ID` | Custom run ID | String (optional) |

### Dry Run (Preview)

1. Open `IDR_Run.py` notebook
2. Set widgets:
   - `RUN_MODE` = `FULL`
   - `DRY_RUN` = `true`
3. Run All

### Live Run

1. Set `DRY_RUN` = `false`
2. Run All

---

## Review Dry Run Results

```python
# View proposed changes
display(spark.sql("""
    SELECT entity_key, current_resolved_id, proposed_resolved_id, change_type
    FROM idr_out.dry_run_results
    WHERE run_id = 'dry_run_abc123'
    ORDER BY change_type
"""))

# Summary
display(spark.sql("""
    SELECT * FROM idr_out.dry_run_summary
    WHERE run_id = 'dry_run_abc123'
"""))
```

---

## Verify Results

```python
# Run history
display(spark.sql("""
    SELECT run_id, status, entities_processed, duration_seconds, warnings
    FROM idr_out.run_history
    ORDER BY started_at DESC
    LIMIT 10
"""))

# Cluster distribution
display(spark.sql("""
    SELECT cluster_size, COUNT(*) as count
    FROM idr_out.identity_clusters_current
    GROUP BY cluster_size
    ORDER BY cluster_size
"""))
```

---

## Scheduling with Databricks Workflows

### Create a Job

1. Go to **Workflows** → **Create Job**
2. Add task:
   - **Type**: Notebook
   - **Path**: `/Repos/.../IDR_Run`
   - **Cluster**: Select your cluster
3. Set parameters:
   ```json
   {
     "RUN_MODE": "INCR",
     "DRY_RUN": "false"
   }
   ```
4. Add schedule (e.g., hourly)

### Job Definition (JSON)

```json
{
  "name": "IDR Hourly",
  "tasks": [
    {
      "task_key": "idr_run",
      "notebook_task": {
        "notebook_path": "/Repos/your-user/sql-identity-resolution/sql/databricks/notebooks/IDR_Run",
        "base_parameters": {
          "RUN_MODE": "INCR",
          "DRY_RUN": "false"
        }
      },
      "existing_cluster_id": "your-cluster-id"
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 * * * ?",
    "timezone_id": "UTC"
  }
}
```

---

## Unity Catalog Integration

For Unity Catalog, update table references:

```python
# Use three-part names
catalog = "main"
schema_meta = f"{catalog}.idr_meta"
schema_work = f"{catalog}.idr_work"
schema_out = f"{catalog}.idr_out"
```

---

## Performance Tuning

1. **Cluster Sizing**: Use memory-optimized instances for large graphs
2. **Caching**: Cache frequently accessed tables
   ```python
   spark.sql("CACHE TABLE idr_work.edges_new")
   ```
3. **Partitioning**: Partition source tables by watermark column
4. **Delta Lake**: Use Delta format for ACID transactions

---

## Monitoring

### View Job Runs

```python
# Query run history
display(spark.sql("""
    SELECT 
        run_id,
        status,
        entities_processed,
        duration_seconds,
        started_at
    FROM idr_out.run_history
    WHERE started_at >= current_date - 7
    ORDER BY started_at DESC
"""))
```

### Set Up Alerts

Use Databricks SQL Alerts to notify on:
- Run failures (`status != 'SUCCESS'`)
- Large clusters exceeding threshold
- Long-running jobs

---

## Next Steps

- [Configuration Guide](../../guides/configuration.md)
- [Dry Run Mode](../../guides/dry-run-mode.md)
- [Metrics & Monitoring](../../guides/metrics-monitoring.md)
