# Production Deployment: Databricks

This guide details the exact steps to deploy SQL Identity Resolution (IDR) to a production Databricks environment.

---

## Prerequisites

- **Databricks Workspace**: Azure Databricks, AWS Databricks, or GCP Databricks.
- **Unity Catalog (Recommended)**: For best governance, though Hive Metastore is supported.
- **SQL Warehouse**: Required for running SQL workloads.

---

## Step 1: Schema Setup

### 1.1 Import Notebooks
Import the following notebooks into your workspace (e.g., `/Shared/IDR/core/`):
- `sql/databricks/core/00_ddl_all.sql` (can be run as a notebook or query)
- `sql/databricks/core/IDR_Run.py`

### 1.2 Create Schemas & Tables
Run the DDL SQL to set up the environment.

```sql
%sql
-- In a SQL Notebook or SQL Editor
CREATE SCHEMA IF NOT EXISTS idr_meta;
CREATE SCHEMA IF NOT EXISTS idr_work;
CREATE SCHEMA IF NOT EXISTS idr_out;

-- Run the contents of 00_ddl_all.sql here or %run it
```

---

## Step 2: Configuration

Create a `production.yaml` file defining your rules and sources.

**Example `production.yaml`:**
```yaml
rules:
  - rule_id: email_exact
    identifier_type: EMAIL
    priority: 1
    settings:
      canonicalize: LOWERCASE

sources:
  - table_id: delta_customers
    table_fqn: catalog.schema.customers
    entity_key_expr: id
    identifiers:
      - type: EMAIL
        expr: email
```

---

## Step 3: Metadata Loading

Use the `load_metadata.py` tool to push your configuration. You can run this from your laptop or a CI/CD pipeline, connecting via Databricks SQL Connector.

```bash
export DATABRICKS_HOST="https://adb-1234.5.azuredatabricks.net"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/..."
export DATABRICKS_TOKEN="dapi..."

python tools/load_metadata.py \
  --platform=databricks \
  --config=production.yaml
```

---

## Step 4: Execution & Scheduling

The IDR process is a PySpark notebook (`IDR_Run.py`).

### Option A: Databricks Workflows (Recommended)

1.  Create a new **Job**.
2.  Task type: **Notebook**.
3.  Path: `/Shared/IDR/core/IDR_Run`.
4.  Parameters:
    - `RUN_MODE`: `FULL` (or `INCR`)
    - `MAX_ITERS`: `30`
    - `DRY_RUN`: `false`

Schedule this job to run daily or hourly.

### Option B: dbt

If using dbt-databricks, you can wrap the logic in a dbt model or use a pre-hook to call the notebook if supported, though Databricks Workflows is the native path.

---

## Step 5: Monitoring

Monitor the pipeline using the `idr_out` tables.

**Check Run History:**
```sql
SELECT run_id, status, duration_seconds 
FROM idr_out.run_history 
ORDER BY started_at DESC;
```
