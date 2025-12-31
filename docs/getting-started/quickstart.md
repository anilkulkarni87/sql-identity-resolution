# Quick Start

Get SQL Identity Resolution running in under 5 minutes.

---

## ⚡ 60-Second Demo (Fastest Way)

Want to see it work immediately? Run one command:

```bash
git clone https://github.com/anilkulkarni87/sql-identity-resolution.git
cd sql-identity-resolution
make demo
```

This will:

1. Create a DuckDB database
2. Generate 10K sample customers with shared identifiers
3. Run a dry run (preview changes)
4. Run a live IDR pass (create clusters)
5. Generate an HTML dashboard

Open `demo_results.html` to explore the results!

---

## Full Setup Guide

### Prerequisites

- Python 3.9+ (for DuckDB/BigQuery CLI)
- Access to your target platform (Snowflake account, GCP project, Databricks workspace)

---

### Step 1: Clone the Repository

```bash
git clone https://github.com/anilkulkarni87/sql-identity-resolution.git
cd sql-identity-resolution
```

---

### Step 2: Choose Your Platform

=== "DuckDB (Local)"

    **Best for**: Development, testing, small datasets (<10M records)

    ```bash
    # Install DuckDB
    pip install duckdb
    
    # Create database with schema
    duckdb idr.duckdb < sql/duckdb/core/00_ddl_all.sql
    ```

=== "Snowflake"

    **Best for**: Enterprise data warehouses, existing Snowflake users

    ```sql
    -- Run in Snowflake worksheet
    -- Execute sql/snowflake/core/00_ddl_all.sql
    
    -- Verify schemas created
    SHOW SCHEMAS IN DATABASE your_database;
    ```

=== "BigQuery"

    **Best for**: GCP users, serverless preference

    ```bash
    # Set credentials
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
    
    # Create datasets
    bq mk --dataset your_project:idr_meta
    bq mk --dataset your_project:idr_work
    bq mk --dataset your_project:idr_out
    
    # Run DDL
    bq query --use_legacy_sql=false < sql/bigquery/core/00_ddl_all.sql
    ```

=== "Databricks"

    **Best for**: Existing Databricks/Spark users, large-scale processing

    ```
    1. Import notebooks from sql/databricks/core/ and sql/databricks/ops/
    2. Run IDR_QuickStart.py notebook
    3. This creates schemas and sample data
    ```

---

## Step 3: Configure Your Sources

### 3.1 Register Source Tables

```sql
INSERT INTO idr_meta.source_table (
  table_id, 
  table_fqn, 
  entity_type, 
  entity_key_expr, 
  watermark_column, 
  watermark_lookback_minutes, 
  is_active
) VALUES (
  'customers',           -- Unique identifier for this source
  'crm.customers',       -- Fully qualified table name
  'PERSON',              -- Entity type
  'customer_id',         -- Expression for entity key
  'updated_at',          -- Watermark column for incremental
  0,                     -- Lookback minutes
  TRUE                   -- Is active
);
```

### 3.2 Define Matching Rules

```sql
INSERT INTO idr_meta.rule (
  rule_id, 
  identifier_type, 
  priority, 
  is_active,
  max_group_size
) VALUES 
  ('email_exact', 'EMAIL', 1, TRUE, 10000),
  ('phone_exact', 'PHONE', 2, TRUE, 5000),
  ('loyalty_id', 'LOYALTY', 3, TRUE, 1000);
```

### 3.3 Map Identifiers

```sql
INSERT INTO idr_meta.identifier_mapping (
  table_id, 
  identifier_type, 
  column_expr, 
  requires_normalization
) VALUES 
  ('customers', 'EMAIL', 'email', TRUE),
  ('customers', 'PHONE', 'phone', TRUE);
```

---

## Step 4: Run a Dry Run

!!! warning "Always Dry Run First"
    Before committing changes, preview the impact with a dry run.

=== "DuckDB"

    ```bash
    python sql/duckdb/core/idr_run.py \
      --db=idr.duckdb \
      --run-mode=FULL \
      --dry-run
    ```

=== "Snowflake"

    ```sql
    CALL idr_run('FULL', 30, TRUE);  -- TRUE = dry run
    ```

=== "BigQuery"

    ```bash
    python sql/bigquery/core/idr_run.py \
      --project=your-project \
      --run-mode=FULL \
      --dry-run
    ```

=== "Databricks"

    Set widget `DRY_RUN` to `true`, then run the notebook.

### Dry Run Output

```
============================================================
DRY RUN SUMMARY (No changes committed)
============================================================
Run ID:          dry_run_abc123
Mode:            FULL (DRY RUN)
Duration:        5s
Status:          DRY_RUN_COMPLETE

IMPACT PREVIEW:
  New Entities:      1,234
  Moved Entities:    89
  Edges Would Create: 5,678
  Largest Cluster:   523 entities

REVIEW QUERIES:
  → All changes:  SELECT * FROM idr_out.dry_run_results WHERE run_id = 'dry_run_abc123'
  → Moved only:   SELECT * FROM idr_out.dry_run_results WHERE ... AND change_type = 'MOVED'

⚠️  THIS WAS A DRY RUN - NO CHANGES COMMITTED
============================================================
```

---

## Step 5: Review and Commit

After reviewing the dry run results:

=== "DuckDB"

    ```bash
    # Remove --dry-run to commit
    python sql/duckdb/core/idr_run.py \
      --db=idr.duckdb \
      --run-mode=FULL
    ```

=== "Snowflake"

    ```sql
    CALL idr_run('FULL', 30, FALSE);  -- FALSE = live run
    ```

=== "BigQuery"

    ```bash
    python sql/bigquery/core/idr_run.py \
      --project=your-project \
      --run-mode=FULL
    ```

=== "Databricks"

    Set widget `DRY_RUN` to `false`, then run.

---

## Step 6: Query Results

```sql
-- View cluster membership
SELECT 
  entity_key,
  resolved_id,
  updated_ts
FROM idr_out.identity_resolved_membership_current
LIMIT 100;

-- View cluster sizes
SELECT 
  resolved_id,
  cluster_size
FROM idr_out.identity_clusters_current
ORDER BY cluster_size DESC
LIMIT 20;

-- View golden profiles
SELECT * FROM idr_out.golden_profile_current LIMIT 10;
```

---

## Next Steps

- [Configuration Guide](../guides/configuration.md) - Advanced rule setup
- [Dry Run Mode](../guides/dry-run-mode.md) - Understanding previews
- [Metrics & Monitoring](../guides/metrics-monitoring.md) - Observability setup
- [Production Hardening](../guides/production-hardening.md) - Data quality controls
