# Snowflake Setup

Snowflake is ideal for **enterprise data warehouses** with existing Snowflake infrastructure.

---

## Prerequisites

- Snowflake account with ACCOUNTADMIN or CREATE SCHEMA privileges
- Snowflake worksheet or SnowSQL CLI

---

## Create Schemas and Tables

Run the DDL script in a Snowflake worksheet:

```sql
-- Run contents of sql/snowflake/00_ddl_all.sql

-- Verify schemas created
SHOW SCHEMAS;
```

This creates:

| Schema | Purpose |
|--------|---------|
| `IDR_META` | Configuration tables |
| `IDR_WORK` | Transient processing tables |
| `IDR_OUT` | Output tables |

---

## Create Stored Procedure

The IDR runner is implemented as a JavaScript stored procedure:

```sql
-- Run contents of sql/snowflake/IDR_Run.sql
-- This creates the idr_run() procedure
```

---

## Configure Sources

```sql
-- Register source table
INSERT INTO idr_meta.source_table VALUES
  ('customers', 'SALES.CUSTOMERS', 'PERSON', 'CUSTOMER_ID', 'UPDATED_AT', 0, TRUE);

-- Define matching rules
INSERT INTO idr_meta.rule VALUES
  ('email_exact', 'EMAIL', 1, TRUE, 10000),
  ('phone_exact', 'PHONE', 2, TRUE, 5000);

-- Map identifiers
INSERT INTO idr_meta.identifier_mapping VALUES
  ('customers', 'EMAIL', 'EMAIL', TRUE),
  ('customers', 'PHONE', 'PHONE', TRUE);
```

---

## Run IDR

### Procedure Signature

```sql
CALL idr_run(
  RUN_MODE,    -- VARCHAR: 'FULL' or 'INCR'
  MAX_ITERS,   -- INTEGER: Max label propagation iterations
  DRY_RUN      -- BOOLEAN: TRUE = preview, FALSE = commit
);
```

### Dry Run (Preview)

```sql
CALL idr_run('FULL', 30, TRUE);
```

**Output:**
```
DRY_RUN_COMPLETE: run_id=dry_run_abc123, new_entities=1234, moved_entities=89, duration=5s | DRY RUN - NO CHANGES COMMITTED
```

### Live Run

```sql
CALL idr_run('FULL', 30, FALSE);
```

### Incremental Run

```sql
CALL idr_run('INCR', 30, FALSE);
```

---

## Review Dry Run Results

```sql
-- View proposed changes
SELECT entity_key, current_resolved_id, proposed_resolved_id, change_type
FROM idr_out.dry_run_results
WHERE run_id = 'dry_run_abc123'
ORDER BY change_type;

-- Summary statistics
SELECT * FROM idr_out.dry_run_summary
WHERE run_id = 'dry_run_abc123';
```

---

## Verify Results

```sql
-- Run history
SELECT run_id, status, entities_processed, duration_seconds, warnings
FROM idr_out.run_history
ORDER BY started_at DESC
LIMIT 10;

-- Cluster distribution
SELECT cluster_size, COUNT(*) as count
FROM idr_out.identity_clusters_current
GROUP BY cluster_size
ORDER BY cluster_size;

-- Metrics
SELECT metric_name, metric_value, recorded_at
FROM idr_out.metrics_export
WHERE run_id = 'run_abc123';
```

---

## Scheduling with Snowflake Tasks

```sql
-- Create a task to run every hour
CREATE OR REPLACE TASK idr_hourly_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '60 MINUTE'
AS
  CALL idr_run('INCR', 30, FALSE);

-- Enable the task
ALTER TASK idr_hourly_task RESUME;

-- Check task history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'IDR_HOURLY_TASK'
ORDER BY SCHEDULED_TIME DESC;
```

---

## Security Best Practices

1. **Use a Service Role**: Create a dedicated role for IDR execution
2. **Grant Minimal Permissions**: Only SELECT on source tables, full access to IDR schemas
3. **Enable Query Tagging**: Add query tags for cost attribution

```sql
-- Create dedicated role
CREATE ROLE IDR_EXECUTOR;

-- Grant required permissions
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE IDR_EXECUTOR;
GRANT USAGE ON DATABASE ANALYTICS TO ROLE IDR_EXECUTOR;
GRANT ALL ON SCHEMA IDR_META TO ROLE IDR_EXECUTOR;
GRANT ALL ON SCHEMA IDR_WORK TO ROLE IDR_EXECUTOR;
GRANT ALL ON SCHEMA IDR_OUT TO ROLE IDR_EXECUTOR;
GRANT SELECT ON ALL TABLES IN SCHEMA SALES TO ROLE IDR_EXECUTOR;
```

---

## Next Steps

- [Configuration Guide](../../guides/configuration.md)
- [Dry Run Mode](../../guides/dry-run-mode.md)
- [Scheduling](../../deployment/scheduling.md)
