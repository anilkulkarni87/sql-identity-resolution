# Snowflake Testing Guide

Step-by-step instructions for testing sql-identity-resolution on Snowflake.

## Prerequisites

- Snowflake account (Standard tier or higher)
- Role with CREATE SCHEMA, CREATE TABLE, CREATE PROCEDURE privileges
- Warehouse to execute queries

## Step 1: Set Up Your Environment

```sql
-- Use your preferred database
USE DATABASE your_database;

-- Set warehouse
USE WAREHOUSE your_warehouse;
```

## Step 2: Create Schemas and Tables

Run the DDL script:

```sql
-- Copy and execute the contents of:
-- sql/snowflake/00_ddl_all.sql

-- Or run directly:
CREATE SCHEMA IF NOT EXISTS idr_meta;
CREATE SCHEMA IF NOT EXISTS idr_work;
CREATE SCHEMA IF NOT EXISTS idr_out;

-- Create metadata tables
CREATE TABLE IF NOT EXISTS idr_meta.source_table (
  table_id VARCHAR,
  table_fqn VARCHAR,
  entity_type VARCHAR,
  entity_key_expr VARCHAR,
  watermark_column VARCHAR,
  watermark_lookback_minutes INTEGER,
  is_active BOOLEAN
);

-- ... (see 00_ddl_all.sql for complete DDL)
```

## Step 3: Create the Stored Procedure

Run the stored procedure creation script:

```sql
-- Execute: sql/snowflake/IDR_Run.sql
-- This creates the idr_run() procedure
```

## Step 4: Generate Sample Data

Run the sample data generator:

```sql
-- Execute: sql/snowflake/IDR_SampleData_Generate.sql

-- Configure rows (optional):
SET N_ROWS = 2000;

-- Then run the script
```

**Expected output:**
- 5 source schemas created (crm, sales, loyalty, digital, store)
- 5 source tables with sample data
- Metadata tables populated

**Verify:**
```sql
SELECT COUNT(*) FROM crm.customer;
-- Expected: 2000

SELECT COUNT(*) FROM idr_meta.source_table;
-- Expected: 5
```

## Step 5: Run Identity Resolution

Execute the stored procedure:

```sql
-- Full run (first time)
CALL idr_run('FULL', 30);

-- Incremental run (subsequent)
CALL idr_run('INCR', 30);
```

**Expected output:**
```
SUCCESS: run_id=run_abc123, entities=XXXX, edges=XXXX, iterations=X, duration=Xs
```

## Step 6: Verify Results

```sql
-- Check edges
SELECT COUNT(*) AS edge_count FROM idr_out.identity_edges_current;
-- Expected: > 0

-- Check membership
SELECT COUNT(*) AS member_count FROM idr_out.identity_resolved_membership_current;
-- Expected: > 0

-- Cluster distribution
SELECT 
  CASE 
    WHEN cluster_size = 1 THEN '1 (singleton)'
    WHEN cluster_size <= 5 THEN '2-5'
    WHEN cluster_size <= 20 THEN '6-20'
    ELSE '20+'
  END AS size_bucket,
  COUNT(*) AS clusters
FROM idr_out.identity_clusters_current
GROUP BY 1
ORDER BY 1;

-- Sample resolved identities
SELECT 
  e.left_entity_key,
  e.right_entity_key,
  e.identifier_type,
  e.identifier_value_norm
FROM idr_out.identity_edges_current e
LIMIT 10;

-- Run history
SELECT run_id, run_mode, status, entities_processed, edges_created, duration_seconds
FROM idr_out.run_history
ORDER BY started_at DESC
LIMIT 5;
```

## Step 7: Test Incremental Processing

```sql
-- Add new records to a source
INSERT INTO crm.customer (customer_id, first_name, last_name, email, phone, loyalty_id, rec_create_dt, rec_update_dt)
VALUES ('C999999', 'Test', 'User', 'test.user@example.com', '9991234567', 'L999999', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Run incremental
CALL idr_run('INCR', 30);

-- Verify new entity was processed
SELECT * FROM idr_out.identity_resolved_membership_current
WHERE entity_key LIKE 'customer:C999999';
```

## Step 8: Check Monitoring Views

```sql
-- Create monitoring views (run once)
-- Execute: sql/common/monitoring_views.sql (adapt for Snowflake if needed)

-- Then query:
SELECT * FROM idr_out.v_run_summary;
SELECT * FROM idr_out.v_cluster_health;
```

## Step 9: Scale Testing & Benchmarks

For testing with millions of rows:

**Run:** `sql/snowflake/IDR_ScaleTest.sql`

```sql
-- Configure scale
SET N_ROWS = 1000000;  -- 1M per table
SET OVERLAP_RATE = 0.6;

-- Run the script
-- Then call:
CALL idr_run('FULL', 50);
```

**Warehouse Sizing:**
| Scale | Warehouse Size | Expected Duration |
|-------|---------------|-------------------|
| 100K | X-Small | 2-5 min |
| 1M | Medium | 15-30 min |
| 5M | Large | 30-60 min |

**Benchmark Results:**
```sql
SELECT run_id, status, entities_processed, edges_created, 
       duration_seconds, lp_iterations
FROM idr_out.run_history
ORDER BY started_at DESC
LIMIT 1;
```

See also: [Cluster Sizing Guide](cluster_sizing.md)

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "No active source tables" | Check `is_active = TRUE` in source_table |
| "Table not found" | Verify table_fqn format: `database.schema.table` |
| "Procedure failed" | Check ACCOUNTADMIN role or grant EXECUTE perms |
| "Label propagation slow" | Reduce MAX_ITERS or check warehouse size |

## Performance Tips

```sql
-- Use larger warehouse for > 100K rows
ALTER WAREHOUSE your_warehouse SET WAREHOUSE_SIZE = 'LARGE';

-- Check query profile for slow steps
-- Use Snowflake Query History to analyze execution
```

## Cleanup

```sql
-- Remove sample data (keep schemas)
DROP TABLE IF EXISTS crm.customer;
DROP TABLE IF EXISTS sales.transactions;
DROP TABLE IF EXISTS loyalty.loyalty_accounts;
DROP TABLE IF EXISTS digital.web_events;
DROP TABLE IF EXISTS store.store_visits;

-- Clear metadata
TRUNCATE TABLE idr_meta.source_table;
TRUNCATE TABLE idr_meta.source;
TRUNCATE TABLE idr_meta.rule;
TRUNCATE TABLE idr_meta.identifier_mapping;

-- Clear outputs
TRUNCATE TABLE idr_out.identity_edges_current;
TRUNCATE TABLE idr_out.identity_resolved_membership_current;
TRUNCATE TABLE idr_out.identity_clusters_current;
TRUNCATE TABLE idr_out.golden_profile_current;
```
