# BigQuery Testing Guide

Step-by-step instructions for testing sql-identity-resolution on Google BigQuery.

## Prerequisites

- Google Cloud Platform account
- BigQuery enabled in your project
- Python 3.8+ installed locally
- `google-cloud-bigquery` package

## Step 1: Set Up Authentication

```bash
# Install the BigQuery client
pip install google-cloud-bigquery

# Authenticate (choose one method):

# Option A: Service account key
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

# Option B: User credentials (interactive)
gcloud auth application-default login
```

## Step 2: Create Datasets and Tables

Run the DDL script in BigQuery console or via bq CLI:

**BigQuery Console:**
1. Go to [BigQuery Console](https://console.cloud.google.com/bigquery)
2. Click "Compose New Query"
3. Paste contents of `sql/bigquery/00_ddl_all.sql`
4. Click "Run"

**bq CLI:**
```bash
# Note: BigQuery doesn't support multi-statement scripts directly
# Run statements individually or use the console

bq query --use_legacy_sql=false "CREATE SCHEMA IF NOT EXISTS idr_meta"
bq query --use_legacy_sql=false "CREATE SCHEMA IF NOT EXISTS idr_work"
bq query --use_legacy_sql=false "CREATE SCHEMA IF NOT EXISTS idr_out"

# Create tables (see 00_ddl_all.sql for full DDL)
```

## Step 3: Generate Sample Data

Run the sample data generator in BigQuery console:

```sql
-- Execute: sql/bigquery/idr_sample_data.sql
-- This creates source tables and loads metadata
```

**Or run section by section:**
```sql
-- 1. Create source schemas
CREATE SCHEMA IF NOT EXISTS crm;
CREATE SCHEMA IF NOT EXISTS sales;
CREATE SCHEMA IF NOT EXISTS loyalty;
CREATE SCHEMA IF NOT EXISTS digital;
CREATE SCHEMA IF NOT EXISTS store;

-- 2. Create person pool and tables
-- (See idr_sample_data.sql)
```

**Verify:**
```sql
SELECT COUNT(*) FROM crm.customer;
-- Expected: ~2000

SELECT * FROM idr_meta.source_table;
-- Expected: 5 rows
```

## Step 4: Run Identity Resolution

Use the Python runner:

```bash
cd sql-identity-resolution/sql/bigquery

# Full run (first time)
python idr_run.py --project=your-gcp-project --run-mode=FULL --max-iters=30

# Incremental run (subsequent)
python idr_run.py --project=your-gcp-project --run-mode=INCR --max-iters=30
```

**Expected output:**
```
🚀 Starting IDR run: run_abc123def456
   Mode: FULL, Max iterations: 30
✅ Preflight OK: 5 sources, 13 mappings
📊 Building entities delta...
🔍 Extracting identifiers...
🔗 Building edges...
📈 Building impacted subgraph...
🔄 Running label propagation...
  iter=1 delta_changed=XXX
  iter=2 delta_changed=XXX
  ...
👥 Updating membership...
💾 Updating run state...

✅ IDR run completed!
   Run ID: run_abc123def456
   Entities processed: XXXX
   Edges created: XXXX
   Clusters: XXXX
   LP iterations: X
   Duration: XXs
```

## Step 5: Verify Results

Run in BigQuery console:

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
    WHEN cluster_size = 1 THEN '1_singleton'
    WHEN cluster_size <= 5 THEN '2_small'
    WHEN cluster_size <= 20 THEN '3_medium'
    ELSE '4_large'
  END AS bucket,
  COUNT(*) AS clusters,
  SUM(cluster_size) AS total_entities
FROM idr_out.identity_clusters_current
GROUP BY 1
ORDER BY 1;

-- Sample edges
SELECT * FROM idr_out.identity_edges_current LIMIT 10;

-- Run history
SELECT run_id, status, entities_processed, edges_created, duration_seconds
FROM idr_out.run_history
ORDER BY started_at DESC
LIMIT 5;
```

## Step 6: Test Specific Scenarios

**Test same email linking:**
```sql
-- Find entities linked by email
SELECT 
  e.identifier_value_norm AS email,
  e.left_entity_key,
  e.right_entity_key
FROM idr_out.identity_edges_current e
WHERE e.identifier_type = 'EMAIL'
LIMIT 10;
```

**Test cluster membership:**
```sql
-- Find all entities in a cluster
SELECT m.entity_key, m.resolved_id
FROM idr_out.identity_resolved_membership_current m
WHERE m.resolved_id = (
  SELECT resolved_id FROM idr_out.identity_resolved_membership_current LIMIT 1
);
```

## Step 7: Check Costs

```sql
-- Check bytes processed
SELECT 
  run_id,
  TIMESTAMP_DIFF(ended_at, started_at, SECOND) AS duration_sec,
  entities_processed,
  edges_created
FROM idr_out.run_history
ORDER BY started_at DESC;
```

Use BigQuery's job history to monitor costs per run.

## Step 8: Scale Testing & Benchmarks

For testing with millions of rows:

```bash
cd sql-identity-resolution/sql/bigquery

# Run scale test (100K to 10M)
python idr_scale_test.py --project=your-project --scale=1M --overlap-rate=0.6
```

**Slot Recommendations:**
| Scale | Pricing Model | Expected Duration |
|-------|--------------|-------------------|
| 100K | On-Demand | 2-5 min |
| 1M | On-Demand | 15-30 min |
| 5M | Flat-Rate 2000+ | 30-60 min |

**Benchmark Output:**
```
BENCHMARK RESULTS
=======================================================
Scale: 1M (5,000,000 total rows)
Status: SUCCESS
Duration: 847s
LP Iterations: 8
Edges: 2,340,000
Clusters: 612,000
```

See also: [Cluster Sizing Guide](cluster_sizing.md)

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "Permission denied" | Check IAM roles: `bigquery.dataEditor`, `bigquery.jobUser` |
| "Dataset not found" | Verify project ID in table_fqn |
| "Quota exceeded" | Use on-demand pricing or increase quota |
| Python import error | `pip install google-cloud-bigquery` |

## Cost Optimization

- Use `CREATE OR REPLACE TABLE` instead of `DELETE` + `INSERT`
- Partition large tables by date
- Set table expiration on work tables:
  ```sql
  ALTER TABLE idr_work.entities_delta SET OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR));
  ```

## Cleanup

```sql
-- Drop sample data
DROP SCHEMA IF EXISTS crm CASCADE;
DROP SCHEMA IF EXISTS sales CASCADE;
DROP SCHEMA IF EXISTS loyalty CASCADE;
DROP SCHEMA IF EXISTS digital CASCADE;
DROP SCHEMA IF EXISTS store CASCADE;

-- Clear outputs (keep schemas)
DELETE FROM idr_out.identity_edges_current WHERE TRUE;
DELETE FROM idr_out.identity_resolved_membership_current WHERE TRUE;
DELETE FROM idr_out.identity_clusters_current WHERE TRUE;
DELETE FROM idr_out.golden_profile_current WHERE TRUE;
DELETE FROM idr_out.run_history WHERE TRUE;
```
