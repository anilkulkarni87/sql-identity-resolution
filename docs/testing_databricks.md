# Databricks Testing Guide

Step-by-step instructions for testing sql-identity-resolution on Databricks.

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Runtime 13.0 or higher
- A catalog you can write to (e.g., `main`)

## Step 1: Clone or Upload the Repository

**Option A: Git Integration**
```
Repos → Add Repo → https://github.com/anilkulkarni87/sql-identity-resolution
```

**Option B: Manual Upload**
Upload the `sql/` folder to your Databricks workspace.

## Step 2: Create Schemas and Tables

Run the DDL scripts in a SQL notebook or Databricks SQL:

```sql
-- Run these in order
%run ./sql/common/00_ddl_meta.sql
%run ./sql/common/01_ddl_outputs.sql
%run ./sql/common/02_ddl_observability.sql
```

Or execute directly:
```sql
CREATE SCHEMA IF NOT EXISTS idr_meta;
CREATE SCHEMA IF NOT EXISTS idr_work;
CREATE SCHEMA IF NOT EXISTS idr_out;
-- ... (see 00_ddl_meta.sql for full DDL)
```

## Step 3: Generate Sample Data

Open and run: `sql/databricks/notebooks/IDR_SampleData_Generate.py`

**Parameters:**
| Parameter | Default | Description |
|-----------|---------|-------------|
| `CATALOG` | `main` | Your Unity Catalog |
| `N_CUSTOMERS` | `5000` | Rows per table |
| `SEED` | `42` | Random seed |

**Expected output:**
```
✅ Created 5 source tables:
   - main.crm.customer (5000 rows)
   - main.sales.transactions (5000 rows)
   - main.loyalty.loyalty_accounts (5000 rows)
   - main.digital.web_events (5000 rows)
   - main.store.store_visits (5000 rows)
```

## Step 4: Load Metadata

Open and run: `sql/databricks/notebooks/IDR_LoadMetadata_Simple.py`

**Parameters:**
| Parameter | Default | Description |
|-----------|---------|-------------|
| `META_ROOT` | Auto-detected | Path to `metadata_samples/` |

**Expected output:**
```
✅ Loaded idr_meta.source_table (5 rows)
✅ Loaded idr_meta.source (5 rows)
✅ Loaded idr_meta.rule (3 rows)
✅ Loaded idr_meta.identifier_mapping (13 rows)
...
```

## Step 5: Validate Metadata (Optional)

Open and run: `sql/databricks/notebooks/IDR_ValidateMetadata.py`

**Expected output:**
```
✅ VALIDATION PASSED - Ready to run IDR
```

## Step 6: Run Identity Resolution

Open and run: `sql/databricks/notebooks/IDR_Run.py`

**Parameters:**
| Parameter | Default | Description |
|-----------|---------|-------------|
| `RUN_MODE` | `INCR` | `FULL` for first run, `INCR` for subsequent |
| `MAX_ITERS` | `30` | Label propagation iterations |

**Expected output:**
```
✅ Preflight OK
📊 Run tracking started
...
iter=1 delta_changed=XXX
iter=2 delta_changed=XXX
...
✅ IDR run completed
   entities_delta: XXXX
   edges_new: XXXX
   membership_current: XXXX
```

## Step 7: Verify Results

Run these queries to verify:

```sql
-- Check edges were created
SELECT COUNT(*) AS edge_count FROM idr_out.identity_edges_current;
-- Expected: > 0

-- Check membership
SELECT COUNT(*) AS member_count FROM idr_out.identity_resolved_membership_current;
-- Expected: > 0

-- Check cluster distribution
SELECT 
  CASE 
    WHEN cluster_size = 1 THEN 'singleton'
    WHEN cluster_size <= 5 THEN '2-5'
    ELSE '6+'
  END AS size_bucket,
  COUNT(*) AS cluster_count
FROM idr_out.identity_clusters_current
GROUP BY 1;

-- Check golden profiles
SELECT * FROM idr_out.golden_profile_current LIMIT 10;

-- Check run history
SELECT run_id, status, entities_processed, edges_created, duration_seconds
FROM idr_out.run_history
ORDER BY started_at DESC
LIMIT 5;
```

## Step 8: Run Integration Tests (Optional)

Open and run: `tests/run_tests.py`

1. Run the notebook to set up test fixtures
2. Run `IDR_Run.py` with `RUN_MODE=FULL`
3. Return to `run_tests.py` and execute the assertions cell

## Step 9: Scale Testing & Benchmarks

For testing with millions of rows, use the scale test notebook:

**Run:** `sql/databricks/notebooks/IDR_ScaleTest.py`

**Parameters:**
| Parameter | Options | Description |
|-----------|---------|-------------|
| `SCALE` | 100K, 500K, 1M, 5M, 10M | Rows per table |
| `OVERLAP_RATE` | 0.3 - 0.7 | Identity overlap (higher = more matches) |

**Cluster Sizing:**
| Scale | Workers | Node Size | Expected Duration |
|-------|---------|-----------|-------------------|
| 100K | 2-4 | m5.xlarge | 2-5 min |
| 1M | 8-16 | m5.2xlarge | 15-30 min |
| 5M | 16-32 | m5.4xlarge | 30-60 min |

**Benchmark Output:**
```
BENCHMARK RESULTS
=======================================================
Scale: 1M (5,000,000 total rows)
Run ID: run_abc123
Status: SUCCESS
Duration: 847s (14.1 min)
LP Iterations: 8
Outputs:
  Entities processed: 5,000,000
  Edges created: 2,340,000
  Clusters: 612,000
```

**Interpreting Results:**
- **Duration** — Compare across runs for regression testing
- **LP Iterations** — Should converge < 30; if not, check for super clusters
- **Cluster distribution** — Giant clusters (1000+) may indicate data quality issues

See also: [Cluster Sizing Guide](cluster_sizing.md)

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "Missing metadata tables" | Run `00_ddl_meta.sql` first |
| "Source tables not found" | Check catalog name matches |
| "Label propagation did not converge" | Increase `MAX_ITERS` or check for data issues |
| "No active source tables" | Check `is_active=true` in `idr_meta.source_table` |

## Quick Start Alternative

For a one-click demo, run: `sql/databricks/notebooks/IDR_QuickStart.py`

This combines all steps above into a single notebook.

