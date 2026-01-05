# DuckDB Testing Guide

Step-by-step instructions for testing sql-identity-resolution on DuckDB.

DuckDB is perfect for **local testing** and **CI/CD pipelines** — no cloud setup required.

## Prerequisites

- Python 3.8+
- `duckdb` package

```bash
pip install duckdb
```

## Step 1: Create the Database and Schemas

```bash
cd sql-identity-resolution/sql/duckdb

# Create database and run DDL
duckdb idr.duckdb < 00_ddl_all.sql
```

**Or in Python:**
```python
import duckdb
con = duckdb.connect('idr.duckdb')
con.execute(open('00_ddl_all.sql').read())
con.close()
```

**Verify:**
```bash
duckdb idr.duckdb -c "SELECT name FROM sqlite_master WHERE type='table'"
```

## Step 2: Generate Sample Data

```bash
python idr_sample_data.py --db=idr.duckdb --rows=2000 --seed=42
```

**Expected output:**
```
🦆 Generating sample data in idr.duckdb
   Rows per table: 2000
📊 Creating crm.customer...
📊 Creating sales.transactions...
📊 Creating loyalty.loyalty_accounts...
📊 Creating digital.web_events...
📊 Creating store.store_visits...
📋 Loading metadata...

✅ Sample data created!
   Database: idr.duckdb
   Tables: 5 source tables with 2000 rows each
   Metadata: Loaded all configuration
```

**Verify:**
```bash
duckdb idr.duckdb -c "SELECT COUNT(*) FROM crm.customer"
# Expected: 2000

duckdb idr.duckdb -c "SELECT * FROM idr_meta.source_table"
# Expected: 5 rows
```

## Step 3: Run Identity Resolution

```bash
python idr_run.py --db=idr.duckdb --run-mode=FULL --max-iters=30
```

**Expected output:**
```
🦆 Starting DuckDB IDR run: run_abc123def456
   Database: idr.duckdb
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

✅ DuckDB IDR run completed!
   Run ID: run_abc123def456
   Database: idr.duckdb
   Entities processed: XXXX
   Edges created: XXXX
   Clusters: XXXX
   LP iterations: X
   Duration: Xs
```

## Step 4: Verify Results

```bash
# Check edge count
duckdb idr.duckdb -c "SELECT COUNT(*) FROM idr_out.identity_edges_current"
# Expected: > 0

# Check membership
duckdb idr.duckdb -c "SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current"
# Expected: > 0

# Cluster distribution
duckdb idr.duckdb -c "
SELECT 
  CASE 
    WHEN cluster_size = 1 THEN '1_singleton'
    WHEN cluster_size <= 5 THEN '2_small'
    ELSE '3_large'
  END AS bucket,
  COUNT(*) AS clusters
FROM idr_out.identity_clusters_current
GROUP BY 1
ORDER BY 1
"

# Sample edges
duckdb idr.duckdb -c "SELECT * FROM idr_out.identity_edges_current LIMIT 5"

# Run history
duckdb idr.duckdb -c "SELECT run_id, status, entities_processed, edges_created, duration_seconds FROM idr_out.run_history"
```

## Step 5: Interactive Exploration

```bash
# Start interactive shell
duckdb idr.duckdb

# Run queries
D SELECT COUNT(*) FROM idr_out.identity_edges_current;
D SELECT * FROM idr_out.golden_profile_current LIMIT 10;
D .quit
```

**Or in Python:**
```python
import duckdb

con = duckdb.connect('idr.duckdb')

# Query results
edges = con.execute("SELECT COUNT(*) FROM idr_out.identity_edges_current").fetchone()[0]
print(f"Edges: {edges}")

clusters = con.execute("""
    SELECT cluster_size, COUNT(*) as count 
    FROM idr_out.identity_clusters_current 
    GROUP BY 1 ORDER BY 1
""").fetchall()
print("Cluster distribution:", clusters)

con.close()
```

## Step 6: Test Incremental Processing

```python
import duckdb

con = duckdb.connect('idr.duckdb')

# Add new record
con.execute("""
INSERT INTO crm.customer VALUES 
('C999999', 'Test', 'User', 'test.user@example.com', '9991234567', 'L999999',
 CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
""")

con.close()
```

```bash
# Run incremental
python idr_run.py --db=idr.duckdb --run-mode=INCR

# Verify new entity
duckdb idr.duckdb -c "SELECT * FROM idr_out.identity_resolved_membership_current WHERE entity_key LIKE 'customer:C999999'"
```

## Step 7: Run Unit Tests

```bash
# Create test database
duckdb test_idr.duckdb < 00_ddl_all.sql

# Generate minimal test data
python idr_sample_data.py --db=test_idr.duckdb --rows=100

# Run and verify
python idr_run.py --db=test_idr.duckdb --run-mode=FULL

# Check results
duckdb test_idr.duckdb -c "SELECT COUNT(*) > 0 AS has_edges FROM idr_out.identity_edges_current"
# Expected: true
```

## Step 8: Export Results

```bash
# Export to CSV
duckdb idr.duckdb -c "COPY idr_out.identity_edges_current TO 'edges.csv' (HEADER, DELIMITER ',')"
duckdb idr.duckdb -c "COPY idr_out.golden_profile_current TO 'profiles.csv' (HEADER, DELIMITER ',')"

# Export to Parquet
duckdb idr.duckdb -c "COPY idr_out.identity_edges_current TO 'edges.parquet' (FORMAT PARQUET)"
```

## CI/CD Integration Example

**GitHub Actions:**
```yaml
name: Test IDR

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install dependencies
        run: pip install duckdb
      
      - name: Create database
        run: duckdb test.duckdb < sql/duckdb/00_ddl_all.sql
      
      - name: Generate test data
        run: python sql/duckdb/idr_sample_data.py --db=test.duckdb --rows=500
      
      - name: Run IDR
        run: python sql/duckdb/idr_run.py --db=test.duckdb --run-mode=FULL
      
      - name: Verify results
        run: |
          duckdb test.duckdb -c "SELECT CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END FROM idr_out.identity_edges_current"
```

## Step 9: Scale Testing & Benchmarks

For performance testing with larger datasets:

```bash
cd sql-identity-resolution/sql/duckdb

# Run scale test (100K to 2M for DuckDB)
python sql/duckdb/testing/idr_scale_test.py --db=scale_test.duckdb --scale=500K --overlap-rate=0.6
```

**RAM Requirements:**
| Scale | RAM Needed | Expected Duration |
|-------|-----------|-------------------|
| 100K | 2 GB | 30s - 2 min |
| 500K | 4 GB | 2-5 min |
| 1M | 8 GB | 5-15 min |

> **Note:** DuckDB is best for < 10M rows. For larger scale, use cloud platforms.

**Benchmark Output:**
```
BENCHMARK RESULTS
=======================================================
Scale: 500K (2,500,000 total rows)
Status: SUCCESS
Duration: 127s
LP Iterations: 6
Edges: 580,000
Clusters: 153,000
```

See also: [Cluster Sizing Guide](cluster_sizing.md)

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "No such table" | Run `00_ddl_all.sql` first |
| "Database locked" | Close other connections to the .duckdb file |
| Import error | `pip install duckdb` |
| Slow on large data | DuckDB is best for < 10M rows locally |

## Cleanup

```bash
# Delete database file
rm idr.duckdb

# Or clear tables
duckdb idr.duckdb -c "
DELETE FROM idr_out.identity_edges_current;
DELETE FROM idr_out.identity_resolved_membership_current;
DELETE FROM idr_out.identity_clusters_current;
DELETE FROM idr_out.run_history;
"
```

## Why DuckDB?

- **Zero setup**: No server, no cloud, just a file
- **Fast**: Columnar engine, vectorized execution
- **Portable**: Single `.duckdb` file
- **Great for CI/CD**: Quick tests in GitHub Actions
- **Python-native**: Easy integration
