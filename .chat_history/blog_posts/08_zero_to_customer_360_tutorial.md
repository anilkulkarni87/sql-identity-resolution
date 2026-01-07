# From Zero to Customer 360 in 60 Minutes

*Complete hands-on tutorial*

**Tags:** `identity-resolution` `customer-360` `tutorial` `duckdb` `hands-on`

**Reading time:** 10 minutes

---

> **TL;DR:** Clone repo → `pip install duckdb` → `make demo` → View results. Full setup with custom data in 60 minutes. By the end, you'll have working identity resolution with unified customer profiles.

The documentation for sql-identity-resolution can be found [here](https://anilkulkarni87.github.io/sql-identity-resolution/). This tutorial follows the DuckDB path (no cloud account needed) but the concepts apply to all platforms.

## Prerequisites (5 minutes)

What you need:
- Python 3.8+
- Git
- 30 minutes of focused time

```bash
# Check Python
python3 --version

# Check Git
git --version
```

## Step 1: Clone and Setup (5 minutes)

```bash
# Clone repository
git clone https://github.com/anilkulkarni87/sql-identity-resolution.git
cd sql-identity-resolution

# Create virtual environment (optional but recommended)
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install duckdb
```

## Step 2: Run the Demo (5 minutes)

The fastest way to see it working:

```bash
make demo
```

This will:
1. Create a DuckDB database
2. Generate sample data (10,000 records)
3. Run identity resolution
4. Generate a dashboard

Open `demo_results.html` to see the output.

## Step 3: Understand the Schema (10 minutes)

Let's explore what was created:

```bash
python3 -c "
import duckdb
conn = duckdb.connect('output/demo.duckdb')

# See all schemas
print('=== Schemas ===')
print(conn.execute('SHOW SCHEMAS').fetchall())

# Configuration tables
print('\n=== Source Tables Configured ===')
print(conn.execute('SELECT * FROM idr_meta.source_table').fetchdf())

# Output
print('\n=== Cluster Summary ===')
print(conn.execute('''
    SELECT cluster_size, COUNT(*) as num_clusters 
    FROM idr_out.identity_clusters_current 
    GROUP BY cluster_size 
    ORDER BY cluster_size
''').fetchdf())
"
```

Key tables:

| Schema | Table | Purpose |
|--------|-------|---------|
| `idr_meta` | `source_table` | Registered data sources |
| `idr_meta` | `identifier_mapping` | Column → identifier type mapping |
| `idr_meta` | `rule` | Matching rules |
| `idr_out` | `identity_resolved_membership_current` | Entity → cluster mapping |
| `idr_out` | `golden_profile_current` | Unified profiles |

## Step 4: Configure Your Own Sources (15 minutes)

Now let's set up your own data. Create a file `my_setup.py`:

```python
import duckdb

# Create fresh database
conn = duckdb.connect('my_idr.duckdb')

# Run DDL
with open('sql/duckdb/core/00_ddl_all.sql') as f:
    conn.execute(f.read())

# Create sample source tables (replace with your data)
conn.execute("""
CREATE SCHEMA IF NOT EXISTS sources;

-- CRM data
CREATE TABLE sources.crm_customers AS
SELECT 
    'CRM-' || i AS customer_id,
    'user' || i || '@example.com' AS email,
    '555' || LPAD(CAST(i % 10000 AS VARCHAR), 7, '0') AS phone,
    'John' AS first_name,
    'Doe' AS last_name,
    CURRENT_TIMESTAMP AS updated_at
FROM generate_series(1, 1000) s(i);

-- Web signups (some overlap)
CREATE TABLE sources.web_signups AS
SELECT 
    'WEB-' || i AS user_id,
    CASE WHEN i % 3 = 0 
         THEN 'user' || (i/3) || '@example.com'  -- Overlap with CRM
         ELSE 'webuser' || i || '@example.com'
    END AS email,
    CURRENT_TIMESTAMP AS created_at
FROM generate_series(1, 500) s(i);
""")

# Configure sources
conn.execute("""
INSERT INTO idr_meta.source_table VALUES
    ('crm', 'sources.crm_customers', 'PERSON', 'customer_id', 'updated_at', 0, TRUE),
    ('web', 'sources.web_signups', 'PERSON', 'user_id', 'created_at', 0, TRUE);

INSERT INTO idr_meta.source (table_id, source_name, trust_rank, is_active) VALUES
    ('crm', 'CRM Master', 1, TRUE),
    ('web', 'Web Signup', 2, TRUE);

INSERT INTO idr_meta.identifier_mapping VALUES
    ('crm', 'EMAIL', 'email', FALSE),
    ('crm', 'PHONE', 'phone', FALSE),
    ('web', 'EMAIL', 'email', FALSE);
""")

print("Setup complete!")
conn.close()
```

Run it:

```bash
python3 my_setup.py
```

## Step 5: Run Identity Resolution (5 minutes)

```bash
# Dry run first
python3 sql/duckdb/core/idr_run.py --db=my_idr.duckdb --run-mode=FULL --dry-run

# Check results
python3 -c "
import duckdb
conn = duckdb.connect('my_idr.duckdb')
print(conn.execute('SELECT * FROM idr_out.dry_run_summary').fetchdf())
"

# If looks good, commit
python3 sql/duckdb/core/idr_run.py --db=my_idr.duckdb --run-mode=FULL
```

## Step 6: Query Your Customer 360 (10 minutes)

```python
import duckdb
conn = duckdb.connect('my_idr.duckdb')

# How many unique customers?
print("=== Unique Customers ===")
result = conn.execute("""
    SELECT COUNT(DISTINCT resolved_id) as unique_customers
    FROM idr_out.identity_resolved_membership_current
""").fetchone()
print(f"Total unique customers: {result[0]}")

# Cluster size distribution
print("\n=== Cluster Sizes ===")
print(conn.execute("""
    SELECT cluster_size, COUNT(*) as num_clusters
    FROM idr_out.identity_clusters_current
    GROUP BY cluster_size
    ORDER BY cluster_size
""").fetchdf())

# Find merged records
print("\n=== Multi-Source Customers (Top 5) ===")
print(conn.execute("""
    SELECT 
        resolved_id,
        COUNT(*) as records,
        COUNT(DISTINCT SPLIT_PART(entity_key, ':', 1)) as sources
    FROM idr_out.identity_resolved_membership_current
    GROUP BY resolved_id
    HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC
    LIMIT 5
""").fetchdf())

# Golden profiles
print("\n=== Sample Golden Profiles ===")
print(conn.execute("""
    SELECT * FROM idr_out.golden_profile_current LIMIT 5
""").fetchdf())
```

## Step 7: Incremental Processing (5 minutes)

For ongoing runs, use incremental mode:

```bash
# Add new data to sources
python3 -c "
import duckdb
conn = duckdb.connect('my_idr.duckdb')
conn.execute('''
    INSERT INTO sources.crm_customers 
    SELECT 'CRM-NEW-' || i, 'newuser' || i || '@example.com', 
           '555' || LPAD(CAST(1000 + i AS VARCHAR), 7, '0'),
           'Jane', 'Smith', CURRENT_TIMESTAMP
    FROM generate_series(1, 100) s(i)
''')
print('Added 100 new records')
"

# Run incremental
python3 sql/duckdb/core/idr_run.py --db=my_idr.duckdb --run-mode=INCR
```

## Summary

What you've accomplished:

| Step | Time | Outcome |
|------|------|---------|
| Setup | 5 min | Repository cloned, dependencies installed |
| Demo | 5 min | Saw working example |
| Schema | 10 min | Understood data model |
| Configure | 15 min | Your sources registered |
| Run | 5 min | Identity resolution executed |
| Query | 10 min | Customer 360 queries |
| Incremental | 5 min | Ongoing processing setup |

**Total: 55 minutes**

## Next Steps

Now that you have working identity resolution:

1. **Connect real data** - Point to your actual source tables
2. **Set up scheduling** - Cron, Airflow, or dbt
3. **Add monitoring** - Query run_history for health checks
4. **Review documentation** - [Full docs](https://anilkulkarni87.github.io/sql-identity-resolution/)

## Related Resources

If you're building a complete Customer 360 solution:

- [CDP Atlas](https://cdpatlas.vercel.app/) - Interactive guide I built for CDP evaluation
- [CDP Atlas Patterns](https://cdpatlas.vercel.app/patterns) - Architecture patterns for customer data
- [CDP Atlas Builder](https://cdpatlas.vercel.app/builder) - Design your CDP stack visually

## Wrapping Up the Series

This completes the 8-part series on warehouse-native identity resolution:

1. What is Warehouse-Native Identity Resolution?
2. Deterministic vs Probabilistic Matching
3. Building Customer 360 in Snowflake
4. The Hidden Cost of CDPs
5. How Label Propagation Works
6. Dry Run Mode Explained
7. Comparing Open Source Tools
8. From Zero to Customer 360 in 60 Minutes ← You are here

If you found this series helpful, please:
- ⭐ Star the [GitHub repo](https://github.com/anilkulkarni87/sql-identity-resolution)
- Share with colleagues facing identity resolution challenges
- Open issues for questions or feature requests

---

*Thanks for following along! If you like, please share with your friends.*
