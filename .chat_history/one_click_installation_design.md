# One-Click Installation - Design Document

> **Status**: Proposed  
> **Author**: Technical Architect  
> **Date**: 2026-01-05  
> **Priority**: High (SMB adoption blocker)

---

## Problem Statement

Current installation requires multiple manual steps:

| Platform | Pain Points |
|----------|-------------|
| **Databricks** | `REPO_ROOT` widget must be set manually if auto-detection fails |
| **Snowflake** | Must run `00_ddl_all.sql` before first `idr_run` |
| **BigQuery** | Must run DDL, create datasets manually, set up credentials |
| **DuckDB** | Best experience - `make demo` works, but still needs `00_ddl_all.sql` |

**Target:** `python idr_run.py --project=X --setup` creates everything needed.

---

## Proposed Solutions

### Solution 1: `--setup` Flag (Recommended)

Add a `--setup` flag to each platform's runner that:
1. Creates schemas if they don't exist
2. Creates all tables if they don't exist
3. Seeds default rules (EMAIL, PHONE)
4. Validates the setup
5. Then continues with the normal run (or exits if `--setup-only`)

```bash
# First time setup + run
python idr_run.py --project=my-project --run-mode=FULL --setup

# Setup only (no data processing)
python idr_run.py --project=my-project --setup-only

# After setup, normal runs work
python idr_run.py --project=my-project --run-mode=INCR
```

**Implementation:**

```python
# Add to argument parser
parser.add_argument('--setup', action='store_true', help='Create schemas and tables if missing')
parser.add_argument('--setup-only', action='store_true', help='Only run setup, do not process data')

# Add setup function
def run_setup():
    """Create all required schemas and tables."""
    print("🔧 Running setup...")
    
    # Read DDL from bundled file or embedded string
    ddl_path = os.path.join(os.path.dirname(__file__), '00_ddl_all.sql')
    if os.path.exists(ddl_path):
        with open(ddl_path) as f:
            ddl_statements = f.read().split(';')
    else:
        ddl_statements = EMBEDDED_DDL.split(';')  # Fallback
    
    for stmt in ddl_statements:
        if stmt.strip():
            try:
                q(stmt)
            except Exception as e:
                if 'already exists' not in str(e).lower():
                    raise
    
    # Seed default rules
    seed_default_rules()
    
    print("✅ Setup complete!")
    return True

# In main flow
if args.setup or args.setup_only:
    run_setup()
    if args.setup_only:
        print("Setup-only mode. Exiting.")
        exit(0)
```

---

### Solution 2: Databricks REPO_ROOT Auto-Detection Improvement

Current auto-detection looks for `/sql/databricks/notebooks/` which may not match user's directory structure.

**Improved detection:**

```python
# Current approach
marker = "/sql/databricks/notebooks/"

# Improved: Try multiple detection methods
def detect_repo_root():
    """Auto-detect repo root using multiple strategies."""
    
    # Strategy 1: Widget override (user-provided)
    if REPO_ROOT:
        return REPO_ROOT, "widget"
    
    # Strategy 2: Look for marker in notebook path
    nb_path = None
    try:
        nb_path = dbutils.notebook.getContext().notebookPath().get()
    except:
        pass
    
    markers = [
        "/sql/databricks/notebooks/",  # Original
        "/sql/databricks/core/",        # If running from core
        "/sql/databricks/",             # Generic
    ]
    
    if nb_path:
        for marker in markers:
            if marker in nb_path:
                repo_root = "/Workspace" + nb_path.split(marker)[0]
                return repo_root, f"auto-detect ({marker})"
    
    # Strategy 3: Look for sql-identity-resolution in path
    if nb_path and "sql-identity-resolution" in nb_path:
        idx = nb_path.index("sql-identity-resolution")
        repo_root = "/Workspace" + nb_path[:idx + len("sql-identity-resolution")]
        return repo_root, "auto-detect (repo name)"
    
    # Strategy 4: Try relative path from current notebook
    try:
        # Check if ../../../sql/common exists
        test_path = dbutils.notebook.getContext().notebookPath().get()
        parts = test_path.split("/")
        for i in range(len(parts), 0, -1):
            candidate = "/Workspace" + "/".join(parts[:i])
            if os.path.exists(f"{candidate}/sql/common"):
                return candidate, "filesystem scan"
    except:
        pass
    
    return None, None

repo_root, detection_method = detect_repo_root()
if not repo_root:
    raise RuntimeError("""
Cannot determine repo root. Please set the REPO_ROOT widget to your repo path.
    
Example: /Workspace/Repos/username/sql-identity-resolution
    
Tip: Navigate to your repo in Databricks, copy the path, and paste it in the REPO_ROOT widget.
""")

print(f"Repo root: {repo_root} (detected via: {detection_method})")
```

---

### Solution 3: Setup Notebooks for Databricks

Create dedicated setup notebooks that are foolproof:

**File: `sql/databricks/notebooks/00_Setup.py`**

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Identity Resolution - One-Time Setup
# MAGIC 
# MAGIC Run this notebook ONCE to set up all required tables and schemas.
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC - Access to create schemas in your catalog
# MAGIC - Unity Catalog enabled (recommended)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Configure Your Catalog
# MAGIC 
# MAGIC Edit the cell below with your catalog name:

# COMMAND ----------
CATALOG = "your_catalog_here"  # <-- EDIT THIS

# Validate
if CATALOG == "your_catalog_here":
    raise ValueError("Please edit CATALOG above with your actual catalog name")

print(f"Using catalog: {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Create Schemas

# COMMAND ----------
spark.sql("CREATE SCHEMA IF NOT EXISTS idr_meta")
spark.sql("CREATE SCHEMA IF NOT EXISTS idr_work")
spark.sql("CREATE SCHEMA IF NOT EXISTS idr_out")
print("✅ Schemas created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Create Tables

# COMMAND ----------
# Read and execute DDL (embedded for reliability)
DDL = """
CREATE TABLE IF NOT EXISTS idr_meta.source_table (
  table_id STRING,
  table_fqn STRING,
  entity_type STRING,
  entity_key_expr STRING,
  watermark_column STRING,
  watermark_lookback_minutes INT,
  is_active BOOLEAN
);

-- ... (rest of DDL)
"""

for stmt in DDL.split(";"):
    if stmt.strip():
        spark.sql(stmt)

print("✅ Tables created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: Seed Default Rules

# COMMAND ----------
spark.sql("""
INSERT INTO idr_meta.rule VALUES
('R_EMAIL', 'Email Exact Match', true, 1, 'EMAIL', 'LOWERCASE', false, true, 10000),
('R_PHONE', 'Phone Exact Match', true, 2, 'PHONE', 'NONE', false, true, 10000)
ON CONFLICT (rule_id) DO NOTHING
""")
print("✅ Default rules seeded")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup Complete! 🎉
# MAGIC 
# MAGIC You can now:
# MAGIC 1. Configure your source tables in `idr_meta.source_table`
# MAGIC 2. Run the `IDR_Run` notebook
```

---

### Solution 4: Unified CLI Installer

Create a single `install.py` that works for all platforms:

**File: `tools/install.py`**

```python
#!/usr/bin/env python3
"""
SQL Identity Resolution - Unified Installer

Usage:
    python install.py --platform=snowflake --account=xxx --user=xxx --password=xxx
    python install.py --platform=bigquery --project=xxx
    python install.py --platform=duckdb --db=./idr.duckdb
    python install.py --platform=databricks --host=xxx --token=xxx --catalog=xxx
"""

import argparse
import os

def install_snowflake(args):
    import snowflake.connector
    conn = snowflake.connector.connect(
        account=args.account,
        user=args.user,
        password=args.password,
        warehouse=args.warehouse or 'COMPUTE_WH'
    )
    
    ddl_path = os.path.join(os.path.dirname(__file__), '../sql/snowflake/core/00_ddl_all.sql')
    with open(ddl_path) as f:
        ddl = f.read()
    
    for stmt in ddl.split(';'):
        if stmt.strip():
            conn.cursor().execute(stmt)
    
    print("✅ Snowflake setup complete!")
    print(f"""
Next steps:
1. Add your source tables to idr_meta.source_table
2. Run: snowsql -q "CALL idr_run('FULL', 30, TRUE)" --dry-run first
""")

def install_bigquery(args):
    from google.cloud import bigquery
    client = bigquery.Client(project=args.project)
    
    ddl_path = os.path.join(os.path.dirname(__file__), '../sql/bigquery/core/00_ddl_all.sql')
    with open(ddl_path) as f:
        ddl = f.read()
    
    for stmt in ddl.split(';'):
        if stmt.strip():
            client.query(stmt).result()
    
    print("✅ BigQuery setup complete!")

def install_duckdb(args):
    import duckdb
    conn = duckdb.connect(args.db)
    
    ddl_path = os.path.join(os.path.dirname(__file__), '../sql/duckdb/core/00_ddl_all.sql')
    with open(ddl_path) as f:
        conn.execute(f.read())
    
    print(f"✅ DuckDB setup complete! Database: {args.db}")

# Main
parser = argparse.ArgumentParser()
parser.add_argument('--platform', required=True, choices=['snowflake', 'bigquery', 'duckdb', 'databricks'])
# ... platform-specific args

args = parser.parse_args()

if args.platform == 'snowflake':
    install_snowflake(args)
elif args.platform == 'bigquery':
    install_bigquery(args)
elif args.platform == 'duckdb':
    install_duckdb(args)
```

---

## Recommended Implementation Path

### Phase 1: Quick Wins (1-2 days)

1. **Add `--setup` flag to all Python runners** (DuckDB, BigQuery)
   - Embed DDL or read from adjacent file
   - Run DDL with `IF NOT EXISTS` / `ON CONFLICT DO NOTHING`
   
2. **Improve Databricks auto-detection**
   - Add multiple marker strategies
   - Better error message with copy-paste instructions

### Phase 2: Full Solution (2-3 days)

3. **Create `tools/install.py`** unified installer
4. **Create Databricks `00_Setup` notebook** with guided setup
5. **Update documentation** with one-command quick starts

### Phase 3: Polish (1 day)

6. **Add verification step** after setup that confirms all tables exist
7. **Add `--verify` flag** to check setup without running
8. **Create Terraform/Pulumi modules** for infrastructure-as-code users

---

## Verification

After implementation:

```bash
# DuckDB (should work with single command)
python sql/duckdb/core/idr_run.py --db=test.duckdb --setup-only
# Expected: Creates schemas, tables, seeds rules

# BigQuery
python sql/bigquery/core/idr_run.py --project=my-project --setup-only
# Expected: Same behavior

# Snowflake (via snowsql)
snowsql -q "source sql/snowflake/core/00_ddl_all.sql"
# Or via new installer:
python tools/install.py --platform=snowflake --account=xxx ...
```

---

## Open Questions

1. **Should setup seed sample source tables?** (Helps with demo but might confuse production users)
2. **How to handle upgrades?** (New columns added to tables in future versions)
3. **Should we create a Docker-based installer?** (One command for any platform)
