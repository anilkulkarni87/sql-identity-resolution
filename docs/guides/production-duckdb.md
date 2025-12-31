# Production Deployment: DuckDB

While DuckDB is primarily embedded, it can be used in "production" for single-node analytical workloads, data apps (Streamlit/Rill), or generating extracts for other systems.

---

## Prerequisites

- **Python Environment**: Python 3.9+.
- **Persistent Storage**: A location for the `.duckdb` file (e.g., EBS volume, local disk).

---

## Step 1: Schema Setup

Initialize the database file with the schema.

```bash
# Initialize persisted database
duckdb proddb.duckdb < sql/duckdb/core/00_ddl_all.sql
```

---

## Step 2: Configuration

Create a `production.yaml` config file.

```yaml
rules:
  - rule_id: email_exact
    identifier_type: EMAIL
    settings:
      canonicalize: LOWERCASE
sources:
  - table_id: local_csv
    table_fqn: "read_csv_auto('data/*.csv')"
    entity_key_expr: user_id
    identifiers:
      - type: EMAIL
        expr: email
```

---

## Step 3: Metadata Loading

Load configuration into the specific database file.

```bash
python tools/load_metadata.py \
  --platform=duckdb \
  --db=proddb.duckdb \
  --config=production.yaml
```

---

## Step 4: Execution & Scheduling

Run the `idr_run.py` script pointing to your production database file.

### Cron Job

```bash
# Run daily at 3 AM
0 3 * * * python sql/duckdb/core/idr_run.py --db=/path/to/proddb.duckdb --run-mode=FULL >> /var/log/idr.log 2>&1
```

### Running as a Library

You can also import the logic if you refactor `idr_run.py` to be callable, allowing you to embed identity resolution directly in your FastAPI or Flask app.

---

## Step 5: Consuming Results

You can query the results directly using the DuckDB CLI or Python client.

```bash
duckdb proddb.duckdb "SELECT * FROM idr_out.golden_profile_current LIMIT 5"
```
