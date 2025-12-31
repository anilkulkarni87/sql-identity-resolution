# Production Deployment: Snowflake

This guide details the exact steps to deploy SQL Identity Resolution (IDR) to a production Snowflake environment.

---

## Prerequisites

- **Snowflake Account**: Usage of `ACCOUNTADMIN` or a role with `CREATE DATABASE/SCHEMA` privileges.
- **Python Environment**: For running the metadata loader (CI/CD or orchestration server).
- **Source Data**: Read access to the tables you wish to resolve.

---

## Step 1: Schema Setup

You need to create the tables and the stored procedure runner.

### 1.1 Create Tables
Execute the content of `sql/snowflake/core/00_ddl_all.sql`. This creates:
- `idr_meta`: Configuration and rules.
- `idr_work`: Transient intermediate tables.
- `idr_out`: Final output tables (clusters, golden profiles).

### 1.2 Deploy Stored Procedure
Execute the content of `sql/snowflake/core/IDR_Run.sql`.
This creates the `idr_run` stored procedure, which contains the entire resolution logic.

```sql
-- Verify deployment
SHOW PROCEDURES LIKE 'idr_run';
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
  - table_id: crm_customers
    table_fqn: RAW.CRM.CUSTOMERS
    entity_key_expr: customer_id
    trust_rank: 1
    identifiers:
      - type: EMAIL
        expr: email_address
    attributes:
      first_name: fname
      last_name: lname
```

---

## Step 3: Metadata Loading

Use the `load_metadata.py` tool to push your configuration to Snowflake. This ensures your config is version-controlled and deployed safely.

**Run via CI/CD (GitHub Actions, Jenkins) or Airflow:**

```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="idr_service_user"
export SNOWFLAKE_PASSWORD="***"
export SNOWFLAKE_DATABASE="IDR_PROD"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"

python tools/load_metadata.py \
  --platform=snowflake \
  --config=production.yaml
```

---

## Step 4: Execution & Scheduling

The IDR process is triggered by calling the stored procedure. You can schedule this using **Snowflake Tasks** or an external orchestrator (Airflow, Prefect, dbt).

### Option A: Snowflake Tasks (Recommended)

```sql
CREATE OR REPLACE TASK run_idr_daily
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 2 * * * America/Los_Angeles' -- Daily at 2AM
AS
  CALL idr_run('FULL', 30, FALSE);
```

### Option B: dbt

You can use a `run-operation` in dbt:

```sql
-- macros/run_idr.sql
{% macro run_idr() %}
  {% do run_query("CALL idr_run('FULL', 30, FALSE)") %}
{% endmacro %}
```

---

## Step 5: Monitoring

Monitor the pipeline using the `idr_out` tables.

**Check Run Status:**
```sql
SELECT run_id, status, duration_seconds, entities_processed, clusters_impacted 
FROM idr_out.run_history 
ORDER BY started_at DESC 
LIMIT 10;
```

**Check for Warnings:**
```sql
SELECT * FROM idr_out.run_history 
WHERE warnings IS NOT NULL 
ORDER BY started_at DESC;
```
