# Production Deployment: BigQuery

This guide details the exact steps to deploy SQL Identity Resolution (IDR) to a production Google BigQuery environment.

---

## Prerequisites

- **GCP Project**: A Google Cloud Project with BigQuery API enabled.
- **Service Account**: A service account with `BigQuery Admin` or `BigQuery Data Editor` + `BigQuery Job User` roles.
- **Python Environment**: For running `load_metadata.py` and `idr_run.py`.

---

## Step 1: Schema Setup

### 1.1 Create Datasets
Create the three required datasets in your project.

```bash
export PROJECT_ID="your-project-id"

bq mk --dataset ${PROJECT_ID}:idr_meta
bq mk --dataset ${PROJECT_ID}:idr_work
bq mk --dataset ${PROJECT_ID}:idr_out
```

### 1.2 Create Tables
Execute the DDL script to create all necessary tables.

```bash
bq query --use_legacy_sql=false < sql/bigquery/core/00_ddl_all.sql
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
  - table_id: ga4_events
    table_fqn: your-project-id.analytics_123456.events_*
    entity_key_expr: user_pseudo_id
    trust_rank: 2
    identifiers:
      - type: COOKIE
        expr: user_pseudo_id
```

---

## Step 3: Metadata Loading

Use the `load_metadata.py` tool to push your configuration to BigQuery.

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/key.json"

python tools/load_metadata.py \
  --platform=bigquery \
  --project=your-project-id \
  --config=production.yaml
```

---

## Step 4: Execution & Scheduling

The IDR process is a Python script (`sql/bigquery/core/idr_run.py`) that orchestrates BigQuery jobs.

### Option A: Cloud Run Jobs (Recommended)

1.  Containerize the application (Dockerfile provided in repo).
2.  Deploy to Cloud Run Jobs or Google Kubernetes Engine (GKE).
3.  Schedule with Cloud Scheduler.

**Command:**
```bash
python sql/bigquery/core/idr_run.py \
  --project=your-project-id \
  --run-mode=FULL
```

### Option B: Cloud Composer (Airflow)

Use the `BashOperator` or `KubernetesPodOperator` to run the script.

```python
run_idr = BashOperator(
    task_id='run_idr',
    bash_command='python sql/bigquery/core/idr_run.py --project={{ var.value.gcp_project }} --run-mode=FULL',
    dag=dag
)
```

---

## Step 5: Monitoring

Monitor the pipeline using the `idr_out` tables.

**Check Run History:**
```sql
SELECT run_id, status, duration_seconds, entities_processed 
FROM `your-project-id.idr_out.run_history` 
ORDER BY started_at DESC 
LIMIT 10;
```
