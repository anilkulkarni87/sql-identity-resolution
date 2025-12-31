# BigQuery Setup

BigQuery is ideal for **GCP-native organizations** seeking serverless identity resolution.

---

## Prerequisites

- GCP project with BigQuery enabled
- Service account with BigQuery Admin role (or equivalent)
- Python 3.9+ with `google-cloud-bigquery` package

---

## Installation

```bash
# Clone repository
git clone https://github.com/anilkulkarni87/sql-identity-resolution.git
cd sql-identity-resolution

# Install dependencies
pip install google-cloud-bigquery

# Set credentials
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

---

## Create Datasets and Tables

```bash
# Create datasets
bq mk --dataset your_project:idr_meta
bq mk --dataset your_project:idr_work
bq mk --dataset your_project:idr_out

# Run DDL (creates all tables)
bq query --use_legacy_sql=false < sql/bigquery/00_ddl_all.sql
```

Or run the DDL directly in BigQuery console.

---

## Configure Sources

```sql
-- Register source table
INSERT INTO `your_project.idr_meta.source_table` VALUES
  ('customers', 'your_project.crm.customers', 'PERSON', 'customer_id', 'updated_at', 0, TRUE);

-- Define matching rules
INSERT INTO `your_project.idr_meta.rule` VALUES
  ('email_exact', 'EMAIL', 1, TRUE, 10000),
  ('phone_exact', 'PHONE', 2, TRUE, 5000);

-- Map identifiers
INSERT INTO `your_project.idr_meta.identifier_mapping` VALUES
  ('customers', 'EMAIL', 'email', TRUE),
  ('customers', 'PHONE', 'phone', TRUE);
```

---

## Run IDR

### CLI Options

| Option | Description | Default |
|--------|-------------|---------|
| `--project` | GCP project ID | Required |
| `--run-mode` | `FULL` or `INCR` | `INCR` |
| `--max-iters` | Max label propagation iterations | 30 |
| `--dry-run` | Preview mode (no commits) | False |

### Dry Run (Preview)

```bash
python sql/bigquery/idr_run.py \
  --project=your-project \
  --run-mode=FULL \
  --dry-run
```

### Live Run

```bash
python sql/bigquery/idr_run.py \
  --project=your-project \
  --run-mode=FULL
```

### Incremental Run

```bash
python sql/bigquery/idr_run.py \
  --project=your-project \
  --run-mode=INCR
```

---

## Review Dry Run Results

```sql
-- View proposed changes
SELECT entity_key, current_resolved_id, proposed_resolved_id, change_type
FROM `your_project.idr_out.dry_run_results`
WHERE run_id = 'dry_run_abc123'
ORDER BY change_type;

-- Summary statistics
SELECT * FROM `your_project.idr_out.dry_run_summary`
WHERE run_id = 'dry_run_abc123';
```

---

## Verify Results

```sql
-- Run history
SELECT run_id, status, entities_processed, duration_seconds
FROM `your_project.idr_out.run_history`
ORDER BY started_at DESC
LIMIT 10;

-- Cluster distribution
SELECT cluster_size, COUNT(*) as count
FROM `your_project.idr_out.identity_clusters_current`
GROUP BY cluster_size
ORDER BY cluster_size;
```

---

## Scheduling with Cloud Scheduler

### 1. Create a Cloud Function

```python
# main.py
import subprocess
import functions_framework

@functions_framework.http
def run_idr(request):
    result = subprocess.run([
        'python', 'idr_run.py',
        '--project=your-project',
        '--run-mode=INCR'
    ], capture_output=True, text=True)
    return result.stdout
```

### 2. Deploy the Function

```bash
gcloud functions deploy idr-runner \
  --runtime=python311 \
  --trigger-http \
  --source=./sql/bigquery
```

### 3. Create Cloud Scheduler Job

```bash
gcloud scheduler jobs create http idr-hourly \
  --schedule="0 * * * *" \
  --uri="https://REGION-PROJECT.cloudfunctions.net/idr-runner" \
  --http-method=GET
```

---

## Terraform Deployment

```hcl
# main.tf
resource "google_bigquery_dataset" "idr_meta" {
  dataset_id = "idr_meta"
  location   = "US"
}

resource "google_bigquery_dataset" "idr_work" {
  dataset_id = "idr_work"
  location   = "US"
}

resource "google_bigquery_dataset" "idr_out" {
  dataset_id = "idr_out"
  location   = "US"
}

resource "google_cloud_scheduler_job" "idr_hourly" {
  name     = "idr-hourly"
  schedule = "0 * * * *"
  
  http_target {
    uri         = google_cloudfunctions_function.idr_runner.https_trigger_url
    http_method = "GET"
  }
}
```

---

## Cost Optimization

1. **Use Slot Reservations**: For predictable costs on large datasets
2. **Partition Tables**: Partition by updated_at for efficient incremental processing
3. **Use BI Engine**: For dashboard queries on output tables

---

## Next Steps

- [Configuration Guide](../../guides/configuration.md)
- [Dry Run Mode](../../guides/dry-run-mode.md)
- [CI/CD](../../deployment/ci-cd.md)
