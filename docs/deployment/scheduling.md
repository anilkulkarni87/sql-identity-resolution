# Scheduling

Set up automated, recurring IDR runs on each platform.

---

## Platform Options

| Platform | Scheduler | Recommended |
|----------|-----------|-------------|
| DuckDB | Cron, Airflow, Prefect | Airflow for production |
| Snowflake | Snowflake Tasks | ✅ Native |
| BigQuery | Cloud Scheduler + Functions | ✅ Native |
| Databricks | Workflows/Jobs | ✅ Native |

---

## DuckDB Scheduling

### Cron

Simple scheduling for standalone deployments:

```bash
# Edit crontab
crontab -e

# Run every hour
0 * * * * cd /path/to/repo && python sql/duckdb/idr_run.py --db=idr.duckdb --run-mode=INCR >> /var/log/idr.log 2>&1

# Run every 15 minutes
*/15 * * * * cd /path/to/repo && python sql/duckdb/idr_run.py --db=idr.duckdb --run-mode=INCR >> /var/log/idr.log 2>&1
```

### Airflow DAG

```python
# dags/idr_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['alerts@company.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'idr_hourly',
    default_args=default_args,
    description='Hourly Identity Resolution',
    schedule_interval='0 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:
    
    dry_run = BashOperator(
        task_id='dry_run',
        bash_command='cd /opt/idr && python sql/duckdb/idr_run.py --db=idr.duckdb --run-mode=INCR --dry-run',
    )
    
    check_dry_run = BashOperator(
        task_id='check_dry_run',
        bash_command='''
            python -c "
            import duckdb
            conn = duckdb.connect('/opt/idr/idr.duckdb')
            result = conn.execute('SELECT status FROM idr_out.run_history ORDER BY started_at DESC LIMIT 1').fetchone()
            if result[0] != 'DRY_RUN_COMPLETE':
                raise Exception('Dry run failed')
            "
        ''',
    )
    
    live_run = BashOperator(
        task_id='live_run',
        bash_command='cd /opt/idr && python sql/duckdb/idr_run.py --db=idr.duckdb --run-mode=INCR',
    )
    
    dry_run >> check_dry_run >> live_run
```

---

## Snowflake Tasks

### Create Task

```sql
-- Create a scheduled task
CREATE OR REPLACE TASK idr_hourly_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '60 MINUTE'
AS
  CALL idr_run('INCR', 30, FALSE);

-- Enable the task
ALTER TASK idr_hourly_task RESUME;
```

### Monitor Tasks

```sql
-- Check task history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'IDR_HOURLY_TASK'
ORDER BY SCHEDULED_TIME DESC
LIMIT 20;

-- Check task status
SHOW TASKS LIKE 'IDR%';
```

### Task with Dry Run Validation

```sql
-- Two-step task with dry run
CREATE OR REPLACE TASK idr_dry_run_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '60 MINUTE'
AS
  CALL idr_run('INCR', 30, TRUE);

CREATE OR REPLACE TASK idr_live_run_task
  WAREHOUSE = COMPUTE_WH
  AFTER idr_dry_run_task
  WHEN (
    SYSTEM$STREAM_HAS_DATA('idr_dry_run_check') = FALSE
    AND (SELECT MAX(status) FROM idr_out.run_history WHERE started_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP)) = 'DRY_RUN_COMPLETE'
  )
AS
  CALL idr_run('INCR', 30, FALSE);
```

---

## BigQuery Scheduling

### Cloud Functions

```python
# main.py
import subprocess
import functions_framework

@functions_framework.http
def run_idr(request):
    project = 'your-project'
    result = subprocess.run([
        'python', '/app/idr_run.py',
        f'--project={project}',
        '--run-mode=INCR'
    ], capture_output=True, text=True)
    
    return {
        'stdout': result.stdout,
        'stderr': result.stderr,
        'returncode': result.returncode
    }
```

### Deploy Function

```bash
gcloud functions deploy idr-runner \
  --gen2 \
  --runtime=python311 \
  --region=us-central1 \
  --source=./sql/bigquery \
  --entry-point=run_idr \
  --trigger-http \
  --timeout=540s \
  --memory=2048MB
```

### Cloud Scheduler

```bash
# Create job
gcloud scheduler jobs create http idr-hourly \
  --location=us-central1 \
  --schedule="0 * * * *" \
  --uri="https://us-central1-your-project.cloudfunctions.net/idr-runner" \
  --http-method=GET \
  --oidc-service-account-email=idr-runner@your-project.iam.gserviceaccount.com

# View job
gcloud scheduler jobs describe idr-hourly --location=us-central1

# Trigger manually
gcloud scheduler jobs run idr-hourly --location=us-central1
```

### Terraform

```hcl
# scheduler.tf
resource "google_cloud_scheduler_job" "idr_hourly" {
  name      = "idr-hourly"
  schedule  = "0 * * * *"
  time_zone = "UTC"

  http_target {
    uri         = google_cloudfunctions2_function.idr_runner.service_config[0].uri
    http_method = "GET"
    
    oidc_token {
      service_account_email = google_service_account.idr_runner.email
    }
  }
}
```

---

## Databricks Workflows

### Create Job (UI)

1. Go to **Workflows** → **Create Job**
2. Add task:
   - **Type**: Notebook
   - **Path**: `/Repos/your-org/sql-identity-resolution/sql/databricks/notebooks/IDR_Run`
3. Set parameters:
   ```json
   {"RUN_MODE": "INCR", "DRY_RUN": "false"}
   ```
4. Configure schedule

### Create Job (API)

```json
{
  "name": "IDR Hourly",
  "tasks": [
    {
      "task_key": "idr_run",
      "notebook_task": {
        "notebook_path": "/Repos/your-org/sql-identity-resolution/sql/databricks/notebooks/IDR_Run",
        "base_parameters": {
          "RUN_MODE": "INCR",
          "DRY_RUN": "false",
          "MAX_ITERS": "30"
        }
      },
      "existing_cluster_id": "your-cluster-id"
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 * * * ?",
    "timezone_id": "UTC"
  },
  "email_notifications": {
    "on_failure": ["alerts@company.com"]
  },
  "max_concurrent_runs": 1
}
```

### Terraform

```hcl
resource "databricks_job" "idr_hourly" {
  name = "IDR Hourly"
  
  task {
    task_key = "idr_run"
    
    notebook_task {
      notebook_path = "/Repos/your-org/sql-identity-resolution/sql/databricks/notebooks/IDR_Run"
      base_parameters = {
        RUN_MODE = "INCR"
        DRY_RUN  = "false"
      }
    }
    
    existing_cluster_id = var.cluster_id
  }
  
  schedule {
    quartz_cron_expression = "0 0 * * * ?"
    timezone_id            = "UTC"
  }
  
  email_notifications {
    on_failure = ["alerts@company.com"]
  }
  
  max_concurrent_runs = 1
}
```

---

## Best Practices

1. **Run incrementally**: After initial FULL run, use INCR
2. **Stagger runs**: Avoid running at exact hour marks
3. **Monitor**: Set up alerts for failures
4. **Dry run first**: For major config changes, dry run before live
5. **Concurrency**: Prevent overlapping runs (`max_concurrent_runs=1`)

---

## Next Steps

- [CI/CD](ci-cd.md)
- [Security](security.md)
- [Metrics & Monitoring](../guides/metrics-monitoring.md)
