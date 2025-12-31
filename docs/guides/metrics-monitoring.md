# Metrics & Monitoring

Set up observability for your identity resolution pipeline.

---

## Built-in Metrics

Every run automatically records metrics to `idr_out.metrics_export`:

| Metric Name | Type | Description |
|-------------|------|-------------|
| `idr_run_duration_seconds` | gauge | Total run duration |
| `idr_entities_processed` | gauge | Entities processed this run |
| `idr_edges_created` | counter | Edges created |
| `idr_clusters_impacted` | gauge | Clusters affected |
| `idr_lp_iterations` | gauge | Label propagation iterations |
| `idr_groups_skipped` | counter | Groups skipped (max_group_size) |
| `idr_large_clusters` | gauge | Clusters exceeding threshold |

---

## Querying Metrics

### View Recent Metrics

```sql
SELECT 
    run_id,
    metric_name,
    metric_value,
    metric_type,
    recorded_at
FROM idr_out.metrics_export
WHERE run_id = 'run_xyz'
ORDER BY recorded_at;
```

### Metrics Over Time

```sql
SELECT 
    DATE(recorded_at) as date,
    metric_name,
    AVG(metric_value) as avg_value,
    MAX(metric_value) as max_value
FROM idr_out.metrics_export
WHERE metric_name = 'idr_run_duration_seconds'
  AND recorded_at >= CURRENT_DATE - 30
GROUP BY DATE(recorded_at), metric_name
ORDER BY date DESC;
```

---

## Metrics Exporter

The `tools/metrics_exporter.py` script exports metrics to external systems.

### Installation

```bash
pip install requests prometheus-client
```

### Usage

```bash
# Export to stdout (debugging)
python tools/metrics_exporter.py \
    --platform=duckdb \
    --connection=idr.duckdb \
    --exporter=stdout

# Export to Prometheus (scrapeable endpoint)
python tools/metrics_exporter.py \
    --platform=duckdb \
    --connection=idr.duckdb \
    --exporter=prometheus \
    --prometheus-port=9090

# Export to DataDog
python tools/metrics_exporter.py \
    --platform=duckdb \
    --connection=idr.duckdb \
    --exporter=datadog \
    --datadog-api-key=$DD_API_KEY

# Export to webhook
python tools/metrics_exporter.py \
    --platform=duckdb \
    --connection=idr.duckdb \
    --exporter=webhook \
    --webhook-url=https://hooks.slack.com/services/xxx
```

---

## Prometheus Integration

### Start Exporter

```bash
python tools/metrics_exporter.py \
    --platform=snowflake \
    --exporter=prometheus \
    --prometheus-port=9090
```

### Prometheus Configuration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'idr'
    static_configs:
      - targets: ['localhost:9090']
    scrape_interval: 60s
```

### Grafana Dashboard

Import the provided dashboard or create panels for:

- Run duration trend
- Entities processed per run
- Cluster growth over time
- Skipped groups alerts

---

## DataDog Integration

### Export Metrics

```bash
export DD_API_KEY=your_api_key
python tools/metrics_exporter.py \
    --platform=bigquery \
    --project=my-project \
    --exporter=datadog
```

### DataDog Dashboard

Create monitors for:

```yaml
# Example monitor: High skipped groups
name: "IDR - High Skipped Groups"
type: metric alert
query: "avg(last_5m):sum:idr.groups_skipped{*} > 100"
message: "More than 100 identifier groups skipped. Review max_group_size settings."

# Example monitor: Long run duration
name: "IDR - Slow Run"
type: metric alert
query: "avg(last_1h):max:idr.run_duration_seconds{*} > 3600"
message: "IDR run took over 1 hour. Investigate performance."
```

---

## Alerting

### SQL-Based Alerts

Run these queries periodically and alert on non-empty results:

```sql
-- Alert: Run failures
SELECT run_id, status, ended_at
FROM idr_out.run_history
WHERE status = 'FAILED'
  AND ended_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour';

-- Alert: High skipped groups
SELECT run_id, groups_skipped
FROM idr_out.run_history
WHERE groups_skipped > 100
  AND ended_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour';

-- Alert: Giant clusters forming
SELECT resolved_id, cluster_size
FROM idr_out.identity_clusters_current
WHERE cluster_size > 10000;
```

### Platform-Specific Alerts

=== "Snowflake"
    ```sql
    -- Use Snowflake Alerts (Snowsight)
    CREATE ALERT idr_failure_alert
      WAREHOUSE = compute_wh
      SCHEDULE = '1 MINUTE'
      IF (EXISTS (
        SELECT 1 FROM idr_out.run_history 
        WHERE status = 'FAILED' 
          AND ended_at >= CURRENT_TIMESTAMP - INTERVAL '5 minutes'
      ))
      THEN CALL SYSTEM$SEND_EMAIL(...);
    ```

=== "BigQuery"
    ```sql
    -- Use Cloud Monitoring alert policies
    -- Create log-based metric from run_history inserts
    ```

=== "Databricks"
    ```python
    # Use Databricks SQL Alerts
    # Configure in Databricks SQL workspace
    ```

---

## Dashboards

### Key Metrics to Display

| Panel | Query |
|-------|-------|
| Run Success Rate | `COUNT(status='SUCCESS') / COUNT(*)` |
| Average Duration | `AVG(duration_seconds)` |
| Entities Per Run | `AVG(entities_processed)` |
| Cluster Growth | `MAX(cluster_size) over time` |
| Skipped Groups Trend | `SUM(groups_skipped) by date` |

### Sample SQL Dashboard Query

```sql
WITH run_stats AS (
    SELECT 
        DATE(started_at) as run_date,
        COUNT(*) as runs,
        SUM(CASE WHEN status LIKE 'SUCCESS%' THEN 1 ELSE 0 END) as successful,
        AVG(duration_seconds) as avg_duration,
        SUM(entities_processed) as total_entities,
        SUM(groups_skipped) as total_skipped
    FROM idr_out.run_history
    WHERE started_at >= CURRENT_DATE - 30
    GROUP BY DATE(started_at)
)
SELECT 
    run_date,
    runs,
    ROUND(100.0 * successful / runs, 1) as success_rate,
    ROUND(avg_duration, 0) as avg_duration_sec,
    total_entities,
    total_skipped
FROM run_stats
ORDER BY run_date DESC;
```

---

## Health Checks

### Pre-Run Health Check

```sql
-- Check sources are accessible
SELECT table_id, table_fqn
FROM idr_meta.source_table
WHERE is_active = TRUE;

-- Check for stale watermarks (no runs in 24h)
SELECT table_id, last_run_ts
FROM idr_meta.run_state
WHERE last_run_ts < CURRENT_TIMESTAMP - INTERVAL '24 hours';
```

### Post-Run Validation

```sql
-- Verify outputs populated
SELECT 
    'membership' as table_name, 
    COUNT(*) as row_count 
FROM idr_out.identity_resolved_membership_current
UNION ALL
SELECT 
    'clusters', 
    COUNT(*) 
FROM idr_out.identity_clusters_current;

-- Check for orphaned clusters
SELECT COUNT(*) as orphaned
FROM idr_out.identity_clusters_current c
LEFT JOIN idr_out.identity_resolved_membership_current m 
    ON c.resolved_id = m.resolved_id
WHERE m.resolved_id IS NULL;
```

---

## Next Steps

- [Troubleshooting](troubleshooting.md)
- [CI/CD](../deployment/ci-cd.md)
- [Production Hardening](production-hardening.md)
