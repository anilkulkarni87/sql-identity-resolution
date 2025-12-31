# Dry Run Mode & Metrics Export Design

## Overview

This document specifies the design for two production-critical features:
1. **Dry Run Mode** - Preview IDR changes without committing to production tables
2. **Metrics Export** - Standardized metrics for monitoring dashboards

Both features are designed to work consistently across DuckDB, BigQuery, Snowflake, and Databricks.

---

## Design Decisions (Approved)

| Decision | Choice | Notes |
|----------|--------|-------|
| Dry Run Retention | **7 days default** | Auto-cleanup via scheduled job |
| Large Cluster Threshold | **5000 default, configurable** | Stored in `idr_meta.config` table |
| Metrics Export | **Plugin framework approach** | Core exports to table; plugins handle providers |
| Per-Rule Metrics | **Yes, with aggregates** | Dimensions include `rule_id` where applicable |

---

## 1. Dry Run Mode

### Use Cases

| Scenario | Who | Why |
|----------|-----|-----|
| **Rule Testing** | Data Engineer | Validate new matching rules before production deployment |
| **Impact Analysis** | Data Steward | Understand how many entities will be affected |
| **Regression Check** | QA Team | Compare expected vs actual cluster changes |
| **Audit Trail** | Compliance | Document what *would* happen before approval |

### Design Principles

1. **Zero Production Impact** - Never write to `_current` tables in dry run
2. **Full Pipeline Execution** - Run complete pipeline to catch all issues
3. **Comparable Output** - Generate diff reports vs current state
4. **Cleanup Automatic** - All dry run data auto-expires or is easily deleted

---

### Schema Changes

#### New Table: `idr_out.dry_run_results`

```sql
CREATE TABLE idr_out.dry_run_results (
    run_id VARCHAR NOT NULL,            -- Links to run_history
    entity_key VARCHAR NOT NULL,
    current_resolved_id VARCHAR,        -- Current cluster (NULL if new entity)
    proposed_resolved_id VARCHAR,       -- What dry run computed
    change_type VARCHAR,                -- 'NEW', 'MOVED', 'MERGED', 'UNCHANGED'
    current_cluster_size INT,
    proposed_cluster_size INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, entity_key)
);
```

#### New Table: `idr_out.dry_run_summary`

```sql
CREATE TABLE idr_out.dry_run_summary (
    run_id VARCHAR PRIMARY KEY,
    total_entities INT,
    new_entities INT,                   -- First time seen
    moved_entities INT,                 -- Changed cluster
    merged_clusters INT,                -- Number of clusters that merged
    split_clusters INT,                 -- Number of clusters that split
    largest_proposed_cluster INT,
    edges_would_create INT,
    groups_would_skip INT,
    values_would_exclude INT,
    execution_time_seconds INT,
    created_at TIMESTAMP
);
```

---

### Runner Interface

#### Parameter: `DRY_RUN`

| Platform | How to Enable |
|----------|---------------|
| **DuckDB** | `python idr_run.py --dry-run` or `DRY_RUN=true` env var |
| **Databricks** | Widget: `dbutils.widgets.text("DRY_RUN", "false")` |
| **Snowflake** | `CALL idr_run('INCR', TRUE);` (second param = dry_run) |
| **BigQuery** | `python idr_run.py --dry-run` or `DRY_RUN=true` env var |

#### Dry Run Behavior

```
┌─────────────────────────────────────────────────────────────────┐
│                       DRY RUN PIPELINE                          │
├─────────────────────────────────────────────────────────────────┤
│ 1. Build entities_delta           ✓ Same as production         │
│ 2. Extract identifiers            ✓ Same as production         │
│ 3. Apply exclusion list           ✓ Same as production         │
│ 4. Filter by max_group_size       ✓ Same as production         │
│ 5. Build edges                    → Write to idr_work.edges_new│
│ 6. Label propagation              ✓ Same as production         │
│ 7. Compute membership             → Write to idr_work.*        │
│                                                                 │
│ 8. DIFF PHASE (Dry Run Only):                                   │
│    - Compare proposed vs current membership                    │
│    - Populate dry_run_results with change_type                 │
│    - Compute dry_run_summary statistics                        │
│                                                                 │
│ ❌ SKIP: Write to idr_out.*_current tables                     │
│ ❌ SKIP: Update idr_meta.run_state watermarks                  │
│ ✓ DO: Update run_history with status='DRY_RUN_COMPLETE'        │
└─────────────────────────────────────────────────────────────────┘
```

---

### Diff Logic (Pseudocode)

```sql
-- Compute change types for each entity
INSERT INTO idr_out.dry_run_results
SELECT
    '{run_id}' AS run_id,
    COALESCE(proposed.entity_key, current.entity_key) AS entity_key,
    current.resolved_id AS current_resolved_id,
    proposed.resolved_id AS proposed_resolved_id,
    CASE
        WHEN current.entity_key IS NULL THEN 'NEW'
        WHEN current.resolved_id = proposed.resolved_id THEN 'UNCHANGED'
        WHEN current.resolved_id <> proposed.resolved_id THEN 'MOVED'
    END AS change_type,
    current_clusters.cluster_size AS current_cluster_size,
    proposed_clusters.cluster_size AS proposed_cluster_size
FROM idr_work.membership_updates proposed
FULL OUTER JOIN idr_out.identity_resolved_membership_current current
    ON proposed.entity_key = current.entity_key
LEFT JOIN idr_out.identity_clusters_current current_clusters
    ON current.resolved_id = current_clusters.resolved_id
LEFT JOIN idr_work.cluster_sizes_updates proposed_clusters
    ON proposed.resolved_id = proposed_clusters.resolved_id;
```

---

### Console Output (Dry Run)

```
============================================================
DRY RUN SUMMARY
============================================================
Run ID:           dry_run_2024-01-15_143022
Mode:             INCR (Dry Run)
Status:           DRY_RUN_COMPLETE

IMPACT PREVIEW:
  Entities Affected:    2,847
  └─ New Entities:        412
  └─ Moved Entities:      189
  └─ Unchanged:         2,246

CLUSTER CHANGES:
  Merges:                 23  (clusters combining)
  Largest Proposed:    1,247  entities

EDGES:
  Would Create:        3,421
  Would Skip:             47  (max_group_size)
  Would Exclude:          12  (exclusion list)

⚠️  THIS WAS A DRY RUN - NO CHANGES COMMITTED
    Review: SELECT * FROM idr_out.dry_run_results 
            WHERE run_id = 'dry_run_2024-01-15_143022'
            AND change_type = 'MOVED'
============================================================
```

---

### Dry Run Retention & Cleanup

| Strategy | SQL |
|----------|-----|
| **Auto-expire after 7 days** | `DELETE FROM idr_out.dry_run_results WHERE created_at < NOW() - INTERVAL 7 DAY` |
| **Keep last N runs** | Cron job or scheduled query |
| **Manual cleanup** | `DELETE FROM idr_out.dry_run_results WHERE run_id = 'xyz'` |

---

## 2. Metrics Export

### Use Cases

| Scenario | Who | Why |
|----------|-----|-----|
| **Real-time Dashboard** | Ops Team | Monitor run health, duration trends |
| **Alerting** | On-call Engineer | Page when failures or anomalies occur |
| **Capacity Planning** | Platform Team | Track growth, plan infrastructure |
| **Business Reporting** | Data Analyst | Report on data quality, cluster trends |

---

### Metrics Taxonomy

#### Run Metrics (per execution)

| Metric | Type | Description |
|--------|------|-------------|
| `idr_run_duration_seconds` | Gauge | Total run time |
| `idr_run_status` | Enum | SUCCESS, SUCCESS_WITH_WARNINGS, FAILED |
| `idr_entities_processed` | Counter | Entities in delta |
| `idr_edges_created` | Counter | New edges |
| `idr_edges_updated` | Counter | Updated edges |
| `idr_lp_iterations` | Gauge | Label propagation iterations |
| `idr_groups_skipped` | Counter | Groups exceeding max_group_size |
| `idr_values_excluded` | Counter | Values matching exclusion list |

#### Cluster Metrics (point-in-time)

| Metric | Type | Description |
|--------|------|-------------|
| `idr_total_clusters` | Gauge | Total unique clusters |
| `idr_total_entities_resolved` | Gauge | Total entities in membership |
| `idr_largest_cluster_size` | Gauge | Size of biggest cluster |
| `idr_clusters_over_1000` | Gauge | Count of large clusters |
| `idr_singleton_count` | Gauge | Entities in size-1 clusters |
| `idr_avg_cluster_size` | Gauge | Mean cluster size |

#### Stage Metrics (per stage per run)

| Metric | Type | Description |
|--------|------|-------------|
| `idr_stage_duration_seconds` | Gauge | Time per stage |
| `idr_stage_rows_processed` | Counter | Rows in/out per stage |

---

### Schema: `idr_out.metrics_export`

```sql
CREATE TABLE idr_out.metrics_export (
    metric_id VARCHAR DEFAULT uuid(),
    run_id VARCHAR,
    metric_name VARCHAR NOT NULL,
    metric_value DOUBLE NOT NULL,
    metric_type VARCHAR DEFAULT 'gauge',    -- 'gauge', 'counter', 'histogram'
    dimensions VARCHAR,                      -- JSON: {"stage": "edges", "rule": "EMAIL"}
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    exported_at TIMESTAMP,                   -- NULL until exported
    PRIMARY KEY (metric_id)
);

-- Index for efficient export queries
CREATE INDEX idx_metrics_not_exported 
ON idr_out.metrics_export(exported_at) 
WHERE exported_at IS NULL;
```

---

### Metrics Collection (Runner Code)

```python
def record_metric(name: str, value: float, dimensions: dict = None):
    """Insert metric into metrics_export table."""
    dim_json = json.dumps(dimensions) if dimensions else None
    q(f"""
    INSERT INTO idr_out.metrics_export (run_id, metric_name, metric_value, dimensions)
    VALUES ('{RUN_ID}', '{name}', {value}, {f"'{dim_json}'" if dim_json else 'NULL'})
    """)

# Usage in runner:
record_metric('idr_run_duration_seconds', duration)
record_metric('idr_entities_processed', entities_count)
record_metric('idr_stage_duration_seconds', stage_time, {'stage': 'label_propagation'})
record_metric('idr_clusters_over_1000', large_cluster_count)
```

---

### Export Mechanisms

#### Option A: Pull-Based (Prometheus)

**Exporter Script** (runs on schedule or as sidecar):

```python
# prometheus_exporter.py
from prometheus_client import Gauge, start_http_server
import duckdb  # or snowflake.connector, google.cloud.bigquery

# Define metrics
run_duration = Gauge('idr_run_duration_seconds', 'IDR run duration', ['run_id'])
entities_processed = Gauge('idr_entities_processed', 'Entities processed', ['run_id'])
clusters_large = Gauge('idr_clusters_over_1000', 'Large clusters')

def collect_metrics():
    """Fetch latest metrics from database."""
    conn = duckdb.connect('idr.duckdb')
    
    # Get latest run metrics
    latest = conn.execute("""
        SELECT metric_name, metric_value, run_id 
        FROM idr_out.metrics_export 
        WHERE recorded_at > NOW() - INTERVAL 1 HOUR
    """).fetchall()
    
    for name, value, run_id in latest:
        if name == 'idr_run_duration_seconds':
            run_duration.labels(run_id=run_id).set(value)
        elif name == 'idr_entities_processed':
            entities_processed.labels(run_id=run_id).set(value)
        # ... etc
    
    conn.close()

if __name__ == '__main__':
    start_http_server(9090)  # Prometheus scrapes this
    while True:
        collect_metrics()
        time.sleep(60)
```

**Prometheus Config**:
```yaml
scrape_configs:
  - job_name: 'idr_exporter'
    static_configs:
      - targets: ['localhost:9090']
    scrape_interval: 60s
```

---

#### Option B: Push-Based (DataDog/CloudWatch)

**Push after each run**:

```python
# At end of runner
def export_metrics_to_datadog():
    """Push metrics to DataDog API."""
    import datadog
    datadog.initialize(api_key=os.environ['DD_API_KEY'])
    
    metrics = collect("""
        SELECT metric_name, metric_value, dimensions 
        FROM idr_out.metrics_export 
        WHERE run_id = '{RUN_ID}' AND exported_at IS NULL
    """)
    
    for m in metrics:
        datadog.api.Metric.send(
            metric=m['metric_name'],
            points=[(time.time(), m['metric_value'])],
            tags=[f"run_id:{RUN_ID}", f"env:{ENV}"]
        )
    
    # Mark as exported
    q(f"UPDATE idr_out.metrics_export SET exported_at = NOW() WHERE run_id = '{RUN_ID}'")
```

---

#### Option C: Webhook (Generic)

**JSON payload to any endpoint**:

```python
def export_metrics_webhook(webhook_url: str):
    """POST metrics to webhook endpoint."""
    metrics = collect("""
        SELECT metric_name, metric_value, dimensions, recorded_at
        FROM idr_out.metrics_export WHERE run_id = '{RUN_ID}'
    """)
    
    payload = {
        "run_id": RUN_ID,
        "timestamp": datetime.utcnow().isoformat(),
        "status": run_status,
        "metrics": [
            {"name": m['metric_name'], "value": m['metric_value'], 
             "dimensions": json.loads(m['dimensions'] or '{}')}
            for m in metrics
        ]
    }
    
    requests.post(webhook_url, json=payload, timeout=10)
```

---

### Platform-Specific Integration

| Platform | Best Export Method | Notes |
|----------|-------------------|-------|
| **DuckDB** | Prometheus exporter + Grafana | Lightweight, local setup |
| **Databricks** | Pushgateway or workspace metrics | Integrate with cluster metrics |
| **Snowflake** | Snowflake → S3 → CloudWatch | Or use Snowflake partner connectors |
| **BigQuery** | BigQuery → Pub/Sub → Cloud Monitoring | Native GCP integration |

---

### Alerting Rules (Grafana/PagerDuty)

```yaml
# Grafana alert rules
groups:
  - name: idr_alerts
    rules:
      - alert: IDRRunFailed
        expr: idr_run_status{status="FAILED"} > 0
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "IDR run failed: {{ $labels.run_id }}"
          
      - alert: IDRLargeClusterCreated
        expr: idr_largest_cluster_size > 5000
        for: 0m
        labels:
          severity: warning
        annotations:
          summary: "Large cluster detected: {{ $value }} entities"
          
      - alert: IDRHighSkipRate
        expr: idr_groups_skipped / idr_entities_processed > 0.1
        for: 0m
        labels:
          severity: warning
        annotations:
          summary: ">10% of groups skipped due to max_group_size"
```

---

## Implementation Phases

### Phase 1: Core Infrastructure
- [ ] Add `dry_run_results` and `dry_run_summary` tables (all platforms)
- [ ] Add `metrics_export` table (all platforms)
- [ ] Add `DRY_RUN` parameter to all runners

### Phase 2: Dry Run Logic
- [ ] Implement diff computation in DuckDB runner
- [ ] Port to Snowflake, BigQuery, Databricks
- [ ] Add dry run console output format

### Phase 3: Metrics Collection
- [ ] Add `record_metric()` helper to all runners
- [ ] Instrument all stages with timing + counts
- [ ] Test metric data accumulation

### Phase 4: Export Integration
- [ ] Create Prometheus exporter script
- [ ] Create DataDog push script
- [ ] Create generic webhook exporter
- [ ] Document Grafana dashboard setup

---

## Estimated Effort

| Component | DuckDB | Snowflake | BigQuery | Databricks | Total |
|-----------|--------|-----------|----------|------------|-------|
| DDL Changes | 1h | 1h | 1h | 1h | 4h |
| Dry Run Logic | 4h | 3h | 3h | 3h | 13h |
| Metrics Collection | 2h | 2h | 2h | 2h | 8h |
| Export Scripts | 3h | - | - | - | 3h |
| Testing | 2h | 2h | 2h | 2h | 8h |
| Documentation | 2h | - | - | - | 2h |
| **Total** | **14h** | **8h** | **8h** | **8h** | **38h** |

---

## Questions for Review

1. **Dry Run Retention**: 7 days default, or configurable per customer?
2. **Metrics Granularity**: Per-rule metrics, or just aggregate?
3. **Export Priority**: Prometheus first, or DataDog/CloudWatch?
4. **Alerting Thresholds**: What defines "large cluster" (1000? 5000? 10000)?
