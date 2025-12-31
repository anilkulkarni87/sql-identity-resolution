# Runbook

Operational guide for running sql-identity-resolution in production.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Operational Procedures](#operational-procedures)
3. [Monitoring](#monitoring)
4. [Troubleshooting](#troubleshooting)
5. [Incident Response](#incident-response)

---

## Quick Start

### Databricks

```bash
# 1. Load metadata
Run: sql/databricks/notebooks/IDR_LoadMetadata_Simple.py

# 2. Generate sample data (optional)
Run: sql/databricks/notebooks/IDR_SampleData_Generate.py

# 3. Run IDR
Run: IDR_Run.py with RUN_MODE=FULL, MAX_ITERS=30

# 4. Verify
Run: sql/databricks/notebooks/IDR_SmokeTest.py
```

### Snowflake

```sql
-- 1. Setup
\i sql/snowflake/00_ddl_all.sql
\i sql/snowflake/IDR_SampleData_Generate.sql

-- 2. Run
CALL idr_run('FULL', 30);
```

### BigQuery

```bash
# 1. Setup
bq query < sql/bigquery/00_ddl_all.sql
bq query < sql/bigquery/idr_sample_data.sql

# 2. Run
python sql/bigquery/idr_run.py --project=your-project --run-mode=FULL
```

### DuckDB (Local/Testing)

```bash
# 1. Setup
duckdb idr.duckdb < sql/duckdb/00_ddl_all.sql
python sql/duckdb/idr_sample_data.py --db=idr.duckdb

# 2. Run
python sql/duckdb/idr_run.py --db=idr.duckdb --run-mode=FULL
```

---

## Operational Procedures

### Full vs Incremental Runs

| Mode | When to Use | Impact |
|------|-------------|--------|
| `FULL` | Initial load, after rule changes, monthly refresh | Re-processes all data |
| `INCR` | Daily/hourly runs | Only processes delta since last watermark |

**Recommended schedule:**
```
Daily:   RUN_MODE=INCR (process deltas)
Weekly:  Optional FULL refresh for data hygiene
Monthly: RUN_MODE=FULL (verify consistency)
```

### Adding New Sources

1. **Create source table entry:**
```sql
INSERT INTO idr_meta.source_table VALUES
    ('new_source', 'catalog.schema.table', 'PERSON', 'customer_id', 'updated_at', 0, TRUE);
```

2. **Add trust ranking:**
```sql
INSERT INTO idr_meta.source VALUES
    ('new_source', 'New Source Name', 5, TRUE);  -- trust_rank: lower = more trusted
```

3. **Map identifiers:**
```sql
INSERT INTO idr_meta.identifier_mapping VALUES
    ('new_source', 'EMAIL', 'email_column', FALSE),
    ('new_source', 'PHONE', 'phone_column', FALSE);
```

4. **Map attributes (for golden profile):**
```sql
INSERT INTO idr_meta.entity_attribute_mapping VALUES
    ('new_source', 'email_raw', 'email_column'),
    ('new_source', 'first_name', 'first_name_column');
```

5. **Run FULL refresh:**
```sql
-- Run with RUN_MODE=FULL to incorporate new source
```

### Adding New Rules

```sql
-- Add a new identifier type
INSERT INTO idr_meta.rule VALUES
    ('R_SSN_EXACT', 'SSN exact match', TRUE, 4, 'SSN', 'NONE', FALSE, TRUE);

-- Map which sources have this identifier
INSERT INTO idr_meta.identifier_mapping VALUES
    ('existing_source', 'SSN', 'ssn_column', TRUE);  -- is_hashed=TRUE if sensitive

-- Run FULL refresh
```

### Schema Migrations

> [!WARNING]
> Always backup before schema changes.

1. **Backup current state:**
```sql
CREATE TABLE idr_out.membership_backup_YYYYMMDD AS 
SELECT * FROM idr_out.identity_resolved_membership_current;
```

2. **Apply DDL changes**

3. **Run FULL refresh to rebuild outputs**

4. **Verify with smoke test**

---

## Monitoring

### Key Metrics

| Metric | Query | Alert Threshold |
|--------|-------|-----------------|
| Run duration | `SELECT duration_seconds FROM idr_out.run_history ORDER BY started_at DESC LIMIT 1` | > 2x average |
| Entities processed | `SELECT entities_processed FROM idr_out.run_history` | 0 (unexpected) |
| LP iterations | `SELECT lp_iterations FROM idr_out.run_history` | > 20 |
| Failed runs | `SELECT * FROM idr_out.run_history WHERE status = 'FAILED'` | Any |

### Health Check Queries

**Cluster size distribution:**
```sql
SELECT 
  CASE 
    WHEN cluster_size = 1 THEN '1 (singletons)'
    WHEN cluster_size <= 5 THEN '2-5'
    WHEN cluster_size <= 20 THEN '6-20'
    WHEN cluster_size <= 100 THEN '21-100'
    ELSE '100+'
  END AS size_bucket,
  COUNT(*) AS cluster_count,
  SUM(cluster_size) AS total_entities
FROM idr_out.identity_clusters_current
GROUP BY 1
ORDER BY 1;
```

**Identify problematic identifiers:**
```sql
-- Find identifiers linking too many entities (data quality issue)
SELECT identifier_type, identifier_value_norm, COUNT(*) AS entity_count
FROM idr_work.identifiers_all
GROUP BY 1, 2
HAVING COUNT(*) > 1000
ORDER BY entity_count DESC
LIMIT 20;
```

**Run history trends:**
```sql
SELECT 
  DATE(started_at) AS run_date,
  COUNT(*) AS runs,
  AVG(duration_seconds) AS avg_duration,
  SUM(entities_processed) AS total_entities,
  SUM(edges_created) AS total_edges
FROM idr_out.run_history
WHERE started_at > CURRENT_DATE - INTERVAL 7 DAY
GROUP BY 1
ORDER BY 1;
```

**Watermark status:**
```sql
SELECT 
  st.table_id,
  st.table_fqn,
  rs.last_watermark_value,
  rs.last_run_ts,
  TIMESTAMPDIFF(HOUR, rs.last_run_ts, CURRENT_TIMESTAMP) AS hours_since_run
FROM idr_meta.source_table st
LEFT JOIN idr_meta.run_state rs ON rs.table_id = st.table_id
WHERE st.is_active = TRUE
ORDER BY hours_since_run DESC;
```

---

## Troubleshooting

### Common Issues

#### 1. "Schemas not created" Error
**Cause:** DDL scripts not run before IDR execution.

**Solution:**
```bash
# Run DDL first
# Databricks: Run IDR_QuickStart.py or 00_ddl scripts
# Snowflake: \i sql/snowflake/00_ddl_all.sql
# DuckDB: duckdb mydb.duckdb < sql/duckdb/00_ddl_all.sql
```

---

#### 2. No Entities Processed
**Cause:** Watermark ahead of source data timestamps.

**Diagnostic:**
```sql
SELECT table_id, last_watermark_value 
FROM idr_meta.run_state;

SELECT MAX(updated_at) as max_ts 
FROM your_source_table;
```

**Solution:**
```sql
-- Reset watermark to force re-processing
UPDATE idr_meta.run_state 
SET last_watermark_value = TIMESTAMP '1900-01-01'
WHERE table_id = 'your_table_id';
```

---

#### 3. Giant Clusters (10K+ entities)
**Cause:** Bad identifier data (NULL, empty, or generic values like "test@test.com").

**Diagnostic:**
```sql
SELECT identifier_value_norm, COUNT(*) as cnt
FROM idr_work.identifiers_all
GROUP BY 1
HAVING COUNT(*) > 1000
ORDER BY cnt DESC;
```

**Solution:**
1. Add data quality rules in source ETL
2. Enable `require_non_null = TRUE` in rules
3. Add exclusion list for known bad values:
```sql
-- Exclude in canonicalization or identifier extraction
WHERE identifier_value NOT IN ('test@test.com', 'null', 'N/A')
```

---

#### 4. Label Propagation Not Converging
**Cause:** Extremely connected graph or circular references.

**Diagnostic:**
```sql
SELECT lp_iterations 
FROM idr_out.run_history 
ORDER BY started_at DESC LIMIT 5;
```

**Solution:**
- Increase `MAX_ITERS` if approaching limit
- Investigate data quality—deep chains indicate identifier issues
- Consider adding `max_group_size` to rules

---

#### 5. Missing Entities in Membership
**Cause:** Singletons (entities without edges) not being added.

**Diagnostic:**
```sql
SELECT COUNT(*) FROM idr_work.entities_delta
WHERE entity_key NOT IN (
  SELECT entity_key FROM idr_out.identity_resolved_membership_current
);
```

**Solution:** Ensure runner includes singleton handling (fixed in Dec 2024):
```sql
-- Singletons get resolved_id = entity_key
INSERT INTO membership
SELECT entity_key, entity_key AS resolved_id
FROM entities_delta
WHERE entity_key NOT IN (SELECT entity_key FROM lp_labels);
```

---

#### 6. Slow Performance
**Causes:**
- Large identifier groups
- Unoptimized tables
- Insufficient cluster resources

**Solutions:**

**Optimize tables (Databricks):**
```sql
OPTIMIZE idr_work.identifiers_all 
ZORDER BY (identifier_type, identifier_value_norm);
```

**Increase parallelism:**
```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

**Right-size cluster:** See [cluster_sizing.md](cluster_sizing.md)

---

## Incident Response

### Failed Run Investigation

1. **Check run history:**
```sql
SELECT run_id, status, started_at, ended_at, 
       entities_processed, edges_created, lp_iterations
FROM idr_out.run_history
ORDER BY started_at DESC
LIMIT 10;
```

2. **Check for partial outputs:**
```sql
SELECT COUNT(*) FROM idr_work.entities_delta;
SELECT COUNT(*) FROM idr_work.edges_new;
SELECT COUNT(*) FROM idr_work.lp_labels;
```

3. **Review error logs** (platform-specific)

### Rollback Procedure

> [!CAUTION]
> Rollback does not restore deleted edges. Consider before proceeding.

1. **Restore membership from last known good state:**
```sql
-- If you have backups
TRUNCATE TABLE idr_out.identity_resolved_membership_current;
INSERT INTO idr_out.identity_resolved_membership_current
SELECT * FROM idr_out.membership_backup_YYYYMMDD;
```

2. **Reset run state:**
```sql
UPDATE idr_meta.run_state
SET last_watermark_value = TIMESTAMP 'YYYY-MM-DD HH:MM:SS',
    last_run_id = 'rollback_manual'
WHERE table_id IN ('affected_tables');
```

3. **Re-run with FULL mode** after fixing root cause

### Data Recovery

If output tables are corrupted:

1. **Rebuild from scratch:**
```sql
-- 1. Truncate outputs
TRUNCATE TABLE idr_out.identity_edges_current;
TRUNCATE TABLE idr_out.identity_resolved_membership_current;
TRUNCATE TABLE idr_out.identity_clusters_current;
TRUNCATE TABLE idr_out.golden_profile_current;

-- 2. Reset all watermarks
UPDATE idr_meta.run_state SET last_watermark_value = TIMESTAMP '1900-01-01';

-- 3. Run FULL refresh
```

2. **Verify integrity:**
```sql
-- Check all entities have membership
SELECT COUNT(*) as orphaned_entities
FROM idr_work.entities_delta e
WHERE NOT EXISTS (
  SELECT 1 FROM idr_out.identity_resolved_membership_current m
  WHERE m.entity_key = e.entity_key
);
```

---

## Contact & Escalation

For issues not covered here:
1. Review [architecture.md](architecture.md) for design context
2. Check [scale_considerations.md](scale_considerations.md) for performance
3. Review platform-specific testing guides: `testing_*.md`
