# Schema Reference

Complete DDL reference for all SQL Identity Resolution tables.

---

## Quick Links

- [idr_meta Schema](#idr_meta-configuration)
- [idr_work Schema](#idr_work-processing)
- [idr_out Schema](#idr_out-output)

---

## idr_meta (Configuration)

### source_table

```sql
CREATE TABLE idr_meta.source_table (
    table_id VARCHAR PRIMARY KEY,
    table_fqn VARCHAR NOT NULL,
    entity_type VARCHAR NOT NULL,
    entity_key_expr VARCHAR NOT NULL,
    watermark_column VARCHAR NOT NULL,
    watermark_lookback_minutes INT DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE
);
```

### rule

```sql
CREATE TABLE idr_meta.rule (
    rule_id VARCHAR PRIMARY KEY,
    identifier_type VARCHAR NOT NULL,
    priority INT NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    max_group_size INT DEFAULT 10000
);
```

### identifier_mapping

```sql
CREATE TABLE idr_meta.identifier_mapping (
    table_id VARCHAR NOT NULL,
    identifier_type VARCHAR NOT NULL,
    column_expr VARCHAR NOT NULL,
    requires_normalization BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (table_id, identifier_type, column_expr)
);
```

### run_state

```sql
CREATE TABLE idr_meta.run_state (
    table_id VARCHAR PRIMARY KEY,
    last_watermark_value TIMESTAMP,
    last_run_id VARCHAR,
    last_run_ts TIMESTAMP
);
```

### config

```sql
CREATE TABLE idr_meta.config (
    config_key VARCHAR PRIMARY KEY,
    config_value VARCHAR NOT NULL,
    description VARCHAR,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Default values
INSERT INTO idr_meta.config VALUES
    ('dry_run_retention_days', '7', 'Days to retain dry run results', CURRENT_TIMESTAMP),
    ('large_cluster_threshold', '5000', 'Threshold for large cluster warnings', CURRENT_TIMESTAMP);
```

### identifier_exclusion

```sql
CREATE TABLE idr_meta.identifier_exclusion (
    identifier_type VARCHAR NOT NULL,
    identifier_pattern VARCHAR NOT NULL,
    is_pattern BOOLEAN DEFAULT FALSE,
    reason VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (identifier_type, identifier_pattern)
);
```

---

## idr_work (Processing)

These are transient/temporary tables created during processing.

### entities_delta

```sql
CREATE TABLE idr_work.entities_delta (
    entity_key VARCHAR PRIMARY KEY,
    table_id VARCHAR NOT NULL,
    watermark_value TIMESTAMP NOT NULL
);
```

### identifiers

```sql
CREATE TABLE idr_work.identifiers (
    entity_key VARCHAR NOT NULL,
    identifier_type VARCHAR NOT NULL,
    identifier_value_raw VARCHAR,
    identifier_value_norm VARCHAR NOT NULL
);
```

### edges_new

```sql
CREATE TABLE idr_work.edges_new (
    entity_a VARCHAR NOT NULL,
    entity_b VARCHAR NOT NULL,
    identifier_type VARCHAR NOT NULL,
    PRIMARY KEY (entity_a, entity_b, identifier_type)
);
```

### lp_labels

```sql
CREATE TABLE idr_work.lp_labels (
    entity_key VARCHAR PRIMARY KEY,
    label VARCHAR NOT NULL
);
```

### membership_updates

```sql
CREATE TABLE idr_work.membership_updates (
    entity_key VARCHAR PRIMARY KEY,
    resolved_id VARCHAR NOT NULL,
    updated_ts TIMESTAMP NOT NULL
);
```

### cluster_sizes_updates

```sql
CREATE TABLE idr_work.cluster_sizes_updates (
    resolved_id VARCHAR PRIMARY KEY,
    cluster_size INT NOT NULL,
    updated_ts TIMESTAMP NOT NULL
);
```

---

## idr_out (Output)

### identity_resolved_membership_current

```sql
CREATE TABLE idr_out.identity_resolved_membership_current (
    entity_key VARCHAR PRIMARY KEY,
    resolved_id VARCHAR NOT NULL,
    updated_ts TIMESTAMP NOT NULL
);
```

### identity_clusters_current

```sql
CREATE TABLE idr_out.identity_clusters_current (
    resolved_id VARCHAR PRIMARY KEY,
    cluster_size INT NOT NULL,
    updated_ts TIMESTAMP NOT NULL
);
```

### golden_profile_current

```sql
CREATE TABLE idr_out.golden_profile_current (
    resolved_id VARCHAR PRIMARY KEY,
    email_primary VARCHAR,
    phone_primary VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    updated_ts TIMESTAMP NOT NULL
);
```

### run_history

```sql
CREATE TABLE idr_out.run_history (
    run_id VARCHAR PRIMARY KEY,
    run_mode VARCHAR NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    status VARCHAR,
    entities_processed INT,
    edges_created INT,
    clusters_impacted INT,
    lp_iterations INT,
    duration_seconds INT,
    groups_skipped INT DEFAULT 0,
    values_excluded INT DEFAULT 0,
    large_clusters INT DEFAULT 0,
    warnings VARCHAR
);
```

### dry_run_results

```sql
CREATE TABLE idr_out.dry_run_results (
    run_id VARCHAR NOT NULL,
    entity_key VARCHAR NOT NULL,
    current_resolved_id VARCHAR,
    proposed_resolved_id VARCHAR,
    change_type VARCHAR NOT NULL,
    current_cluster_size INT,
    proposed_cluster_size INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, entity_key)
);
```

### dry_run_summary

```sql
CREATE TABLE idr_out.dry_run_summary (
    run_id VARCHAR PRIMARY KEY,
    total_entities INT NOT NULL,
    new_entities INT NOT NULL,
    moved_entities INT NOT NULL,
    unchanged_entities INT NOT NULL,
    merged_clusters INT DEFAULT 0,
    split_clusters INT DEFAULT 0,
    largest_proposed_cluster INT,
    edges_would_create INT,
    groups_would_skip INT,
    values_would_exclude INT,
    execution_time_seconds INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### metrics_export

```sql
CREATE TABLE idr_out.metrics_export (
    metric_id VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id VARCHAR NOT NULL,
    metric_name VARCHAR NOT NULL,
    metric_value DOUBLE NOT NULL,
    metric_type VARCHAR DEFAULT 'gauge',
    dimensions VARCHAR,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    exported_at TIMESTAMP
);
```

### skipped_identifier_groups

```sql
CREATE TABLE idr_out.skipped_identifier_groups (
    run_id VARCHAR NOT NULL,
    identifier_type VARCHAR NOT NULL,
    identifier_value_norm VARCHAR NOT NULL,
    group_size INT NOT NULL,
    max_allowed INT NOT NULL,
    sample_entity_keys VARCHAR,
    reason VARCHAR,
    skipped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, identifier_type, identifier_value_norm)
);
```

### stage_metrics

```sql
CREATE TABLE idr_out.stage_metrics (
    run_id VARCHAR NOT NULL,
    stage_name VARCHAR NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    rows_affected INT,
    duration_seconds INT,
    PRIMARY KEY (run_id, stage_name)
);
```

---

## Platform-Specific Notes

### DuckDB

- Uses `gen_random_uuid()` for UUID generation
- All types are standard SQL types

### Snowflake

- Uses `UUID_STRING()` for UUID generation
- `VARCHAR` maps to `VARCHAR`
- Transient tables for work schema

### BigQuery

- Uses `GENERATE_UUID()` for UUID generation
- `VARCHAR` maps to `STRING`
- `BOOLEAN` maps to `BOOL`
- `TIMESTAMP` maps to `TIMESTAMP`

### Databricks

- Uses `uuid()` for UUID generation
- Delta Lake format recommended
- Supports Unity Catalog

---

## Next Steps

- [CLI Reference](cli-reference.md)
- [Metrics Reference](metrics-reference.md)
- [Data Model](../concepts/data-model.md)
