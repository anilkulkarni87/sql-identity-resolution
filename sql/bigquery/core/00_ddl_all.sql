-- BigQuery DDL: All metadata and output tables
-- Run this first to set up the IDR datasets
--
-- Usage:
--   1. Replace PROJECT_ID with your GCP project
--   2. Run in BigQuery console or bq CLI

-- ============================================
-- METADATA DATASET
-- ============================================
CREATE SCHEMA IF NOT EXISTS idr_meta;

CREATE TABLE IF NOT EXISTS idr_meta.source_table (
  table_id STRING,
  table_fqn STRING,          -- project.dataset.table format
  entity_type STRING,
  entity_key_expr STRING,
  watermark_column STRING,
  watermark_lookback_minutes INT64,
  is_active BOOL
);

CREATE TABLE IF NOT EXISTS idr_meta.run_state (
  table_id STRING,
  last_watermark_value TIMESTAMP,
  last_run_id STRING,
  last_run_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idr_meta.source (
  table_id STRING,
  source_name STRING,
  trust_rank INT64,
  is_active BOOL
);

CREATE TABLE IF NOT EXISTS idr_meta.rule (
  rule_id STRING,
  rule_name STRING,
  is_active BOOL,
  priority INT64,
  identifier_type STRING,
  canonicalize STRING,
  allow_hashed BOOL,
  require_non_null BOOL,
  max_group_size INT64  -- Skip identifier groups larger than this (default 10000)
);

CREATE TABLE IF NOT EXISTS idr_meta.identifier_mapping (
  table_id STRING,
  identifier_type STRING,
  identifier_value_expr STRING,
  is_hashed BOOL
);

CREATE TABLE IF NOT EXISTS idr_meta.entity_attribute_mapping (
  table_id STRING,
  attribute_name STRING,
  attribute_expr STRING
);

CREATE TABLE IF NOT EXISTS idr_meta.survivorship_rule (
  attribute_name STRING,
  strategy STRING,
  source_priority_list STRING,
  recency_field STRING
);

-- Exclusion list for known bad identifier values
CREATE TABLE IF NOT EXISTS idr_meta.identifier_exclusion (
  identifier_type STRING,
  identifier_value_pattern STRING,
  match_type STRING,  -- EXACT or LIKE
  reason STRING,
  created_at TIMESTAMP,
  created_by STRING
);

-- ============================================
-- WORK DATASET (temporary tables)
-- ============================================
CREATE SCHEMA IF NOT EXISTS idr_work;

-- Work tables are created as needed and replaced each run

-- ============================================
-- OUTPUT DATASET
-- ============================================
CREATE SCHEMA IF NOT EXISTS idr_out;

CREATE TABLE IF NOT EXISTS idr_out.identity_edges_current (
  rule_id STRING,
  left_entity_key STRING,
  right_entity_key STRING,
  identifier_type STRING,
  identifier_value_norm STRING,
  first_seen_ts TIMESTAMP,
  last_seen_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idr_out.identity_resolved_membership_current (
  entity_key STRING,
  resolved_id STRING,
  updated_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idr_out.identity_clusters_current (
  resolved_id STRING,
  cluster_size INT64,
  updated_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idr_out.golden_profile_current (
  resolved_id STRING,
  email_primary STRING,
  phone_primary STRING,
  first_name STRING,
  last_name STRING,
  updated_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idr_out.rule_match_audit_current (
  run_id STRING,
  rule_id STRING,
  edges_created INT64,
  started_at TIMESTAMP,
  ended_at TIMESTAMP
);

-- Audit table for identifier groups that exceeded max_group_size
CREATE TABLE IF NOT EXISTS idr_out.skipped_identifier_groups (
  run_id STRING,
  identifier_type STRING,
  identifier_value_norm STRING,
  group_size INT64,
  max_allowed INT64,
  sample_entity_keys STRING,  -- JSON array of sample entity keys
  reason STRING,
  skipped_at TIMESTAMP
);

-- ============================================
-- OBSERVABILITY TABLES
-- ============================================
CREATE TABLE IF NOT EXISTS idr_out.run_history (
  run_id STRING,
  run_mode STRING,
  status STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_seconds INT64,
  entities_processed INT64,
  edges_created INT64,
  edges_updated INT64,
  clusters_impacted INT64,
  lp_iterations INT64,
  source_tables_processed INT64,
  groups_skipped INT64,
  values_excluded INT64,
  large_clusters INT64,
  warnings STRING,
  watermarks_json STRING,
  error_message STRING,
  error_stage STRING,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idr_out.stage_metrics (
  run_id STRING,
  stage_name STRING,
  stage_order INT64,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_seconds INT64,
  rows_in INT64,
  rows_out INT64,
  notes STRING
);

-- ============================================
-- CONFIGURATION TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS idr_meta.config (
  config_key STRING NOT NULL,
  config_value STRING NOT NULL,
  description STRING,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_by STRING
);

-- Insert default configuration values (run separately after table creation)
-- MERGE INTO idr_meta.config AS tgt
-- USING (SELECT 'large_cluster_threshold' AS config_key, '5000' AS config_value, 'Cluster size threshold' AS description) AS src
-- ON tgt.config_key = src.config_key
-- WHEN NOT MATCHED THEN INSERT (config_key, config_value, description) VALUES (src.config_key, src.config_value, src.description);

-- ============================================
-- DRY RUN TABLES
-- ============================================
CREATE TABLE IF NOT EXISTS idr_out.dry_run_results (
  run_id STRING NOT NULL,
  entity_key STRING NOT NULL,
  current_resolved_id STRING,
  proposed_resolved_id STRING,
  change_type STRING,
  current_cluster_size INT64,
  proposed_cluster_size INT64,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS idr_out.dry_run_summary (
  run_id STRING NOT NULL,
  total_entities INT64,
  new_entities INT64,
  moved_entities INT64,
  merged_clusters INT64,
  split_clusters INT64,
  unchanged_entities INT64,
  largest_proposed_cluster INT64,
  edges_would_create INT64,
  groups_would_skip INT64,
  values_would_exclude INT64,
  execution_time_seconds INT64,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- METRICS EXPORT TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS idr_out.metrics_export (
  metric_id STRING DEFAULT GENERATE_UUID(),
  run_id STRING,
  metric_name STRING NOT NULL,
  metric_value FLOAT64 NOT NULL,
  metric_type STRING DEFAULT 'gauge',
  dimensions STRING,
  recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  exported_at TIMESTAMP
);
