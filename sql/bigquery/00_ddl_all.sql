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
  require_non_null BOOL
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
