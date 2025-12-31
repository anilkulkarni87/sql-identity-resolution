-- Snowflake DDL: All metadata and output tables
-- Run this first to set up the IDR schema

-- ============================================
-- METADATA SCHEMA
-- ============================================
CREATE SCHEMA IF NOT EXISTS idr_meta;

CREATE TABLE IF NOT EXISTS idr_meta.source_table (
  table_id VARCHAR,
  table_fqn VARCHAR,
  entity_type VARCHAR,
  entity_key_expr VARCHAR,
  watermark_column VARCHAR,
  watermark_lookback_minutes INTEGER,
  is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS idr_meta.run_state (
  table_id VARCHAR,
  last_watermark_value TIMESTAMP_NTZ,
  last_run_id VARCHAR,
  last_run_ts TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS idr_meta.source (
  table_id VARCHAR,
  source_name VARCHAR,
  trust_rank INTEGER,
  is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS idr_meta.rule (
  rule_id VARCHAR,
  rule_name VARCHAR,
  is_active BOOLEAN,
  priority INTEGER,
  identifier_type VARCHAR,
  canonicalize VARCHAR,
  allow_hashed BOOLEAN,
  require_non_null BOOLEAN
);

CREATE TABLE IF NOT EXISTS idr_meta.identifier_mapping (
  table_id VARCHAR,
  identifier_type VARCHAR,
  identifier_value_expr VARCHAR,
  is_hashed BOOLEAN
);

CREATE TABLE IF NOT EXISTS idr_meta.entity_attribute_mapping (
  table_id VARCHAR,
  attribute_name VARCHAR,
  attribute_expr VARCHAR
);

CREATE TABLE IF NOT EXISTS idr_meta.survivorship_rule (
  attribute_name VARCHAR,
  strategy VARCHAR,
  source_priority_list VARCHAR,
  recency_field VARCHAR
);

-- ============================================
-- WORK SCHEMA (transient tables)
-- ============================================
CREATE SCHEMA IF NOT EXISTS idr_work;

-- Work tables are created as TRANSIENT for performance
-- They are recreated each run

-- ============================================
-- OUTPUT SCHEMA
-- ============================================
CREATE SCHEMA IF NOT EXISTS idr_out;

CREATE TABLE IF NOT EXISTS idr_out.identity_edges_current (
  rule_id VARCHAR,
  left_entity_key VARCHAR,
  right_entity_key VARCHAR,
  identifier_type VARCHAR,
  identifier_value_norm VARCHAR,
  first_seen_ts TIMESTAMP_NTZ,
  last_seen_ts TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS idr_out.identity_resolved_membership_current (
  entity_key VARCHAR,
  resolved_id VARCHAR,
  updated_ts TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS idr_out.identity_clusters_current (
  resolved_id VARCHAR,
  cluster_size INTEGER,
  updated_ts TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS idr_out.golden_profile_current (
  resolved_id VARCHAR,
  email_primary VARCHAR,
  phone_primary VARCHAR,
  first_name VARCHAR,
  last_name VARCHAR,
  updated_ts TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS idr_out.rule_match_audit_current (
  run_id VARCHAR,
  rule_id VARCHAR,
  edges_created INTEGER,
  started_at TIMESTAMP_NTZ,
  ended_at TIMESTAMP_NTZ
);

-- ============================================
-- OBSERVABILITY TABLES
-- ============================================
CREATE TABLE IF NOT EXISTS idr_out.run_history (
  run_id VARCHAR,
  run_mode VARCHAR,
  status VARCHAR,
  started_at TIMESTAMP_NTZ,
  ended_at TIMESTAMP_NTZ,
  duration_seconds INTEGER,
  entities_processed INTEGER,
  edges_created INTEGER,
  edges_updated INTEGER,
  clusters_impacted INTEGER,
  lp_iterations INTEGER,
  source_tables_processed INTEGER,
  watermarks_json VARCHAR,
  error_message VARCHAR,
  error_stage VARCHAR,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS idr_out.stage_metrics (
  run_id VARCHAR,
  stage_name VARCHAR,
  stage_order INTEGER,
  started_at TIMESTAMP_NTZ,
  ended_at TIMESTAMP_NTZ,
  duration_seconds INTEGER,
  rows_in INTEGER,
  rows_out INTEGER,
  notes VARCHAR
);
