-- Common DDL (Databricks + Snowflake friendly). Run in both engines.
CREATE SCHEMA IF NOT EXISTS idr_meta;
CREATE SCHEMA IF NOT EXISTS idr_work;
CREATE SCHEMA IF NOT EXISTS idr_out;

CREATE TABLE IF NOT EXISTS idr_meta.source_table (
  table_id STRING,
  table_fqn STRING,
  entity_type STRING,
  entity_key_expr STRING,
  watermark_column STRING,
  watermark_lookback_minutes INT,
  is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS idr_meta.run_state (
  table_id STRING,
  last_watermark_value TIMESTAMP,
  last_run_id STRING,
  last_run_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idr_meta.rule (
  rule_id STRING,
  rule_name STRING,
  is_active BOOLEAN,
  priority INT,
  identifier_type STRING,
  canonicalize STRING, -- NONE | LOWERCASE
  allow_hashed BOOLEAN,
  require_non_null BOOLEAN
);

-- Mapping: how to extract identifiers from each source table
CREATE TABLE IF NOT EXISTS idr_meta.identifier_mapping (
  table_id STRING,
  identifier_type STRING,
  identifier_value_expr STRING,
  is_hashed BOOLEAN
);

-- Mapping: how to extract entity attributes used in golden profile
CREATE TABLE IF NOT EXISTS idr_meta.entity_attribute_mapping (
  table_id STRING,
  attribute_name STRING,
  attribute_expr STRING
);

CREATE TABLE IF NOT EXISTS idr_meta.source (
  table_id STRING,
  source_name STRING,
  trust_rank INT,
  is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS idr_meta.survivorship_rule (
  attribute_name STRING,
  strategy STRING,
  source_priority_list STRING,
  recency_field STRING
);

CREATE TABLE IF NOT EXISTS idr_meta.run_log (
  run_id STRING,
  run_ts TIMESTAMP,
  run_mode STRING,
  status STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  notes STRING
);
