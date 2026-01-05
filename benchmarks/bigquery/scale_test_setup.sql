-- ============================================
-- BigQuery IDR Setup Script for Scale Testing
-- Project: ga4-134745
-- Generated from DuckDB scale test configuration
-- ============================================

-- Step 1: Create datasets (run via bq CLI or console)
-- bq mk --dataset ga4-134745:idr_meta
-- bq mk --dataset ga4-134745:idr_work
-- bq mk --dataset ga4-134745:idr_out

-- ============================================
-- METADATA TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS `ga4-134745.idr_meta.source_table` (
    table_id STRING NOT NULL,
    table_fqn STRING NOT NULL,
    entity_type STRING DEFAULT 'PERSON',
    entity_key_expr STRING NOT NULL,
    watermark_column STRING NOT NULL,
    watermark_lookback_minutes INT64 DEFAULT 0,
    is_active BOOL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS `ga4-134745.idr_meta.source` (
    source_id STRING NOT NULL,
    source_name STRING NOT NULL,
    priority INT64 DEFAULT 1,
    is_active BOOL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS `ga4-134745.idr_meta.rule` (
    rule_id STRING NOT NULL,
    rule_description STRING,
    is_active BOOL DEFAULT TRUE,
    priority INT64 DEFAULT 1,
    identifier_type STRING NOT NULL,
    canonicalize STRING DEFAULT 'NONE',
    is_deterministic BOOL DEFAULT TRUE,
    is_transitive BOOL DEFAULT TRUE,
    max_group_size INT64 DEFAULT 10000
);

CREATE TABLE IF NOT EXISTS `ga4-134745.idr_meta.identifier_mapping` (
    table_id STRING NOT NULL,
    identifier_type STRING NOT NULL,
    identifier_value_expr STRING NOT NULL,
    is_hashed BOOL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS `ga4-134745.idr_meta.identifier_exclusion` (
    identifier_type STRING NOT NULL,
    identifier_value_pattern STRING NOT NULL,
    match_type STRING DEFAULT 'EXACT',
    reason STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `ga4-134745.idr_meta.run_state` (
    table_id STRING NOT NULL,
    last_watermark_value TIMESTAMP,
    last_run_id STRING,
    last_run_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `ga4-134745.idr_meta.config` (
    config_key STRING NOT NULL,
    config_value STRING,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- OUTPUT TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS `ga4-134745.idr_out.identity_edges_current` (
    rule_id STRING NOT NULL,
    left_entity_key STRING NOT NULL,
    right_entity_key STRING NOT NULL,
    identifier_type STRING NOT NULL,
    identifier_value_norm STRING,
    first_seen_ts TIMESTAMP,
    last_seen_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `ga4-134745.idr_out.identity_resolved_membership_current` (
    entity_key STRING NOT NULL,
    resolved_id STRING NOT NULL,
    updated_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `ga4-134745.idr_out.identity_clusters_current` (
    resolved_id STRING NOT NULL,
    cluster_size INT64 NOT NULL,
    updated_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `ga4-134745.idr_out.golden_profile_current` (
    resolved_id STRING NOT NULL,
    email_primary STRING,
    phone_primary STRING,
    first_name STRING,
    last_name STRING,
    updated_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `ga4-134745.idr_out.run_history` (
    run_id STRING NOT NULL,
    run_mode STRING,
    status STRING,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    duration_seconds INT64,
    source_tables_processed INT64,
    entities_processed INT64,
    edges_created INT64,
    clusters_impacted INT64,
    lp_iterations INT64,
    groups_skipped INT64,
    values_excluded INT64,
    large_clusters INT64,
    warnings STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `ga4-134745.idr_out.skipped_identifier_groups` (
    run_id STRING,
    identifier_type STRING,
    identifier_value_norm STRING,
    group_size INT64,
    max_allowed INT64,
    sample_entity_keys STRING,
    reason STRING,
    skipped_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `ga4-134745.idr_out.metrics_export` (
    run_id STRING,
    metric_name STRING,
    metric_value FLOAT64,
    metric_type STRING,
    dimensions STRING,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `ga4-134745.idr_out.dry_run_results` (
    run_id STRING,
    entity_key STRING,
    current_resolved_id STRING,
    proposed_resolved_id STRING,
    change_type STRING,
    current_cluster_size INT64,
    proposed_cluster_size INT64
);

CREATE TABLE IF NOT EXISTS `ga4-134745.idr_out.dry_run_summary` (
    run_id STRING,
    total_entities INT64,
    new_entities INT64,
    moved_entities INT64,
    unchanged_entities INT64,
    merged_clusters INT64,
    split_clusters INT64,
    largest_proposed_cluster INT64,
    edges_would_create INT64,
    groups_would_skip INT64,
    values_would_exclude INT64,
    execution_time_seconds INT64
);

-- ============================================
-- LOAD METADATA CONFIGURATION
-- ============================================

-- Source tables (adjust FQNs to match your BigQuery dataset)
DELETE FROM `ga4-134745.idr_meta.source_table` WHERE 1=1;
INSERT INTO `ga4-134745.idr_meta.source_table` 
(table_id, table_fqn, entity_type, entity_key_expr, watermark_column, watermark_lookback_minutes, is_active)
VALUES
('customer', 'ga4-134745.crm.customer', 'PERSON', 'customer_id', 'rec_create_dt', 0, TRUE),
('transactions', 'ga4-134745.crm.transactions', 'PERSON', "CONCAT(order_id, '-', line_id)", 'load_ts', 0, TRUE),
('loyalty_accounts', 'ga4-134745.crm.loyalty_accounts', 'PERSON', 'loyalty_account_id', 'updated_at', 0, TRUE),
('web_events', 'ga4-134745.crm.web_events', 'PERSON', 'event_id', 'event_ts', 0, TRUE),
('store_visits', 'ga4-134745.crm.store_visits', 'PERSON', 'visit_id', 'visit_ts', 0, TRUE);

-- Source priority
DELETE FROM `ga4-134745.idr_meta.source` WHERE 1=1;
INSERT INTO `ga4-134745.idr_meta.source` (source_id, source_name, priority, is_active) VALUES
('customer', 'CRM', 1, TRUE),
('loyalty_accounts', 'Loyalty', 1, TRUE),
('store_visits', 'Store', 2, TRUE),
('transactions', 'POS', 3, TRUE),
('web_events', 'Digital', 4, TRUE);

-- Matching rules
DELETE FROM `ga4-134745.idr_meta.rule` WHERE 1=1;
INSERT INTO `ga4-134745.idr_meta.rule` 
(rule_id, rule_description, is_active, priority, identifier_type, canonicalize, is_deterministic, is_transitive, max_group_size)
VALUES
('R_EMAIL_EXACT', 'Email exact match', TRUE, 1, 'EMAIL', 'LOWERCASE', TRUE, TRUE, 10000),
('R_PHONE_EXACT', 'Phone exact match', TRUE, 2, 'PHONE', 'NONE', TRUE, TRUE, 10000),
('R_LOYALTY_EXACT', 'Loyalty ID exact match', TRUE, 3, 'LOYALTY_ID', 'NONE', TRUE, TRUE, 10000);

-- Identifier mappings (which columns to extract from each table)
DELETE FROM `ga4-134745.idr_meta.identifier_mapping` WHERE 1=1;
INSERT INTO `ga4-134745.idr_meta.identifier_mapping` (table_id, identifier_type, identifier_value_expr, is_hashed) VALUES
('customer', 'EMAIL', 'email', FALSE),
('customer', 'PHONE', 'phone', FALSE),
('customer', 'LOYALTY_ID', 'loyalty_id', FALSE),
('transactions', 'EMAIL', 'customer_email', FALSE),
('transactions', 'PHONE', 'customer_phone', FALSE),
('loyalty_accounts', 'EMAIL', 'email', FALSE),
('loyalty_accounts', 'PHONE', 'phone', FALSE),
('loyalty_accounts', 'LOYALTY_ID', 'loyalty_id', FALSE),
('web_events', 'EMAIL', 'email', FALSE),
('web_events', 'PHONE', 'phone', FALSE),
('store_visits', 'EMAIL', 'email', FALSE),
('store_visits', 'PHONE', 'phone', FALSE),
('store_visits', 'LOYALTY_ID', 'loyalty_id', FALSE);
