-- BigQuery Scale Test: Metadata Configuration Only
-- Project: ga4-134745
-- 
-- Run this AFTER 00_ddl_all.sql to configure the scale test
-- This file only contains data (INSERT statements), no DDL
--
-- Usage:
--   bq query --use_legacy_sql=false --project_id=ga4-134745 < scale_test_metadata.sql

-- ============================================
-- SOURCE TABLE CONFIGURATION
-- ============================================
DELETE FROM `ga4-134745.idr_meta.source_table` WHERE 1=1;
INSERT INTO `ga4-134745.idr_meta.source_table` 
(table_id, table_fqn, entity_type, entity_key_expr, watermark_column, watermark_lookback_minutes, is_active)
VALUES
('customer', 'ga4-134745.crm.customer', 'PERSON', 'customer_id', 'rec_create_dt', 0, TRUE),
('transactions', 'ga4-134745.crm.transactions', 'PERSON', "CONCAT(order_id, '-', line_id)", 'load_ts', 0, TRUE),
('loyalty_accounts', 'ga4-134745.crm.loyalty_accounts', 'PERSON', 'loyalty_account_id', 'updated_at', 0, TRUE),
('web_events', 'ga4-134745.crm.web_events', 'PERSON', 'event_id', 'event_ts', 0, TRUE),
('store_visits', 'ga4-134745.crm.store_visits', 'PERSON', 'visit_id', 'visit_ts', 0, TRUE);

-- ============================================
-- SOURCE PRIORITY CONFIGURATION
-- ============================================
DELETE FROM `ga4-134745.idr_meta.source` WHERE 1=1;
INSERT INTO `ga4-134745.idr_meta.source` (table_id, source_name, trust_rank, is_active) VALUES
('customer', 'CRM', 1, TRUE),
('loyalty_accounts', 'Loyalty', 1, TRUE),
('store_visits', 'Store', 2, TRUE),
('transactions', 'POS', 3, TRUE),
('web_events', 'Digital', 4, TRUE);

-- ============================================
-- MATCHING RULES
-- ============================================
DELETE FROM `ga4-134745.idr_meta.rule` WHERE 1=1;
INSERT INTO `ga4-134745.idr_meta.rule` 
(rule_id, rule_name, is_active, priority, identifier_type, canonicalize, allow_hashed, require_non_null, max_group_size)
VALUES
('R_EMAIL_EXACT', 'Email exact match', TRUE, 1, 'EMAIL', 'LOWERCASE', TRUE, TRUE, 10000),
('R_PHONE_EXACT', 'Phone exact match', TRUE, 2, 'PHONE', 'NONE', TRUE, TRUE, 10000),
('R_LOYALTY_EXACT', 'Loyalty ID exact match', TRUE, 3, 'LOYALTY_ID', 'NONE', TRUE, TRUE, 10000);

-- ============================================
-- IDENTIFIER MAPPINGS
-- ============================================
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
