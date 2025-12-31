-- BigQuery Sample Data Generator
-- Creates 5 demo source tables with overlapping identities
--
-- Usage:
--   1. Replace YOUR_PROJECT with your GCP project ID
--   2. Run in BigQuery console
--
-- Configuration: Set ROWS_PER_TABLE as needed (default: 2000)

DECLARE ROWS_PER_TABLE INT64 DEFAULT 2000;

-- ============================================
-- CREATE DATASETS
-- ============================================
CREATE SCHEMA IF NOT EXISTS crm;
CREATE SCHEMA IF NOT EXISTS sales;
CREATE SCHEMA IF NOT EXISTS loyalty;
CREATE SCHEMA IF NOT EXISTS digital;
CREATE SCHEMA IF NOT EXISTS store;

-- ============================================
-- PERSON POOL (shared identities)
-- ============================================
CREATE OR REPLACE TABLE idr_work.person_pool AS
WITH numbers AS (
  SELECT ROW_NUMBER() OVER () AS person_id
  FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS x
),
names AS (
  SELECT
    person_id,
    ['Anil','Rashmi','Asha','Rohan','Maya','Vikram','Neha','Arjun','Priya','Kiran'][SAFE_ORDINAL(1 + MOD(CAST(RAND() * 10 AS INT64), 10))] AS first_name,
    ['Kulkarni','Sharma','Patel','Reddy','Gupta','Iyer','Nair','Singh','Das','Khan'][SAFE_ORDINAL(1 + MOD(CAST(RAND() * 10 AS INT64), 10))] AS last_name,
    CONCAT('9', LPAD(CAST(CAST(RAND() * 999999999 AS INT64) AS STRING), 9, '0')) AS phone,
    CONCAT('L', LPAD(CAST(100000 + person_id AS STRING), 6, '0')) AS loyalty_id
  FROM numbers
)
SELECT
  person_id,
  first_name,
  last_name,
  CONCAT(LOWER(first_name), '.', LOWER(last_name), CAST(person_id AS STRING), '@', 
         ['gmail.com','yahoo.com','outlook.com','example.com'][SAFE_ORDINAL(1 + MOD(CAST(RAND() * 4 AS INT64), 4))]) AS email,
  phone,
  loyalty_id
FROM names;

-- ============================================
-- 1. CUSTOMER TABLE (CRM)
-- ============================================
CREATE OR REPLACE TABLE crm.customer AS
SELECT
  CONCAT('C', LPAD(CAST(ROW_NUMBER() OVER () AS STRING), 6, '0')) AS customer_id,
  p.first_name,
  p.last_name,
  CASE 
    WHEN RAND() < 0.35 THEN UPPER(p.email)
    WHEN RAND() < 0.5 THEN INITCAP(p.email)
    ELSE p.email 
  END AS email,
  p.phone,
  CASE WHEN RAND() < 0.9 THEN p.loyalty_id ELSE NULL END AS loyalty_id,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(RAND() * 45 AS INT64) DAY) AS rec_create_dt,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(RAND() * 30 AS INT64) DAY) AS rec_update_dt
FROM idr_work.person_pool p
CROSS JOIN UNNEST(GENERATE_ARRAY(1, CAST(ROWS_PER_TABLE / 1000 AS INT64) + 1)) AS x
WHERE (p.person_id + x) <= ROWS_PER_TABLE * 2
LIMIT ROWS_PER_TABLE;

-- ============================================
-- 2. TRANSACTIONS TABLE (Sales)
-- ============================================
CREATE OR REPLACE TABLE sales.transactions AS
SELECT
  CONCAT('O', LPAD(CAST(ROW_NUMBER() OVER () AS STRING), 7, '0')) AS order_id,
  CAST(1 + MOD(CAST(RAND() * 3 AS INT64), 3) AS STRING) AS line_id,
  CASE WHEN RAND() < 0.9 THEN
    CASE WHEN RAND() < 0.35 THEN UPPER(p.email) ELSE p.email END
  ELSE NULL END AS customer_email,
  CASE WHEN RAND() < 0.9 THEN p.phone ELSE NULL END AS customer_phone,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(RAND() * 45 AS INT64) DAY) AS load_ts
FROM idr_work.person_pool p
CROSS JOIN UNNEST(GENERATE_ARRAY(1, CAST(ROWS_PER_TABLE / 1000 AS INT64) + 1)) AS x
WHERE (p.person_id + x) <= ROWS_PER_TABLE * 2
LIMIT ROWS_PER_TABLE;

-- ============================================
-- 3. LOYALTY_ACCOUNTS TABLE
-- ============================================
CREATE OR REPLACE TABLE loyalty.loyalty_accounts AS
SELECT
  CONCAT('LA', LPAD(CAST(ROW_NUMBER() OVER () AS STRING), 7, '0')) AS loyalty_account_id,
  p.loyalty_id,
  CASE WHEN RAND() < 0.85 THEN p.email ELSE NULL END AS email,
  CASE WHEN RAND() < 0.85 THEN p.phone ELSE NULL END AS phone,
  ['BRONZE','SILVER','GOLD','PLATINUM'][SAFE_ORDINAL(1 + MOD(CAST(RAND() * 4 AS INT64), 4))] AS tier,
  CAST(RAND() * 25000 AS INT64) AS points,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(RAND() * 45 AS INT64) DAY) AS updated_at
FROM idr_work.person_pool p
CROSS JOIN UNNEST(GENERATE_ARRAY(1, CAST(ROWS_PER_TABLE / 1000 AS INT64) + 1)) AS x
WHERE (p.person_id + x) <= ROWS_PER_TABLE * 2
LIMIT ROWS_PER_TABLE;

-- ============================================
-- 4. WEB_EVENTS TABLE (Digital)
-- ============================================
CREATE OR REPLACE TABLE digital.web_events AS
SELECT
  GENERATE_UUID() AS event_id,
  CONCAT('anon_', LPAD(CAST(1 + MOD(CAST(RAND() * 1200 AS INT64), 1200) AS STRING), 5, '0')) AS anonymous_id,
  ['page_view','product_view','add_to_cart','checkout_start'][SAFE_ORDINAL(1 + MOD(CAST(RAND() * 4 AS INT64), 4))] AS event_type,
  CASE WHEN RAND() < 0.7 THEN p.email ELSE NULL END AS email,
  CASE WHEN RAND() < 0.3 THEN p.phone ELSE NULL END AS phone,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(RAND() * 45 AS INT64) DAY) AS event_ts
FROM idr_work.person_pool p
CROSS JOIN UNNEST(GENERATE_ARRAY(1, CAST(ROWS_PER_TABLE / 1000 AS INT64) + 1)) AS x
WHERE (p.person_id + x) <= ROWS_PER_TABLE * 2
LIMIT ROWS_PER_TABLE;

-- ============================================
-- 5. STORE_VISITS TABLE
-- ============================================
CREATE OR REPLACE TABLE store.store_visits AS
SELECT
  CONCAT('V', LPAD(CAST(ROW_NUMBER() OVER () AS STRING), 8, '0')) AS visit_id,
  CONCAT('S', LPAD(CAST(1 + MOD(CAST(RAND() * 250 AS INT64), 250) AS STRING), 4, '0')) AS store_id,
  CASE WHEN RAND() < 0.6 THEN p.email ELSE NULL END AS email,
  CASE WHEN RAND() < 0.6 THEN p.phone ELSE NULL END AS phone,
  CASE WHEN RAND() < 0.8 THEN p.loyalty_id ELSE NULL END AS loyalty_id,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(RAND() * 45 AS INT64) DAY) AS visit_ts
FROM idr_work.person_pool p
CROSS JOIN UNNEST(GENERATE_ARRAY(1, CAST(ROWS_PER_TABLE / 1000 AS INT64) + 1)) AS x
WHERE (p.person_id + x) <= ROWS_PER_TABLE * 2
LIMIT ROWS_PER_TABLE;

-- ============================================
-- LOAD SAMPLE METADATA
-- ============================================

-- Source tables
DELETE FROM idr_meta.source_table WHERE TRUE;
INSERT INTO idr_meta.source_table VALUES
  ('customer', 'crm.customer', 'PERSON', 'customer_id', 'rec_create_dt', 0, TRUE),
  ('transactions', 'sales.transactions', 'PERSON', "CONCAT(order_id, '-', line_id)", 'load_ts', 0, TRUE),
  ('loyalty_accounts', 'loyalty.loyalty_accounts', 'PERSON', 'loyalty_account_id', 'updated_at', 0, TRUE),
  ('web_events', 'digital.web_events', 'PERSON', 'event_id', 'event_ts', 0, TRUE),
  ('store_visits', 'store.store_visits', 'PERSON', 'visit_id', 'visit_ts', 0, TRUE);

-- Source trust ranking
DELETE FROM idr_meta.source WHERE TRUE;
INSERT INTO idr_meta.source VALUES
  ('customer', 'CRM', 1, TRUE),
  ('loyalty_accounts', 'Loyalty', 1, TRUE),
  ('store_visits', 'Store', 2, TRUE),
  ('transactions', 'POS', 3, TRUE),
  ('web_events', 'Digital', 4, TRUE);

-- Matching rules
DELETE FROM idr_meta.rule WHERE TRUE;
INSERT INTO idr_meta.rule VALUES
  ('R_EMAIL_EXACT', 'Email exact match', TRUE, 1, 'EMAIL', 'LOWERCASE', TRUE, TRUE),
  ('R_PHONE_EXACT', 'Phone exact match', TRUE, 2, 'PHONE', 'NONE', TRUE, TRUE),
  ('R_LOYALTY_EXACT', 'Loyalty ID exact match', TRUE, 3, 'LOYALTY_ID', 'NONE', TRUE, TRUE);

-- Identifier mappings
DELETE FROM idr_meta.identifier_mapping WHERE TRUE;
INSERT INTO idr_meta.identifier_mapping VALUES
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

-- Entity attribute mappings
DELETE FROM idr_meta.entity_attribute_mapping WHERE TRUE;
INSERT INTO idr_meta.entity_attribute_mapping VALUES
  ('customer', 'record_updated_at', 'rec_update_dt'),
  ('customer', 'first_name', 'first_name'),
  ('customer', 'last_name', 'last_name'),
  ('customer', 'email_raw', 'email'),
  ('customer', 'phone_raw', 'phone');

-- Survivorship rules
DELETE FROM idr_meta.survivorship_rule WHERE TRUE;
INSERT INTO idr_meta.survivorship_rule VALUES
  ('email_primary', 'SOURCE_PRIORITY', 'CRM,Loyalty,Store,POS,Digital', 'record_updated_at'),
  ('phone_primary', 'SOURCE_PRIORITY', 'CRM,Loyalty,Store,POS,Digital', 'record_updated_at'),
  ('first_name', 'MOST_RECENT', NULL, 'record_updated_at'),
  ('last_name', 'MOST_RECENT', NULL, 'record_updated_at');

-- Cleanup
DROP TABLE IF EXISTS idr_work.person_pool;

SELECT 'Sample data and metadata created successfully!' AS result;
