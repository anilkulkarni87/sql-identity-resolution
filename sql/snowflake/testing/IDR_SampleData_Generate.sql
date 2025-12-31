-- Snowflake Sample Data Generator
-- Creates 5 demo source tables with overlapping identities

-- Usage:
--   SET ROWS_PER_TABLE = 2000;
--   Execute this script

-- ============================================
-- CONFIGURATION
-- ============================================
SET N_ROWS = 2000;  -- Rows per table (adjust as needed)

-- ============================================
-- CREATE SCHEMAS
-- ============================================
CREATE SCHEMA IF NOT EXISTS crm;
CREATE SCHEMA IF NOT EXISTS sales;
CREATE SCHEMA IF NOT EXISTS loyalty;
CREATE SCHEMA IF NOT EXISTS digital;
CREATE SCHEMA IF NOT EXISTS store;

-- ============================================
-- HELPER: Generate random data using sequences
-- ============================================

-- Person pool (shared identities for overlaps)
CREATE OR REPLACE TRANSIENT TABLE idr_work.person_pool AS
WITH names AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY 1) AS person_id,
        ARRAY_CONSTRUCT('Anil','Rashmi','Asha','Rohan','Maya','Vikram','Neha','Arjun','Priya','Kiran')[UNIFORM(0, 9, RANDOM())] AS first_name,
        ARRAY_CONSTRUCT('Kulkarni','Sharma','Patel','Reddy','Gupta','Iyer','Nair','Singh','Das','Khan')[UNIFORM(0, 9, RANDOM())] AS last_name,
        '9' || LPAD(FLOOR(UNIFORM(0, 999999999, RANDOM()))::VARCHAR, 9, '0') AS phone,
        'L' || LPAD((100000 + ROW_NUMBER() OVER (ORDER BY 1))::VARCHAR, 6, '0') AS loyalty_id
    FROM TABLE(GENERATOR(ROWCOUNT => 1000))
)
SELECT 
    person_id,
    first_name,
    last_name,
    LOWER(first_name || '.' || last_name || person_id) || '@' || 
        ARRAY_CONSTRUCT('gmail.com','yahoo.com','outlook.com','example.com')[UNIFORM(0, 3, RANDOM())] AS email,
    phone,
    loyalty_id
FROM names;

-- ============================================
-- 1. CUSTOMER TABLE (CRM)
-- ============================================
CREATE OR REPLACE TABLE crm.customer AS
SELECT 
    'C' || LPAD(ROW_NUMBER() OVER (ORDER BY 1)::VARCHAR, 6, '0') AS customer_id,
    p.first_name,
    p.last_name,
    CASE WHEN UNIFORM(0, 100, RANDOM()) < 35 THEN UPPER(p.email)
         WHEN UNIFORM(0, 100, RANDOM()) < 50 THEN INITCAP(p.email)
         ELSE p.email END AS email,
    p.phone,
    CASE WHEN UNIFORM(0, 100, RANDOM()) < 90 THEN p.loyalty_id ELSE NULL END AS loyalty_id,
    DATEADD(DAY, -UNIFORM(0, 45, RANDOM()), CURRENT_TIMESTAMP()) AS rec_create_dt,
    DATEADD(DAY, -UNIFORM(0, 30, RANDOM()), CURRENT_TIMESTAMP()) AS rec_update_dt
FROM idr_work.person_pool p, TABLE(GENERATOR(ROWCOUNT => $N_ROWS)) g
WHERE p.person_id = UNIFORM(1, 1000, RANDOM());

-- ============================================
-- 2. TRANSACTIONS TABLE (Sales)
-- ============================================
CREATE OR REPLACE TABLE sales.transactions AS
SELECT 
    'O' || LPAD(ROW_NUMBER() OVER (ORDER BY 1)::VARCHAR, 7, '0') AS order_id,
    FLOOR(UNIFORM(1, 4, RANDOM()))::VARCHAR AS line_id,
    CASE WHEN UNIFORM(0, 100, RANDOM()) < 90 THEN
        CASE WHEN UNIFORM(0, 100, RANDOM()) < 35 THEN UPPER(p.email) ELSE p.email END
    ELSE NULL END AS customer_email,
    CASE WHEN UNIFORM(0, 100, RANDOM()) < 90 THEN p.phone ELSE NULL END AS customer_phone,
    DATEADD(DAY, -UNIFORM(0, 45, RANDOM()), CURRENT_TIMESTAMP()) AS load_ts
FROM idr_work.person_pool p, TABLE(GENERATOR(ROWCOUNT => $N_ROWS)) g
WHERE p.person_id = UNIFORM(1, 1000, RANDOM());

-- ============================================
-- 3. LOYALTY_ACCOUNTS TABLE
-- ============================================
CREATE OR REPLACE TABLE loyalty.loyalty_accounts AS
SELECT 
    'LA' || LPAD(ROW_NUMBER() OVER (ORDER BY 1)::VARCHAR, 7, '0') AS loyalty_account_id,
    p.loyalty_id,
    CASE WHEN UNIFORM(0, 100, RANDOM()) < 85 THEN p.email ELSE NULL END AS email,
    CASE WHEN UNIFORM(0, 100, RANDOM()) < 85 THEN p.phone ELSE NULL END AS phone,
    ARRAY_CONSTRUCT('BRONZE','SILVER','GOLD','PLATINUM')[UNIFORM(0, 3, RANDOM())] AS tier,
    FLOOR(UNIFORM(0, 25000, RANDOM())) AS points,
    DATEADD(DAY, -UNIFORM(0, 45, RANDOM()), CURRENT_TIMESTAMP()) AS updated_at
FROM idr_work.person_pool p, TABLE(GENERATOR(ROWCOUNT => $N_ROWS)) g
WHERE p.person_id = UNIFORM(1, 1000, RANDOM());

-- ============================================
-- 4. WEB_EVENTS TABLE (Digital)
-- ============================================
CREATE OR REPLACE TABLE digital.web_events AS
SELECT 
    UUID_STRING() AS event_id,
    'anon_' || LPAD(FLOOR(UNIFORM(1, 1200, RANDOM()))::VARCHAR, 5, '0') AS anonymous_id,
    ARRAY_CONSTRUCT('page_view','product_view','add_to_cart','checkout_start')[UNIFORM(0, 3, RANDOM())] AS event_type,
    CASE WHEN UNIFORM(0, 100, RANDOM()) < 70 THEN p.email ELSE NULL END AS email,
    CASE WHEN UNIFORM(0, 100, RANDOM()) < 30 THEN p.phone ELSE NULL END AS phone,
    DATEADD(DAY, -UNIFORM(0, 45, RANDOM()), CURRENT_TIMESTAMP()) AS event_ts
FROM idr_work.person_pool p, TABLE(GENERATOR(ROWCOUNT => $N_ROWS)) g
WHERE p.person_id = UNIFORM(1, 1000, RANDOM());

-- ============================================
-- 5. STORE_VISITS TABLE
-- ============================================
CREATE OR REPLACE TABLE store.store_visits AS
SELECT 
    'V' || LPAD(ROW_NUMBER() OVER (ORDER BY 1)::VARCHAR, 8, '0') AS visit_id,
    'S' || LPAD(FLOOR(UNIFORM(1, 250, RANDOM()))::VARCHAR, 4, '0') AS store_id,
    CASE WHEN UNIFORM(0, 100, RANDOM()) < 60 THEN p.email ELSE NULL END AS email,
    CASE WHEN UNIFORM(0, 100, RANDOM()) < 60 THEN p.phone ELSE NULL END AS phone,
    CASE WHEN UNIFORM(0, 100, RANDOM()) < 80 THEN p.loyalty_id ELSE NULL END AS loyalty_id,
    DATEADD(DAY, -UNIFORM(0, 45, RANDOM()), CURRENT_TIMESTAMP()) AS visit_ts
FROM idr_work.person_pool p, TABLE(GENERATOR(ROWCOUNT => $N_ROWS)) g
WHERE p.person_id = UNIFORM(1, 1000, RANDOM());

-- ============================================
-- LOAD SAMPLE METADATA
-- ============================================

-- Source tables
DELETE FROM idr_meta.source_table;
INSERT INTO idr_meta.source_table VALUES
  ('customer', 'crm.customer', 'PERSON', 'customer_id', 'rec_create_dt', 0, TRUE),
  ('transactions', 'sales.transactions', 'PERSON', 'order_id || ''-'' || line_id', 'load_ts', 0, TRUE),
  ('loyalty_accounts', 'loyalty.loyalty_accounts', 'PERSON', 'loyalty_account_id', 'updated_at', 0, TRUE),
  ('web_events', 'digital.web_events', 'PERSON', 'event_id', 'event_ts', 0, TRUE),
  ('store_visits', 'store.store_visits', 'PERSON', 'visit_id', 'visit_ts', 0, TRUE);

-- Source trust ranking
DELETE FROM idr_meta.source;
INSERT INTO idr_meta.source VALUES
  ('customer', 'CRM', 1, TRUE),
  ('loyalty_accounts', 'Loyalty', 1, TRUE),
  ('store_visits', 'Store', 2, TRUE),
  ('transactions', 'POS', 3, TRUE),
  ('web_events', 'Digital', 4, TRUE);

-- Matching rules
DELETE FROM idr_meta.rule;
INSERT INTO idr_meta.rule VALUES
  ('R_EMAIL_EXACT', 'Email exact match', TRUE, 1, 'EMAIL', 'LOWERCASE', TRUE, TRUE),
  ('R_PHONE_EXACT', 'Phone exact match', TRUE, 2, 'PHONE', 'NONE', TRUE, TRUE),
  ('R_LOYALTY_EXACT', 'Loyalty ID exact match', TRUE, 3, 'LOYALTY_ID', 'NONE', TRUE, TRUE);

-- Identifier mappings
DELETE FROM idr_meta.identifier_mapping;
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
DELETE FROM idr_meta.entity_attribute_mapping;
INSERT INTO idr_meta.entity_attribute_mapping VALUES
  ('customer', 'record_updated_at', 'rec_update_dt'),
  ('customer', 'first_name', 'first_name'),
  ('customer', 'last_name', 'last_name'),
  ('customer', 'email_raw', 'email'),
  ('customer', 'phone_raw', 'phone');

-- Survivorship rules
DELETE FROM idr_meta.survivorship_rule;
INSERT INTO idr_meta.survivorship_rule VALUES
  ('email_primary', 'SOURCE_PRIORITY', 'CRM,Loyalty,Store,POS,Digital', 'record_updated_at'),
  ('phone_primary', 'SOURCE_PRIORITY', 'CRM,Loyalty,Store,POS,Digital', 'record_updated_at'),
  ('first_name', 'MOST_RECENT', NULL, 'record_updated_at'),
  ('last_name', 'MOST_RECENT', NULL, 'record_updated_at');

-- Cleanup
DROP TABLE IF EXISTS idr_work.person_pool;

SELECT 'Sample data created successfully' AS result;
