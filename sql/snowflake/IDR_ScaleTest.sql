-- Snowflake Scale Test & Benchmark
-- Generate millions of rows of realistic test data and benchmark IDR performance
--
-- Cluster Sizing Recommendations:
-- | Rows per Table | Total Rows | Warehouse Size | Expected Duration |
-- |----------------|------------|----------------|-------------------|
-- | 100K | 500K | X-Small | 2-5 min |
-- | 500K | 2.5M | Small | 5-15 min |
-- | 1M | 5M | Medium | 15-30 min |
-- | 5M | 25M | Large | 30-60 min |
-- | 10M | 50M | X-Large | 1-2 hours |
--
-- Usage:
--   SET N_ROWS = 1000000;  -- 1M per table
--   Execute this script

-- ============================================
-- CONFIGURATION
-- ============================================
SET N_ROWS = 1000000;          -- Rows per table
SET OVERLAP_RATE = 0.6;        -- Identity overlap (0.3-0.7)
SET N_PEOPLE = $N_ROWS * $OVERLAP_RATE;

-- Use larger warehouse for scale
-- ALTER WAREHOUSE your_warehouse SET WAREHOUSE_SIZE = 'LARGE';

-- ============================================
-- CREATE SCHEMAS
-- ============================================
CREATE SCHEMA IF NOT EXISTS crm;
CREATE SCHEMA IF NOT EXISTS sales;
CREATE SCHEMA IF NOT EXISTS loyalty;
CREATE SCHEMA IF NOT EXISTS digital;
CREATE SCHEMA IF NOT EXISTS store;

-- ============================================
-- PERSON POOL (Shared Identities)
-- ============================================
CREATE OR REPLACE TRANSIENT TABLE idr_work.person_pool AS
WITH numbers AS (
  SELECT SEQ4() + 1 AS person_id
  FROM TABLE(GENERATOR(ROWCOUNT => $N_PEOPLE))
),
names AS (
  SELECT
    person_id,
    ARRAY_CONSTRUCT('Anil','Rashmi','Asha','Rohan','Maya','Vikram','Neha','Arjun','Priya','Kiran',
                    'Raj','Sita','Kavita','Suresh','Anita','Deepak','Meera','Rahul','Pooja','Amit')
      [MOD(person_id, 20)]::VARCHAR AS first_name,
    ARRAY_CONSTRUCT('Kulkarni','Sharma','Patel','Reddy','Gupta','Iyer','Nair','Singh','Das','Khan',
                    'Verma','Joshi','Rao','Kumar','Mehta','Shah','Pandey','Thakur','Yadav','Mishra')
      [MOD(FLOOR(person_id / 20), 20)]::VARCHAR AS last_name,
    CONCAT('9', LPAD(FLOOR(UNIFORM(0, 999999999, RANDOM()))::VARCHAR, 9, '0')) AS phone,
    CONCAT('L', LPAD((100000 + person_id)::VARCHAR, 6, '0')) AS loyalty_id
  FROM numbers
)
SELECT
  person_id,
  first_name,
  last_name,
  CONCAT(LOWER(first_name), '.', LOWER(last_name), person_id::VARCHAR, '@', 
         ARRAY_CONSTRUCT('gmail.com','yahoo.com','outlook.com','example.com','company.com')[MOD(person_id, 5)]) AS email,
  phone,
  loyalty_id
FROM names;

-- ============================================
-- 1. CUSTOMER TABLE (CRM)
-- ============================================
CREATE OR REPLACE TABLE crm.customer AS
SELECT
  CONCAT('C', LPAD(ROW_NUMBER() OVER (ORDER BY 1)::VARCHAR, 8, '0')) AS customer_id,
  p.first_name,
  p.last_name,
  CASE 
    WHEN UNIFORM(0, 100, RANDOM()) < 5 THEN NULL
    WHEN UNIFORM(0, 100, RANDOM()) < 35 THEN UPPER(p.email)
    WHEN UNIFORM(0, 100, RANDOM()) < 50 THEN INITCAP(p.email)
    ELSE p.email 
  END AS email,
  CASE WHEN UNIFORM(0, 100, RANDOM()) < 10 THEN NULL ELSE p.phone END AS phone,
  CASE WHEN UNIFORM(0, 100, RANDOM()) < 15 THEN NULL ELSE p.loyalty_id END AS loyalty_id,
  DATEADD(DAY, -UNIFORM(0, 45, RANDOM()), CURRENT_TIMESTAMP()) AS rec_create_dt,
  DATEADD(DAY, -UNIFORM(0, 30, RANDOM()), CURRENT_TIMESTAMP()) AS rec_update_dt
FROM idr_work.person_pool p, TABLE(GENERATOR(ROWCOUNT => $N_ROWS / $N_PEOPLE + 1)) g
LIMIT $N_ROWS;

-- ============================================
-- 2. TRANSACTIONS TABLE (Sales)
-- ============================================
CREATE OR REPLACE TABLE sales.transactions AS
SELECT
  CONCAT('O', LPAD(ROW_NUMBER() OVER (ORDER BY 1)::VARCHAR, 10, '0')) AS order_id,
  (1 + MOD(UNIFORM(0, 100, RANDOM()), 3))::VARCHAR AS line_id,
  CASE WHEN UNIFORM(0, 100, RANDOM()) < 15 THEN NULL
       WHEN UNIFORM(0, 100, RANDOM()) < 35 THEN UPPER(p.email) 
       ELSE p.email END AS customer_email,
  CASE WHEN UNIFORM(0, 100, RANDOM()) < 20 THEN NULL ELSE p.phone END AS customer_phone,
  DATEADD(DAY, -UNIFORM(0, 45, RANDOM()), CURRENT_TIMESTAMP()) AS load_ts
FROM idr_work.person_pool p, TABLE(GENERATOR(ROWCOUNT => $N_ROWS / $N_PEOPLE + 1)) g
LIMIT $N_ROWS;

-- ============================================
-- 3. LOYALTY_ACCOUNTS TABLE
-- ============================================
CREATE OR REPLACE TABLE loyalty.loyalty_accounts AS
SELECT
  CONCAT('LA', LPAD(ROW_NUMBER() OVER (ORDER BY 1)::VARCHAR, 10, '0')) AS loyalty_account_id,
  p.loyalty_id,
  CASE WHEN UNIFORM(0, 100, RANDOM()) < 20 THEN NULL ELSE p.email END AS email,
  CASE WHEN UNIFORM(0, 100, RANDOM()) < 20 THEN NULL ELSE p.phone END AS phone,
  ARRAY_CONSTRUCT('BRONZE','SILVER','GOLD','PLATINUM')[MOD(UNIFORM(0, 100, RANDOM()), 4)] AS tier,
  UNIFORM(0, 50000, RANDOM()) AS points,
  DATEADD(DAY, -UNIFORM(0, 45, RANDOM()), CURRENT_TIMESTAMP()) AS updated_at
FROM idr_work.person_pool p, TABLE(GENERATOR(ROWCOUNT => $N_ROWS / $N_PEOPLE + 1)) g
LIMIT $N_ROWS;

-- ============================================
-- 4. WEB_EVENTS TABLE (Digital)
-- ============================================
CREATE OR REPLACE TABLE digital.web_events AS
SELECT
  UUID_STRING() AS event_id,
  CONCAT('anon_', LPAD(MOD(UNIFORM(0, 10000, RANDOM()), 10000)::VARCHAR, 6, '0')) AS anonymous_id,
  ARRAY_CONSTRUCT('page_view','product_view','add_to_cart','checkout_start','purchase')[MOD(UNIFORM(0, 100, RANDOM()), 5)] AS event_type,
  CASE WHEN UNIFORM(0, 100, RANDOM()) < 35 THEN NULL ELSE p.email END AS email,
  CASE WHEN UNIFORM(0, 100, RANDOM()) < 70 THEN NULL ELSE p.phone END AS phone,
  DATEADD(DAY, -UNIFORM(0, 45, RANDOM()), CURRENT_TIMESTAMP()) AS event_ts
FROM idr_work.person_pool p, TABLE(GENERATOR(ROWCOUNT => $N_ROWS / $N_PEOPLE + 1)) g
LIMIT $N_ROWS;

-- ============================================
-- 5. STORE_VISITS TABLE
-- ============================================
CREATE OR REPLACE TABLE store.store_visits AS
SELECT
  CONCAT('V', LPAD(ROW_NUMBER() OVER (ORDER BY 1)::VARCHAR, 12, '0')) AS visit_id,
  CONCAT('S', LPAD(MOD(UNIFORM(0, 1000, RANDOM()), 500)::VARCHAR, 4, '0')) AS store_id,
  CASE WHEN UNIFORM(0, 100, RANDOM()) < 40 THEN NULL ELSE p.email END AS email,
  CASE WHEN UNIFORM(0, 100, RANDOM()) < 40 THEN NULL ELSE p.phone END AS phone,
  CASE WHEN UNIFORM(0, 100, RANDOM()) < 25 THEN NULL ELSE p.loyalty_id END AS loyalty_id,
  DATEADD(DAY, -UNIFORM(0, 45, RANDOM()), CURRENT_TIMESTAMP()) AS visit_ts
FROM idr_work.person_pool p, TABLE(GENERATOR(ROWCOUNT => $N_ROWS / $N_PEOPLE + 1)) g
LIMIT $N_ROWS;

-- ============================================
-- LOAD METADATA (if not done)
-- ============================================
-- Run IDR_SampleData_Generate.sql metadata section

-- ============================================
-- RUN BENCHMARK
-- ============================================
-- Record start time
SET benchmark_start = CURRENT_TIMESTAMP();

CALL idr_run('FULL', 50);

-- ============================================
-- BENCHMARK RESULTS
-- ============================================
SELECT 
  run_id,
  run_mode,
  status,
  entities_processed,
  edges_created,
  clusters_impacted,
  lp_iterations,
  duration_seconds,
  ROUND(duration_seconds / 60, 1) AS duration_minutes
FROM idr_out.run_history
ORDER BY started_at DESC
LIMIT 1;

-- Cluster distribution
SELECT 
  CASE 
    WHEN cluster_size = 1 THEN '01_singleton'
    WHEN cluster_size <= 5 THEN '02_small (2-5)'
    WHEN cluster_size <= 20 THEN '03_medium (6-20)'
    WHEN cluster_size <= 100 THEN '04_large (21-100)'
    WHEN cluster_size <= 1000 THEN '05_xlarge (101-1000)'
    ELSE '06_giant (1000+)'
  END AS size_bucket,
  COUNT(*) AS cluster_count,
  SUM(cluster_size) AS total_entities
FROM idr_out.identity_clusters_current
GROUP BY 1
ORDER BY 1;

-- Cleanup pool
DROP TABLE IF EXISTS idr_work.person_pool;

SELECT 'Scale test complete!' AS result;
