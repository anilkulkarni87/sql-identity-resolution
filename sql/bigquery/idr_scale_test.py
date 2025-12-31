#!/usr/bin/env python3
"""
BigQuery Scale Test & Benchmark
Generate millions of rows and benchmark IDR performance.

Cluster Sizing Recommendations (BigQuery slots):
| Rows per Table | Total Rows | On-Demand | Flat-Rate Slots | Expected Duration |
|----------------|------------|-----------|-----------------|-------------------|
| 100K | 500K | Default | 100 | 2-5 min |
| 500K | 2.5M | Default | 500 | 5-15 min |
| 1M | 5M | Default | 1000 | 15-30 min |
| 5M | 25M | Enterprise | 2000+ | 30-60 min |
| 10M | 50M | Enterprise | 4000+ | 1-2 hours |

Usage:
    python idr_scale_test.py --project=your-project --scale=1M
"""

import argparse
import time
import uuid
import random
import string
from datetime import datetime, timedelta
from google.cloud import bigquery

# ============================================
# CONFIGURATION
# ============================================
parser = argparse.ArgumentParser(description='BigQuery Scale Test')
parser.add_argument('--project', required=True, help='GCP project ID')
parser.add_argument('--scale', default='1M', choices=['100K', '500K', '1M', '5M', '10M'])
parser.add_argument('--overlap-rate', type=float, default=0.6, help='Identity overlap rate (0.3-0.7)')
parser.add_argument('--seed', type=int, default=42)
args = parser.parse_args()

SCALE_MAP = {"100K": 100000, "500K": 500000, "1M": 1000000, "5M": 5000000, "10M": 10000000}
PROJECT = args.project
N_ROWS = SCALE_MAP[args.scale]
OVERLAP_RATE = args.overlap_rate
N_PEOPLE = int(N_ROWS * OVERLAP_RATE)
random.seed(args.seed)

client = bigquery.Client(project=PROJECT)

print(f"📊 BigQuery Scale Test")
print(f"   Project: {PROJECT}")
print(f"   Scale: {args.scale} ({N_ROWS:,} rows per table)")
print(f"   Total rows: {N_ROWS * 5:,}")
print(f"   Overlap rate: {OVERLAP_RATE} ({N_PEOPLE:,} unique identities)")

def q(sql: str):
    return client.query(sql).result()

def collect_one(sql: str):
    for row in client.query(sql).result():
        return row[0]
    return None

# ============================================
# CREATE SCHEMAS
# ============================================
print("\n🔧 Creating schemas...")
for schema in ['crm', 'sales', 'loyalty', 'digital', 'store']:
    q(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# ============================================
# GENERATE DATA
# ============================================
print(f"\n📝 Generating {N_ROWS * 5:,} rows of test data...")

first_names = ['Anil','Rashmi','Asha','Rohan','Maya','Vikram','Neha','Arjun','Priya','Kiran',
               'Raj','Sita','Kavita','Suresh','Anita','Deepak','Meera','Rahul','Pooja','Amit']
last_names = ['Kulkarni','Sharma','Patel','Reddy','Gupta','Iyer','Nair','Singh','Das','Khan',
              'Verma','Joshi','Rao','Kumar','Mehta','Shah','Pandey','Thakur','Yadav','Mishra']
domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'example.com', 'company.com']
tiers = ['BRONZE', 'SILVER', 'GOLD', 'PLATINUM']
events = ['page_view', 'product_view', 'add_to_cart', 'checkout_start', 'purchase']

# Generate person pool via SQL
print("   Creating person pool...")
q(f"""
CREATE OR REPLACE TABLE idr_work.person_pool AS
SELECT
  person_id,
  {first_names}[SAFE_ORDINAL(1 + MOD(person_id, {len(first_names)}))] AS first_name,
  {last_names}[SAFE_ORDINAL(1 + MOD(CAST(person_id / {len(first_names)} AS INT64), {len(last_names)}))] AS last_name,
  CONCAT(LOWER({first_names}[SAFE_ORDINAL(1 + MOD(person_id, {len(first_names)}))]), '.', 
         LOWER({last_names}[SAFE_ORDINAL(1 + MOD(CAST(person_id / {len(first_names)} AS INT64), {len(last_names)}))]),
         CAST(person_id AS STRING), '@', {domains}[SAFE_ORDINAL(1 + MOD(person_id, {len(domains)}))]
        ) AS email,
  CONCAT('9', LPAD(CAST(MOD(FARM_FINGERPRINT(CAST(person_id AS STRING)), 1000000000) AS STRING), 9, '0')) AS phone,
  CONCAT('L', LPAD(CAST(100000 + person_id AS STRING), 6, '0')) AS loyalty_id
FROM UNNEST(GENERATE_ARRAY(1, {N_PEOPLE})) AS person_id
""")

# Customer table
print("   Creating crm.customer...")
start = time.time()
q(f"""
CREATE OR REPLACE TABLE crm.customer AS
SELECT
  CONCAT('C', LPAD(CAST(ROW_NUMBER() OVER () AS STRING), 8, '0')) AS customer_id,
  p.first_name,
  p.last_name,
  CASE 
    WHEN RAND() < 0.05 THEN NULL
    WHEN RAND() < 0.35 THEN UPPER(p.email)
    ELSE p.email 
  END AS email,
  CASE WHEN RAND() < 0.1 THEN NULL ELSE p.phone END AS phone,
  CASE WHEN RAND() < 0.15 THEN NULL ELSE p.loyalty_id END AS loyalty_id,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(RAND() * 45 AS INT64) DAY) AS rec_create_dt,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(RAND() * 30 AS INT64) DAY) AS rec_update_dt
FROM idr_work.person_pool p
CROSS JOIN UNNEST(GENERATE_ARRAY(1, CAST({N_ROWS} / {N_PEOPLE} AS INT64) + 1)) AS x
LIMIT {N_ROWS}
""")
print(f"      ✅ {time.time() - start:.1f}s")

# Transactions table
print("   Creating sales.transactions...")
start = time.time()
q(f"""
CREATE OR REPLACE TABLE sales.transactions AS
SELECT
  CONCAT('O', LPAD(CAST(ROW_NUMBER() OVER () AS STRING), 10, '0')) AS order_id,
  CAST(1 + MOD(CAST(RAND() * 3 AS INT64), 3) AS STRING) AS line_id,
  CASE WHEN RAND() < 0.15 THEN NULL WHEN RAND() < 0.35 THEN UPPER(p.email) ELSE p.email END AS customer_email,
  CASE WHEN RAND() < 0.2 THEN NULL ELSE p.phone END AS customer_phone,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(RAND() * 45 AS INT64) DAY) AS load_ts
FROM idr_work.person_pool p
CROSS JOIN UNNEST(GENERATE_ARRAY(1, CAST({N_ROWS} / {N_PEOPLE} AS INT64) + 1)) AS x
LIMIT {N_ROWS}
""")
print(f"      ✅ {time.time() - start:.1f}s")

# Loyalty accounts table
print("   Creating loyalty.loyalty_accounts...")
start = time.time()
q(f"""
CREATE OR REPLACE TABLE loyalty.loyalty_accounts AS
SELECT
  CONCAT('LA', LPAD(CAST(ROW_NUMBER() OVER () AS STRING), 10, '0')) AS loyalty_account_id,
  p.loyalty_id,
  CASE WHEN RAND() < 0.2 THEN NULL ELSE p.email END AS email,
  CASE WHEN RAND() < 0.2 THEN NULL ELSE p.phone END AS phone,
  {tiers}[SAFE_ORDINAL(1 + MOD(CAST(RAND() * 4 AS INT64), 4))] AS tier,
  CAST(RAND() * 50000 AS INT64) AS points,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(RAND() * 45 AS INT64) DAY) AS updated_at
FROM idr_work.person_pool p
CROSS JOIN UNNEST(GENERATE_ARRAY(1, CAST({N_ROWS} / {N_PEOPLE} AS INT64) + 1)) AS x
LIMIT {N_ROWS}
""")
print(f"      ✅ {time.time() - start:.1f}s")

# Web events table
print("   Creating digital.web_events...")
start = time.time()
q(f"""
CREATE OR REPLACE TABLE digital.web_events AS
SELECT
  GENERATE_UUID() AS event_id,
  CONCAT('anon_', LPAD(CAST(MOD(CAST(RAND() * 10000 AS INT64), 10000) AS STRING), 6, '0')) AS anonymous_id,
  {events}[SAFE_ORDINAL(1 + MOD(CAST(RAND() * 5 AS INT64), 5))] AS event_type,
  CASE WHEN RAND() < 0.35 THEN NULL ELSE p.email END AS email,
  CASE WHEN RAND() < 0.7 THEN NULL ELSE p.phone END AS phone,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(RAND() * 45 AS INT64) DAY) AS event_ts
FROM idr_work.person_pool p
CROSS JOIN UNNEST(GENERATE_ARRAY(1, CAST({N_ROWS} / {N_PEOPLE} AS INT64) + 1)) AS x
LIMIT {N_ROWS}
""")
print(f"      ✅ {time.time() - start:.1f}s")

# Store visits table
print("   Creating store.store_visits...")
start = time.time()
q(f"""
CREATE OR REPLACE TABLE store.store_visits AS
SELECT
  CONCAT('V', LPAD(CAST(ROW_NUMBER() OVER () AS STRING), 12, '0')) AS visit_id,
  CONCAT('S', LPAD(CAST(MOD(CAST(RAND() * 500 AS INT64), 500) AS STRING), 4, '0')) AS store_id,
  CASE WHEN RAND() < 0.4 THEN NULL ELSE p.email END AS email,
  CASE WHEN RAND() < 0.4 THEN NULL ELSE p.phone END AS phone,
  CASE WHEN RAND() < 0.25 THEN NULL ELSE p.loyalty_id END AS loyalty_id,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(RAND() * 45 AS INT64) DAY) AS visit_ts
FROM idr_work.person_pool p
CROSS JOIN UNNEST(GENERATE_ARRAY(1, CAST({N_ROWS} / {N_PEOPLE} AS INT64) + 1)) AS x
LIMIT {N_ROWS}
""")
print(f"      ✅ {time.time() - start:.1f}s")

# Verify counts
print("\n📊 Table counts:")
for table in ['crm.customer', 'sales.transactions', 'loyalty.loyalty_accounts', 'digital.web_events', 'store.store_visits']:
    count = collect_one(f"SELECT COUNT(*) FROM {table}")
    print(f"   {table}: {count:,}")

# ============================================
# RUN BENCHMARK
# ============================================
print("\n🚀 Running IDR benchmark...")
benchmark_start = time.time()

# Import and run IDR
import subprocess
result = subprocess.run([
    'python', 'idr_run.py',
    f'--project={PROJECT}',
    '--run-mode=FULL',
    '--max-iters=50'
], capture_output=True, text=True, cwd='.')

print(result.stdout)
if result.returncode != 0:
    print(f"Error: {result.stderr}")

benchmark_duration = time.time() - benchmark_start

# ============================================
# BENCHMARK RESULTS
# ============================================
print("\n" + "=" * 70)
print("BENCHMARK RESULTS")
print("=" * 70)

run_history = list(client.query("""
    SELECT * FROM idr_out.run_history
    ORDER BY started_at DESC
    LIMIT 1
""").result())

if run_history:
    r = dict(run_history[0])
    print(f"Scale: {args.scale} ({N_ROWS * 5:,} total rows)")
    print(f"Run ID: {r.get('run_id', 'N/A')}")
    print(f"Status: {r.get('status', 'N/A')}")
    print(f"Duration: {r.get('duration_seconds', 0)}s ({r.get('duration_seconds', 0)/60:.1f} min)")
    print(f"LP Iterations: {r.get('lp_iterations', 0)}")
    print(f"Entities: {r.get('entities_processed', 0):,}")
    print(f"Edges: {r.get('edges_created', 0):,}")
    print(f"Clusters: {r.get('clusters_impacted', 0):,}")

# Cluster distribution
print("\nCluster Size Distribution:")
clusters = client.query("""
    SELECT 
      CASE 
        WHEN cluster_size = 1 THEN '01_singleton'
        WHEN cluster_size <= 5 THEN '02_small'
        WHEN cluster_size <= 20 THEN '03_medium'
        WHEN cluster_size <= 100 THEN '04_large'
        ELSE '05_xlarge'
      END AS bucket,
      COUNT(*) AS count
    FROM idr_out.identity_clusters_current
    GROUP BY 1
    ORDER BY 1
""").result()
for row in clusters:
    print(f"   {row[0]}: {row[1]:,}")

print("=" * 70)

# Cleanup
q("DROP TABLE IF EXISTS idr_work.person_pool")
print("\n✅ Scale test complete!")
