#!/usr/bin/env python3
"""
DuckDB Scale Test & Benchmark
Generate realistic test data and benchmark IDR performance locally.

DuckDB is best for local testing up to ~10M rows.

Sizing Recommendations:
| Rows per Table | Total Rows | RAM Required | Expected Duration |
|----------------|------------|--------------|-------------------|
| 100K | 500K | 2 GB | 30s - 2 min |
| 500K | 2.5M | 4 GB | 2-5 min |
| 1M | 5M | 8 GB | 5-15 min |
| 2M | 10M | 16 GB | 15-30 min |

Usage:
    python idr_scale_test.py --db=scale_test.duckdb --scale=1M
"""

import argparse
import time
import uuid
import random
import string
from datetime import datetime, timedelta

try:
    import duckdb
except ImportError:
    print("Please install duckdb: pip install duckdb")
    exit(1)

# ============================================
# CONFIGURATION
# ============================================
parser = argparse.ArgumentParser(description='DuckDB Scale Test')
parser.add_argument('--db', default='scale_test.duckdb', help='DuckDB database file')
parser.add_argument('--scale', default='500K', choices=['100K', '500K', '1M', '2M'])
parser.add_argument('--overlap-rate', type=float, default=0.6, help='Identity overlap (0.3-0.7)')
parser.add_argument('--seed', type=int, default=42)
args = parser.parse_args()

SCALE_MAP = {"100K": 100000, "500K": 500000, "1M": 1000000, "2M": 2000000}
DB_PATH = args.db
N_ROWS = SCALE_MAP[args.scale]
OVERLAP_RATE = args.overlap_rate
N_PEOPLE = int(N_ROWS * OVERLAP_RATE)

random.seed(args.seed)

print(f"🦆 DuckDB Scale Test")
print(f"   Database: {DB_PATH}")
print(f"   Scale: {args.scale} ({N_ROWS:,} rows per table)")
print(f"   Total rows: {N_ROWS * 5:,}")
print(f"   Overlap rate: {OVERLAP_RATE} ({N_PEOPLE:,} unique identities)")

# ============================================
# SETUP DATABASE
# ============================================
con = duckdb.connect(DB_PATH)

# Run DDL
print("\n🔧 Setting up schemas...")
con.execute(open('00_ddl_all.sql').read())

# Create source schemas
for schema in ['crm', 'sales', 'loyalty', 'digital', 'store']:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# ============================================
# GENERATE PERSON POOL
# ============================================
print(f"\n📝 Generating {N_PEOPLE:,} unique identities...")

first_names = ['Sameer','Govind','Asha','Rohan','Maya','Vikram','Neha','Arjun','Priya','Kiran',
               'Raj','Sita','Kavita','Suresh','Anita','Deepak','Meera','Rahul','Pooja','Amit']
last_names = ['Kulkarni','Sharma','Patel','Reddy','Gupta','Iyer','Nair','Singh','Das','Khan',
              'Verma','Joshi','Rao','Kumar','Mehta','Shah','Pandey','Thakur','Yadav','Mishra']
domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'example.com', 'company.com']
tiers = ['BRONZE', 'SILVER', 'GOLD', 'PLATINUM']
events = ['page_view', 'product_view', 'add_to_cart', 'checkout_start', 'purchase']

people = []
for pid in range(1, N_PEOPLE + 1):
    fn = first_names[pid % len(first_names)]
    ln = last_names[(pid // len(first_names)) % len(last_names)]
    email = f"{fn.lower()}.{ln.lower()}{pid}@{domains[pid % len(domains)]}"
    phone = '9' + ''.join(random.choice(string.digits) for _ in range(9))
    loyalty_id = f"L{100000 + pid}"
    people.append((pid, fn, ln, email, phone, loyalty_id))

con.execute("DROP TABLE IF EXISTS person_pool")
con.execute("""
CREATE TABLE person_pool (
    person_id INTEGER, first_name VARCHAR, last_name VARCHAR,
    email VARCHAR, phone VARCHAR, loyalty_id VARCHAR
)
""")
con.executemany("INSERT INTO person_pool VALUES (?, ?, ?, ?, ?, ?)", people)
print(f"   ✅ Person pool created")

# ============================================
# GENERATE SOURCE TABLES
# ============================================
start_dt = datetime.utcnow() - timedelta(days=45)
end_dt = datetime.utcnow()

def rand_ts():
    delta = end_dt - start_dt
    return start_dt + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def case_variant(email):
    if email is None:
        return None
    r = random.random()
    if r < 0.17:
        return email.upper()
    elif r < 0.35:
        return email.title()
    return email

# Customer table
print("   Creating crm.customer...")
start = time.time()
rows = []
for i in range(1, N_ROWS + 1):
    p = people[random.randint(0, N_PEOPLE - 1)]
    fn = p[1] if random.random() > 0.1 else p[1][0] + "."
    rows.append((
        f"C{i:08d}", fn, p[2],
        None if random.random() < 0.05 else case_variant(p[3]),
        None if random.random() < 0.1 else p[4],
        None if random.random() < 0.15 else p[5],
        rand_ts(), rand_ts()
    ))
    
con.execute("DROP TABLE IF EXISTS crm.customer")
con.execute("""
CREATE TABLE crm.customer (
    customer_id VARCHAR, first_name VARCHAR, last_name VARCHAR,
    email VARCHAR, phone VARCHAR, loyalty_id VARCHAR,
    rec_create_dt TIMESTAMP, rec_update_dt TIMESTAMP
)
""")
con.executemany("INSERT INTO crm.customer VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
print(f"      ✅ {time.time() - start:.1f}s, {len(rows):,} rows")

# Transactions table
print("   Creating sales.transactions...")
start = time.time()
rows = []
for i in range(1, N_ROWS + 1):
    p = people[random.randint(0, N_PEOPLE - 1)]
    rows.append((
        f"O{i:010d}", str(random.randint(1, 3)),
        None if random.random() < 0.15 else case_variant(p[3]),
        None if random.random() < 0.2 else p[4],
        rand_ts()
    ))

con.execute("DROP TABLE IF EXISTS sales.transactions")
con.execute("""
CREATE TABLE sales.transactions (
    order_id VARCHAR, line_id VARCHAR, customer_email VARCHAR,
    customer_phone VARCHAR, load_ts TIMESTAMP
)
""")
con.executemany("INSERT INTO sales.transactions VALUES (?, ?, ?, ?, ?)", rows)
print(f"      ✅ {time.time() - start:.1f}s, {len(rows):,} rows")

# Loyalty accounts table
print("   Creating loyalty.loyalty_accounts...")
start = time.time()
rows = []
for i in range(1, N_ROWS + 1):
    p = people[random.randint(0, N_PEOPLE - 1)]
    rows.append((
        f"LA{i:010d}", p[5],
        None if random.random() < 0.2 else p[3],
        None if random.random() < 0.2 else p[4],
        random.choice(tiers), random.randint(0, 50000), rand_ts()
    ))

con.execute("DROP TABLE IF EXISTS loyalty.loyalty_accounts")
con.execute("""
CREATE TABLE loyalty.loyalty_accounts (
    loyalty_account_id VARCHAR, loyalty_id VARCHAR, email VARCHAR,
    phone VARCHAR, tier VARCHAR, points INTEGER, updated_at TIMESTAMP
)
""")
con.executemany("INSERT INTO loyalty.loyalty_accounts VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
print(f"      ✅ {time.time() - start:.1f}s, {len(rows):,} rows")

# Web events table
print("   Creating digital.web_events...")
start = time.time()
rows = []
for i in range(1, N_ROWS + 1):
    p = people[random.randint(0, N_PEOPLE - 1)]
    rows.append((
        str(uuid.uuid4()), f"anon_{random.randint(1, 10000):06d}",
        random.choice(events),
        None if random.random() < 0.35 else p[3],
        None if random.random() < 0.7 else p[4],
        rand_ts()
    ))

con.execute("DROP TABLE IF EXISTS digital.web_events")
con.execute("""
CREATE TABLE digital.web_events (
    event_id VARCHAR, anonymous_id VARCHAR, event_type VARCHAR,
    email VARCHAR, phone VARCHAR, event_ts TIMESTAMP
)
""")
con.executemany("INSERT INTO digital.web_events VALUES (?, ?, ?, ?, ?, ?)", rows)
print(f"      ✅ {time.time() - start:.1f}s, {len(rows):,} rows")

# Store visits table
print("   Creating store.store_visits...")
start = time.time()
rows = []
for i in range(1, N_ROWS + 1):
    p = people[random.randint(0, N_PEOPLE - 1)]
    rows.append((
        f"V{i:012d}", f"S{random.randint(1, 500):04d}",
        None if random.random() < 0.4 else p[3],
        None if random.random() < 0.4 else p[4],
        None if random.random() < 0.25 else p[5],
        rand_ts()
    ))

con.execute("DROP TABLE IF EXISTS store.store_visits")
con.execute("""
CREATE TABLE store.store_visits (
    visit_id VARCHAR, store_id VARCHAR, email VARCHAR,
    phone VARCHAR, loyalty_id VARCHAR, visit_ts TIMESTAMP
)
""")
con.executemany("INSERT INTO store.store_visits VALUES (?, ?, ?, ?, ?, ?)", rows)
print(f"      ✅ {time.time() - start:.1f}s, {len(rows):,} rows")

# Drop person pool
con.execute("DROP TABLE IF EXISTS person_pool")

# ============================================
# LOAD METADATA
# ============================================
print("\n📋 Loading metadata...")
con.execute("DELETE FROM idr_meta.source_table")
con.execute("DELETE FROM idr_meta.source")
con.execute("DELETE FROM idr_meta.rule")
con.execute("DELETE FROM idr_meta.identifier_mapping")

con.executemany("INSERT INTO idr_meta.source_table VALUES (?, ?, ?, ?, ?, ?, ?)", [
    ('customer', 'crm.customer', 'PERSON', 'customer_id', 'rec_create_dt', 0, True),
    ('transactions', 'sales.transactions', 'PERSON', "order_id || '-' || line_id", 'load_ts', 0, True),
    ('loyalty_accounts', 'loyalty.loyalty_accounts', 'PERSON', 'loyalty_account_id', 'updated_at', 0, True),
    ('web_events', 'digital.web_events', 'PERSON', 'event_id', 'event_ts', 0, True),
    ('store_visits', 'store.store_visits', 'PERSON', 'visit_id', 'visit_ts', 0, True),
])

con.executemany("INSERT INTO idr_meta.source VALUES (?, ?, ?, ?)", [
    ('customer', 'CRM', 1, True),
    ('loyalty_accounts', 'Loyalty', 1, True),
    ('store_visits', 'Store', 2, True),
    ('transactions', 'POS', 3, True),
    ('web_events', 'Digital', 4, True),
])

con.executemany("INSERT INTO idr_meta.rule VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [
    ('R_EMAIL_EXACT', 'Email exact match', True, 1, 'EMAIL', 'LOWERCASE', True, True),
    ('R_PHONE_EXACT', 'Phone exact match', True, 2, 'PHONE', 'NONE', True, True),
    ('R_LOYALTY_EXACT', 'Loyalty ID exact match', True, 3, 'LOYALTY_ID', 'NONE', True, True),
])

con.executemany("INSERT INTO idr_meta.identifier_mapping VALUES (?, ?, ?, ?)", [
    ('customer', 'EMAIL', 'email', False),
    ('customer', 'PHONE', 'phone', False),
    ('customer', 'LOYALTY_ID', 'loyalty_id', False),
    ('transactions', 'EMAIL', 'customer_email', False),
    ('transactions', 'PHONE', 'customer_phone', False),
    ('loyalty_accounts', 'EMAIL', 'email', False),
    ('loyalty_accounts', 'PHONE', 'phone', False),
    ('loyalty_accounts', 'LOYALTY_ID', 'loyalty_id', False),
    ('web_events', 'EMAIL', 'email', False),
    ('web_events', 'PHONE', 'phone', False),
    ('store_visits', 'EMAIL', 'email', False),
    ('store_visits', 'PHONE', 'phone', False),
    ('store_visits', 'LOYALTY_ID', 'loyalty_id', False),
])

con.close()

# ============================================
# RUN BENCHMARK
# ============================================
print("\n🚀 Running IDR benchmark...")
benchmark_start = time.time()

import subprocess
result = subprocess.run([
    'python', 'idr_run.py',
    f'--db={DB_PATH}',
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
con = duckdb.connect(DB_PATH)

print("\n" + "=" * 70)
print("BENCHMARK RESULTS")
print("=" * 70)

run_history = con.execute("""
    SELECT * FROM idr_out.run_history
    ORDER BY started_at DESC
    LIMIT 1
""").fetchone()

if run_history:
    print(f"Scale: {args.scale} ({N_ROWS * 5:,} total rows)")
    print(f"Run ID: {run_history[0]}")
    print(f"Status: {run_history[2]}")
    print(f"Duration: {run_history[5]}s ({run_history[5]/60:.1f} min)")
    print(f"LP Iterations: {run_history[10]}")
    print(f"Entities: {run_history[6]:,}")
    print(f"Edges: {run_history[7]:,}")
    print(f"Clusters: {run_history[9]:,}")

# Cluster distribution
print("\nCluster Size Distribution:")
clusters = con.execute("""
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
""").fetchall()
for row in clusters:
    print(f"   {row[0]}: {row[1]:,}")

print("=" * 70)

con.close()
print(f"\n✅ Scale test complete! Database: {DB_PATH}")
