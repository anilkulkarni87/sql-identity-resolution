#!/usr/bin/env python3
"""
DuckDB Sample Data Generator
Creates demo source tables with overlapping identities.

Usage:
    python idr_sample_data.py --db=idr.duckdb --rows=2000
    
Prerequisites:
    pip install duckdb
"""

import argparse
import random
import string
import uuid
from datetime import datetime, timedelta

try:
    import duckdb
except ImportError:
    print("Please install duckdb: pip install duckdb")
    exit(1)

# ============================================
# CONFIGURATION
# ============================================
parser = argparse.ArgumentParser(description='DuckDB Sample Data Generator')
parser.add_argument('--db', default='idr.duckdb', help='DuckDB database file')
parser.add_argument('--rows', type=int, default=2000, help='Rows per table')
parser.add_argument('--seed', type=int, default=42, help='Random seed')
args = parser.parse_args()

DB_PATH = args.db
N_ROWS = args.rows
random.seed(args.seed)

con = duckdb.connect(DB_PATH)

print(f"🦆 Generating sample data in {DB_PATH}")
print(f"   Rows per table: {N_ROWS}")

# ============================================
# CREATE SCHEMAS
# ============================================
con.execute("CREATE SCHEMA IF NOT EXISTS crm")
con.execute("CREATE SCHEMA IF NOT EXISTS sales")
con.execute("CREATE SCHEMA IF NOT EXISTS loyalty")
con.execute("CREATE SCHEMA IF NOT EXISTS digital")
con.execute("CREATE SCHEMA IF NOT EXISTS store")

# ============================================
# GENERATE PERSON POOL
# ============================================
first_names = ['Anil','Rashmi','Asha','Rohan','Maya','Vikram','Neha','Arjun','Priya','Kiran']
last_names = ['Kulkarni','Sharma','Patel','Reddy','Gupta','Iyer','Nair','Singh','Das','Khan']
domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'example.com']
tiers = ['BRONZE', 'SILVER', 'GOLD', 'PLATINUM']
events = ['page_view', 'product_view', 'add_to_cart', 'checkout_start']

n_people = max(500, int(N_ROWS * 0.6))
people = []
for pid in range(1, n_people + 1):
    fn = random.choice(first_names)
    ln = random.choice(last_names)
    email = f"{fn.lower()}.{ln.lower()}{pid}@{random.choice(domains)}"
    phone = '9' + ''.join(random.choice(string.digits) for _ in range(9))
    loyalty_id = f"L{100000 + pid}"
    people.append({'fn': fn, 'ln': ln, 'email': email, 'phone': phone, 'loyalty_id': loyalty_id})

start_dt = datetime.utcnow() - timedelta(days=45)
end_dt = datetime.utcnow()

def rand_ts():
    delta = end_dt - start_dt
    return start_dt + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def case_variant(email):
    if email is None:
        return None
    if random.random() < 0.35:
        return email.upper() if random.random() < 0.5 else email.title()
    return email

# ============================================
# 1. CUSTOMER TABLE
# ============================================
print("📊 Creating crm.customer...")
rows = []
for i in range(1, N_ROWS + 1):
    p = random.choice(people)
    fn = p['fn'] if random.random() > 0.2 else p['fn'][0] + "."
    rows.append((
        f"C{i:06d}", fn, p['ln'], case_variant(p['email']), p['phone'],
        p['loyalty_id'] if random.random() < 0.9 else None,
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

# ============================================
# 2. TRANSACTIONS TABLE
# ============================================
print("📊 Creating sales.transactions...")
rows = []
for i in range(1, N_ROWS + 1):
    p = random.choice(people)
    rows.append((
        f"O{i:07d}", str(random.randint(1, 3)),
        case_variant(p['email']) if random.random() < 0.9 else None,
        p['phone'] if random.random() < 0.9 else None,
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

# ============================================
# 3. LOYALTY_ACCOUNTS TABLE
# ============================================
print("📊 Creating loyalty.loyalty_accounts...")
rows = []
for i in range(1, N_ROWS + 1):
    p = random.choice(people)
    rows.append((
        f"LA{i:07d}", p['loyalty_id'],
        case_variant(p['email']) if random.random() < 0.85 else None,
        p['phone'] if random.random() < 0.85 else None,
        random.choice(tiers), random.randint(0, 25000), rand_ts()
    ))

con.execute("DROP TABLE IF EXISTS loyalty.loyalty_accounts")
con.execute("""
CREATE TABLE loyalty.loyalty_accounts (
    loyalty_account_id VARCHAR, loyalty_id VARCHAR, email VARCHAR,
    phone VARCHAR, tier VARCHAR, points INTEGER, updated_at TIMESTAMP
)
""")
con.executemany("INSERT INTO loyalty.loyalty_accounts VALUES (?, ?, ?, ?, ?, ?, ?)", rows)

# ============================================
# 4. WEB_EVENTS TABLE
# ============================================
print("📊 Creating digital.web_events...")
rows = []
for i in range(1, N_ROWS + 1):
    p = random.choice(people)
    rows.append((
        str(uuid.uuid4()), f"anon_{random.randint(1, 1200):05d}",
        random.choice(events),
        case_variant(p['email']) if random.random() < 0.7 else None,
        p['phone'] if random.random() < 0.3 else None,
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

# ============================================
# 5. STORE_VISITS TABLE
# ============================================
print("📊 Creating store.store_visits...")
rows = []
for i in range(1, N_ROWS + 1):
    p = random.choice(people)
    rows.append((
        f"V{i:08d}", f"S{random.randint(1, 250):04d}",
        case_variant(p['email']) if random.random() < 0.6 else None,
        p['phone'] if random.random() < 0.6 else None,
        p['loyalty_id'] if random.random() < 0.8 else None,
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

# ============================================
# LOAD METADATA
# ============================================
print("📋 Loading metadata...")

# Clear existing
con.execute("DELETE FROM idr_meta.source_table")
con.execute("DELETE FROM idr_meta.source")
con.execute("DELETE FROM idr_meta.rule")
con.execute("DELETE FROM idr_meta.identifier_mapping")
con.execute("DELETE FROM idr_meta.entity_attribute_mapping")
con.execute("DELETE FROM idr_meta.survivorship_rule")

# Source tables
con.executemany("INSERT INTO idr_meta.source_table VALUES (?, ?, ?, ?, ?, ?, ?)", [
    ('customer', 'crm.customer', 'PERSON', 'customer_id', 'rec_create_dt', 0, True),
    ('transactions', 'sales.transactions', 'PERSON', "order_id || '-' || line_id", 'load_ts', 0, True),
    ('loyalty_accounts', 'loyalty.loyalty_accounts', 'PERSON', 'loyalty_account_id', 'updated_at', 0, True),
    ('web_events', 'digital.web_events', 'PERSON', 'event_id', 'event_ts', 0, True),
    ('store_visits', 'store.store_visits', 'PERSON', 'visit_id', 'visit_ts', 0, True),
])

# Source trust ranking
con.executemany("INSERT INTO idr_meta.source VALUES (?, ?, ?, ?)", [
    ('customer', 'CRM', 1, True),
    ('loyalty_accounts', 'Loyalty', 1, True),
    ('store_visits', 'Store', 2, True),
    ('transactions', 'POS', 3, True),
    ('web_events', 'Digital', 4, True),
])

# Rules
con.executemany("INSERT INTO idr_meta.rule VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [
    ('R_EMAIL_EXACT', 'Email exact match', True, 1, 'EMAIL', 'LOWERCASE', True, True),
    ('R_PHONE_EXACT', 'Phone exact match', True, 2, 'PHONE', 'NONE', True, True),
    ('R_LOYALTY_EXACT', 'Loyalty ID exact match', True, 3, 'LOYALTY_ID', 'NONE', True, True),
])

# Identifier mappings
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

# Entity attribute mappings
con.executemany("INSERT INTO idr_meta.entity_attribute_mapping VALUES (?, ?, ?)", [
    ('customer', 'record_updated_at', 'rec_update_dt'),
    ('customer', 'first_name', 'first_name'),
    ('customer', 'last_name', 'last_name'),
    ('customer', 'email_raw', 'email'),
    ('customer', 'phone_raw', 'phone'),
])

# Survivorship rules
con.executemany("INSERT INTO idr_meta.survivorship_rule VALUES (?, ?, ?, ?)", [
    ('email_primary', 'SOURCE_PRIORITY', 'CRM,Loyalty,Store,POS,Digital', 'record_updated_at'),
    ('phone_primary', 'SOURCE_PRIORITY', 'CRM,Loyalty,Store,POS,Digital', 'record_updated_at'),
    ('first_name', 'MOST_RECENT', None, 'record_updated_at'),
    ('last_name', 'MOST_RECENT', None, 'record_updated_at'),
])

con.close()

print(f"\n✅ Sample data created!")
print(f"   Database: {DB_PATH}")
print(f"   Tables: 5 source tables with {N_ROWS} rows each")
print(f"   Metadata: Loaded all configuration")
print(f"\n   Next: python idr_run.py --db={DB_PATH} --run-mode=FULL")
