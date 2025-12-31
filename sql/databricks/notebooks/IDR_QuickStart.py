# Databricks notebook source
# MAGIC %md
# MAGIC # IDR Quick Start
# MAGIC 
# MAGIC **One notebook to run the complete identity resolution demo.**
# MAGIC 
# MAGIC This notebook:
# MAGIC 1. ✅ Creates schemas and runs DDL
# MAGIC 2. ✅ Generates sample source data (5 tables)
# MAGIC 3. ✅ Loads metadata configuration
# MAGIC 4. ✅ Runs identity resolution
# MAGIC 5. ✅ Shows results and sample queries
# MAGIC 
# MAGIC **Time**: ~5-10 minutes with default settings

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
from datetime import datetime, timedelta
import time
import uuid
import random
import string

# COMMAND ----------
# Configuration widgets
dbutils.widgets.text("CATALOG", "main")
dbutils.widgets.text("N_ROWS_PER_TABLE", "2000")  # Increase for scale testing
dbutils.widgets.text("SEED", "42")

CATALOG = dbutils.widgets.get("CATALOG").strip()
N_ROWS = int(dbutils.widgets.get("N_ROWS_PER_TABLE"))
SEED = int(dbutils.widgets.get("SEED"))

random.seed(SEED)

# Schema names
CRM = "crm"
SALES = "sales"
LOY = "loyalty"
DIG = "digital"
STORE = "store"

print(f"📊 Quick Start Configuration")
print(f"   Catalog: {CATALOG}")
print(f"   Rows per table: {N_ROWS:,}")
print(f"   Seed: {SEED}")

# COMMAND ----------
def q(sql: str):
    return spark.sql(sql)

def collect(sql: str):
    return q(sql).collect()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Create Schemas & DDL

# COMMAND ----------
print("🔧 Step 1: Creating schemas and tables...")

# Create schemas
q("CREATE SCHEMA IF NOT EXISTS idr_meta")
q("CREATE SCHEMA IF NOT EXISTS idr_work")
q("CREATE SCHEMA IF NOT EXISTS idr_out")
q(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{CRM}")
q(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SALES}")
q(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{LOY}")
q(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{DIG}")
q(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{STORE}")

# Metadata tables
q("""
CREATE TABLE IF NOT EXISTS idr_meta.source_table (
  table_id STRING, table_fqn STRING, entity_type STRING, entity_key_expr STRING,
  watermark_column STRING, watermark_lookback_minutes INT, is_active BOOLEAN
)""")

q("""
CREATE TABLE IF NOT EXISTS idr_meta.run_state (
  table_id STRING, last_watermark_value TIMESTAMP, last_run_id STRING, last_run_ts TIMESTAMP
)""")

q("""
CREATE TABLE IF NOT EXISTS idr_meta.source (
  table_id STRING, source_name STRING, trust_rank INT, is_active BOOLEAN
)""")

q("""
CREATE TABLE IF NOT EXISTS idr_meta.rule (
  rule_id STRING, rule_name STRING, is_active BOOLEAN, priority INT,
  identifier_type STRING, canonicalize STRING, allow_hashed BOOLEAN, require_non_null BOOLEAN
)""")

q("""
CREATE TABLE IF NOT EXISTS idr_meta.identifier_mapping (
  table_id STRING, identifier_type STRING, identifier_value_expr STRING, is_hashed BOOLEAN
)""")

q("""
CREATE TABLE IF NOT EXISTS idr_meta.entity_attribute_mapping (
  table_id STRING, attribute_name STRING, attribute_expr STRING
)""")

q("""
CREATE TABLE IF NOT EXISTS idr_meta.survivorship_rule (
  attribute_name STRING, strategy STRING, source_priority_list STRING, recency_field STRING
)""")

# Output tables
q("""
CREATE TABLE IF NOT EXISTS idr_out.identity_edges_current (
  rule_id STRING, left_entity_key STRING, right_entity_key STRING,
  identifier_type STRING, identifier_value_norm STRING,
  first_seen_ts TIMESTAMP, last_seen_ts TIMESTAMP
)""")

q("""
CREATE TABLE IF NOT EXISTS idr_out.identity_resolved_membership_current (
  entity_key STRING, resolved_id STRING, updated_ts TIMESTAMP
)""")

q("""
CREATE TABLE IF NOT EXISTS idr_out.identity_clusters_current (
  resolved_id STRING, cluster_size BIGINT, updated_ts TIMESTAMP
)""")

q("""
CREATE TABLE IF NOT EXISTS idr_out.golden_profile_current (
  resolved_id STRING, email_primary STRING, phone_primary STRING,
  first_name STRING, last_name STRING, updated_ts TIMESTAMP
)""")

q("""
CREATE TABLE IF NOT EXISTS idr_out.rule_match_audit_current (
  run_id STRING, rule_id STRING, edges_created BIGINT, started_at TIMESTAMP, ended_at TIMESTAMP
)""")

print("✅ Step 1 complete: Schemas and tables created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Generate Sample Data

# COMMAND ----------
print(f"📝 Step 2: Generating {N_ROWS:,} rows per source table...")

# Helper functions
def rand_name():
    first = random.choice(["Anil","Rashmi","Asha","Rohan","Maya","Vikram","Neha","Arjun","Priya","Kiran"])
    last = random.choice(["Kulkarni","Sharma","Patel","Reddy","Gupta","Iyer","Nair","Singh","Das","Khan"])
    return first, last

def rand_phone():
    return "9" + "".join(random.choice(string.digits) for _ in range(9))

def email_for(first, last, idx):
    base = f"{first}.{last}{idx}".lower().replace(" ", "")
    domain = random.choice(["gmail.com", "yahoo.com", "outlook.com", "example.com"])
    return f"{base}@{domain}"

def maybe_case_variant(email):
    if email is None:
        return None
    if random.random() < 0.35:
        return email.upper() if random.random() < 0.5 else email.title()
    return email

start_dt = datetime.utcnow() - timedelta(days=45)
end_dt = datetime.utcnow()

def rand_ts():
    delta = end_dt - start_dt
    return start_dt + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

# Build shared person pool (creates identity overlaps)
n_people = max(500, int(N_ROWS * 0.6))
people = [(pid, *rand_name(), email_for(*rand_name(), pid), rand_phone(), f"L{100000+pid}") 
          for pid in range(1, n_people+1)]
people_schema = "person_id int, first_name string, last_name string, email string, phone string, loyalty_id string"
people_df = spark.createDataFrame([(p[0], p[1], p[2], p[3], p[4], p[5]) for p in people], schema=people_schema).cache()
people_local = people_df.collect()

# Customer table
cust_rows = []
for i in range(1, N_ROWS+1):
    p = random.choice(people_local)
    fn, ln = p["first_name"], p["last_name"]
    if random.random() < 0.2:
        fn = fn[0] + "."
    cust_rows.append((f"C{i:06d}", fn, ln, maybe_case_variant(p["email"]), p["phone"],
                      p["loyalty_id"] if random.random() < 0.9 else None, rand_ts(), rand_ts()))

cust_df = spark.createDataFrame(cust_rows, 
    "customer_id string, first_name string, last_name string, email string, phone string, loyalty_id string, rec_create_dt timestamp, rec_update_dt timestamp")
q(f"DROP TABLE IF EXISTS {CATALOG}.{CRM}.customer")
cust_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{CRM}.customer")

# Transactions table
txn_rows = [(f"O{i:07d}", str(random.randint(1,3)), 
             maybe_case_variant(random.choice(people_local)["email"]) if random.random() < 0.9 else None,
             random.choice(people_local)["phone"] if random.random() < 0.9 else None, rand_ts())
            for i in range(1, N_ROWS+1)]
txn_df = spark.createDataFrame(txn_rows, "order_id string, line_id string, customer_email string, customer_phone string, load_ts timestamp")
q(f"DROP TABLE IF EXISTS {CATALOG}.{SALES}.transactions")
txn_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SALES}.transactions")

# Loyalty accounts table
loy_rows = [(f"LA{i:07d}", random.choice(people_local)["loyalty_id"],
             maybe_case_variant(random.choice(people_local)["email"]) if random.random() < 0.85 else None,
             random.choice(people_local)["phone"] if random.random() < 0.85 else None,
             random.choice(["BRONZE","SILVER","GOLD","PLATINUM"]), random.randint(0, 25000), rand_ts())
            for i in range(1, N_ROWS+1)]
loy_df = spark.createDataFrame(loy_rows, "loyalty_account_id string, loyalty_id string, email string, phone string, tier string, points int, updated_at timestamp")
q(f"DROP TABLE IF EXISTS {CATALOG}.{LOY}.loyalty_accounts")
loy_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{LOY}.loyalty_accounts")

# Web events table
web_rows = [(str(uuid.uuid4()), f"anon_{random.randint(1, 1200):05d}",
             random.choice(["page_view","product_view","add_to_cart","checkout_start"]),
             maybe_case_variant(random.choice(people_local)["email"]) if random.random() < 0.7 else None,
             random.choice(people_local)["phone"] if random.random() < 0.3 else None, rand_ts())
            for i in range(1, N_ROWS+1)]
web_df = spark.createDataFrame(web_rows, "event_id string, anonymous_id string, event_type string, email string, phone string, event_ts timestamp")
q(f"DROP TABLE IF EXISTS {CATALOG}.{DIG}.web_events")
web_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{DIG}.web_events")

# Store visits table
visit_rows = [(f"V{i:08d}", f"S{random.randint(1,250):04d}",
               maybe_case_variant(random.choice(people_local)["email"]) if random.random() < 0.6 else None,
               random.choice(people_local)["phone"] if random.random() < 0.6 else None,
               random.choice(people_local)["loyalty_id"] if random.random() < 0.8 else None, rand_ts())
              for i in range(1, N_ROWS+1)]
visit_df = spark.createDataFrame(visit_rows, "visit_id string, store_id string, email string, phone string, loyalty_id string, visit_ts timestamp")
q(f"DROP TABLE IF EXISTS {CATALOG}.{STORE}.store_visits")
visit_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{STORE}.store_visits")

print(f"✅ Step 2 complete: Created 5 source tables with {N_ROWS:,} rows each")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Load Metadata

# COMMAND ----------
print("📋 Step 3: Loading metadata configuration...")

# Clear existing metadata
q("TRUNCATE TABLE idr_meta.source_table")
q("TRUNCATE TABLE idr_meta.source")
q("TRUNCATE TABLE idr_meta.rule")
q("TRUNCATE TABLE idr_meta.identifier_mapping")
q("TRUNCATE TABLE idr_meta.entity_attribute_mapping")
q("TRUNCATE TABLE idr_meta.survivorship_rule")

# Source tables
q(f"""
INSERT INTO idr_meta.source_table VALUES
  ('customer', '{CATALOG}.{CRM}.customer', 'PERSON', 'customer_id', 'rec_create_dt', 0, true),
  ('transactions', '{CATALOG}.{SALES}.transactions', 'PERSON', "order_id||'-'||line_id", 'load_ts', 0, true),
  ('loyalty_accounts', '{CATALOG}.{LOY}.loyalty_accounts', 'PERSON', 'loyalty_account_id', 'updated_at', 0, true),
  ('web_events', '{CATALOG}.{DIG}.web_events', 'PERSON', 'event_id', 'event_ts', 0, true),
  ('store_visits', '{CATALOG}.{STORE}.store_visits', 'PERSON', 'visit_id', 'visit_ts', 0, true)
""")

# Source trust rankings
q("""
INSERT INTO idr_meta.source VALUES
  ('customer', 'CRM', 1, true),
  ('loyalty_accounts', 'Loyalty', 1, true),
  ('store_visits', 'Store', 2, true),
  ('transactions', 'POS', 3, true),
  ('web_events', 'Digital', 4, true)
""")

# Matching rules
q("""
INSERT INTO idr_meta.rule VALUES
  ('R_EMAIL_EXACT', 'Email exact match', true, 1, 'EMAIL', 'LOWERCASE', true, true),
  ('R_PHONE_EXACT', 'Phone exact match', true, 2, 'PHONE', 'NONE', true, true),
  ('R_LOYALTY_EXACT', 'Loyalty ID exact match', true, 3, 'LOYALTY_ID', 'NONE', true, true)
""")

# Identifier mappings
q("""
INSERT INTO idr_meta.identifier_mapping VALUES
  ('customer', 'EMAIL', 'email', false),
  ('customer', 'PHONE', 'phone', false),
  ('customer', 'LOYALTY_ID', 'loyalty_id', false),
  ('transactions', 'EMAIL', 'customer_email', false),
  ('transactions', 'PHONE', 'customer_phone', false),
  ('loyalty_accounts', 'EMAIL', 'email', false),
  ('loyalty_accounts', 'PHONE', 'phone', false),
  ('loyalty_accounts', 'LOYALTY_ID', 'loyalty_id', false),
  ('web_events', 'EMAIL', 'email', false),
  ('web_events', 'PHONE', 'phone', false),
  ('store_visits', 'EMAIL', 'email', false),
  ('store_visits', 'PHONE', 'phone', false),
  ('store_visits', 'LOYALTY_ID', 'loyalty_id', false)
""")

# Entity attribute mappings
q("""
INSERT INTO idr_meta.entity_attribute_mapping VALUES
  ('customer', 'record_updated_at', 'rec_update_dt'),
  ('customer', 'first_name', 'first_name'),
  ('customer', 'last_name', 'last_name'),
  ('customer', 'email_raw', 'email'),
  ('customer', 'phone_raw', 'phone'),
  ('loyalty_accounts', 'record_updated_at', 'updated_at'),
  ('loyalty_accounts', 'email_raw', 'email'),
  ('loyalty_accounts', 'phone_raw', 'phone'),
  ('web_events', 'record_updated_at', 'event_ts'),
  ('web_events', 'email_raw', 'email'),
  ('web_events', 'phone_raw', 'phone'),
  ('store_visits', 'record_updated_at', 'visit_ts'),
  ('store_visits', 'email_raw', 'email'),
  ('store_visits', 'phone_raw', 'phone'),
  ('transactions', 'record_updated_at', 'load_ts'),
  ('transactions', 'email_raw', 'customer_email'),
  ('transactions', 'phone_raw', 'customer_phone')
""")

# Survivorship rules
q("""
INSERT INTO idr_meta.survivorship_rule VALUES
  ('email_primary', 'SOURCE_PRIORITY', 'CRM,Loyalty,Store,POS,Digital', 'record_updated_at'),
  ('phone_primary', 'SOURCE_PRIORITY', 'CRM,Loyalty,Store,POS,Digital', 'record_updated_at'),
  ('first_name', 'MOST_RECENT', NULL, 'record_updated_at'),
  ('last_name', 'MOST_RECENT', NULL, 'record_updated_at')
""")

# Initialize run state
q("""
MERGE INTO idr_meta.run_state tgt
USING (SELECT table_id FROM idr_meta.source_table WHERE is_active = true) src
ON tgt.table_id = src.table_id
WHEN NOT MATCHED THEN INSERT (table_id, last_watermark_value, last_run_id, last_run_ts)
VALUES (src.table_id, TIMESTAMP('1900-01-01 00:00:00'), NULL, NULL)
""")

print("✅ Step 3 complete: Metadata loaded")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: Run IDR

# COMMAND ----------
print("🚀 Step 4: Running identity resolution...")
print("   This may take a few minutes...")

# Run the IDR_Run notebook
result = dbutils.notebook.run(
    "./IDR_Run",
    timeout_seconds=1800,
    arguments={"RUN_MODE": "FULL", "MAX_ITERS": "30"}
)

print(f"✅ Step 4 complete: IDR run finished")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5: Results Summary

# COMMAND ----------
# Display summary statistics
print("📊 Step 5: Results Summary")
print("="*60)

edge_count = q("SELECT COUNT(*) FROM idr_out.identity_edges_current").first()[0]
member_count = q("SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current").first()[0]
cluster_count = q("SELECT COUNT(*) FROM idr_out.identity_clusters_current").first()[0]
profile_count = q("SELECT COUNT(*) FROM idr_out.golden_profile_current").first()[0]

print(f"   Edges created:           {edge_count:,}")
print(f"   Entities resolved:       {member_count:,}")
print(f"   Unique clusters:         {cluster_count:,}")
print(f"   Golden profiles:         {profile_count:,}")
print("="*60)

# COMMAND ----------
# Cluster size distribution
print("\n📈 Cluster Size Distribution:")
display(q("""
SELECT 
  CASE 
    WHEN cluster_size = 1 THEN '1 (singletons)'
    WHEN cluster_size <= 5 THEN '2-5'
    WHEN cluster_size <= 20 THEN '6-20'
    WHEN cluster_size <= 100 THEN '21-100'
    ELSE '100+'
  END AS size_bucket,
  COUNT(*) AS cluster_count,
  SUM(cluster_size) AS total_entities
FROM idr_out.identity_clusters_current
GROUP BY 1
ORDER BY 1
"""))

# COMMAND ----------
# Sample golden profiles
print("\n👤 Sample Golden Profiles:")
display(q("""
SELECT resolved_id, email_primary, phone_primary, first_name, last_name
FROM idr_out.golden_profile_current
WHERE email_primary IS NOT NULL
LIMIT 10
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Sample Queries
# MAGIC 
# MAGIC Use these queries to explore your resolved identities:

# COMMAND ----------
# Find all entities for a specific resolved_id
# Uncomment and replace with an actual resolved_id from the results above
# display(q("""
#   SELECT m.entity_key, m.resolved_id
#   FROM idr_out.identity_resolved_membership_current m
#   WHERE m.resolved_id = 'customer:C000001'
# """))

# COMMAND ----------
# Find edges for a specific identifier
# display(q("""
#   SELECT * FROM idr_out.identity_edges_current
#   WHERE identifier_value_norm LIKE '%example.com%'
#   LIMIT 20
# """))

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Quick Start Complete!
# MAGIC 
# MAGIC **What was created:**
# MAGIC - 5 source tables with sample data
# MAGIC - Identity edges linking entities
# MAGIC - Resolved identity clusters
# MAGIC - Golden profiles with best values
# MAGIC 
# MAGIC **Next steps:**
# MAGIC - Explore the queries above
# MAGIC - Read [Architecture](../../../docs/architecture.md) for algorithm details
# MAGIC - Read [Scale Considerations](../../../docs/scale_considerations.md) for production guidance
# MAGIC - Try with your own data by modifying metadata tables
