# Databricks notebook source
# MAGIC %md
# MAGIC # IDR_SampleData_Generate (Databricks)
# MAGIC Creates **5 source tables** with configurable row counts and realistic overlaps for identity resolution demos.

# COMMAND ----------
from datetime import datetime, timedelta
import random
import uuid
import string

# COMMAND ----------
dbutils.widgets.text("CATALOG", "main")
dbutils.widgets.text("CRM_SCHEMA", "crm")
dbutils.widgets.text("SALES_SCHEMA", "sales")
dbutils.widgets.text("LOYALTY_SCHEMA", "loyalty")
dbutils.widgets.text("DIGITAL_SCHEMA", "digital")
dbutils.widgets.text("STORE_SCHEMA", "store")

dbutils.widgets.text("N_CUSTOMERS", "2000")
dbutils.widgets.text("N_TXNS", "2000")
dbutils.widgets.text("N_LOYALTY", "2000")
dbutils.widgets.text("N_WEB_EVENTS", "2000")
dbutils.widgets.text("N_STORE_VISITS", "2000")

dbutils.widgets.text("SEED", "42")

CATALOG = dbutils.widgets.get("CATALOG").strip()
CRM = dbutils.widgets.get("CRM_SCHEMA").strip()
SALES = dbutils.widgets.get("SALES_SCHEMA").strip()
LOY = dbutils.widgets.get("LOYALTY_SCHEMA").strip()
DIG = dbutils.widgets.get("DIGITAL_SCHEMA").strip()
STORE = dbutils.widgets.get("STORE_SCHEMA").strip()

N_CUSTOMERS = int(dbutils.widgets.get("N_CUSTOMERS"))
N_TXNS = int(dbutils.widgets.get("N_TXNS"))
N_LOYALTY = int(dbutils.widgets.get("N_LOYALTY"))
N_WEB_EVENTS = int(dbutils.widgets.get("N_WEB_EVENTS"))
N_STORE_VISITS = int(dbutils.widgets.get("N_STORE_VISITS"))
SEED = int(dbutils.widgets.get("SEED"))

random.seed(SEED)

# COMMAND ----------
# Create schemas
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{CRM}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SALES}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{LOY}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{DIG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{STORE}")

# COMMAND ----------
def rand_name():
    first = random.choice(["Anil","Rashmi","Asha","Rohan","Maya","Vikram","Neha","Arjun","Priya","Kiran","Deepa","Sanjay","Meera","Nikhil","Sonia"])
    last  = random.choice(["Kulkarni","Sharma","Patel","Reddy","Gupta","Iyer","Nair","Singh","Das","Khan","Joshi","Shetty","Menon"])
    return first, last

def rand_phone():
    return "9" + "".join(random.choice(string.digits) for _ in range(9))

def email_for(first,last,idx):
    base = f"{first}.{last}{idx}".lower().replace(" ", "")
    domain = random.choice(["gmail.com","yahoo.com","outlook.com","example.com"])
    return f"{base}@{domain}"

def maybe_case_variant(email):
    if email is None:
        return None
    if random.random() < 0.35:
        return email.upper() if random.random() < 0.5 else email.title()
    return email

def dt_range(days_back=45):
    now = datetime.utcnow()
    return now - timedelta(days=days_back), now

start_dt, end_dt = dt_range(45)

def rand_ts():
    delta = end_dt - start_dt
    return start_dt + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

# COMMAND ----------
# Build shared person pool
n_people = max(500, int(N_CUSTOMERS * 0.6))
people = []
for pid in range(1, n_people+1):
    first,last = rand_name()
    phone = rand_phone()
    email = email_for(first,last,pid)
    loyalty_id = f"L{100000+pid}"
    people.append((pid, first, last, email, phone, loyalty_id))

people_schema = "person_id int, first_name string, last_name string, email string, phone string, loyalty_id string"
people_df = spark.createDataFrame(people, schema=people_schema).cache()
people_local = people_df.collect()

# COMMAND ----------
# customer (watermark: rec_create_dt)
cust_rows = []
for i in range(1, N_CUSTOMERS+1):
    p = random.choice(people_local)
    fn = p["first_name"]
    ln = p["last_name"]
    if random.random() < 0.2:
        fn = fn[0] + "."
    email = maybe_case_variant(p["email"])
    phone = p["phone"]
    loyalty_id = p["loyalty_id"] if random.random() < 0.9 else None
    cust_rows.append((f"C{i:06d}", fn, ln, email, phone, loyalty_id, rand_ts(), rand_ts()))

cust_schema = "customer_id string, first_name string, last_name string, email string, phone string, loyalty_id string, rec_create_dt timestamp, rec_update_dt timestamp"
cust_df = spark.createDataFrame(cust_rows, schema=cust_schema)
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{CRM}.customer")
cust_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{CRM}.customer")

# COMMAND ----------
# transactions (watermark: load_ts)
txn_rows = []
for i in range(1, N_TXNS+1):
    p = random.choice(people_local)
    order_id = f"O{i:07d}"
    line_id = str(random.randint(1,3))
    email = maybe_case_variant(p["email"]) if random.random() < 0.9 else None
    phone = p["phone"] if random.random() < 0.9 else None
    txn_rows.append((order_id, line_id, email, phone, rand_ts()))

txn_schema = "order_id string, line_id string, customer_email string, customer_phone string, load_ts timestamp"
txn_df = spark.createDataFrame(txn_rows, schema=txn_schema)
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SALES}.transactions")
txn_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SALES}.transactions")

# COMMAND ----------
# loyalty_accounts (watermark: updated_at)
loy_rows = []
for i in range(1, N_LOYALTY+1):
    p = random.choice(people_local)
    acct_id = f"LA{i:07d}"
    loyalty_id = p["loyalty_id"]
    email = maybe_case_variant(p["email"]) if random.random() < 0.85 else None
    phone = p["phone"] if random.random() < 0.85 else None
    tier = random.choice(["BRONZE","SILVER","GOLD","PLATINUM"])
    points = random.randint(0, 25000)
    loy_rows.append((acct_id, loyalty_id, email, phone, tier, points, rand_ts()))

loy_schema = "loyalty_account_id string, loyalty_id string, email string, phone string, tier string, points int, updated_at timestamp"
loy_df = spark.createDataFrame(loy_rows, schema=loy_schema)
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{LOY}.loyalty_accounts")
loy_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{LOY}.loyalty_accounts")

# COMMAND ----------
# web_events (watermark: event_ts)
web_rows = []
for i in range(1, N_WEB_EVENTS+1):
    p = random.choice(people_local)
    event_id = str(uuid.uuid4())
    email = maybe_case_variant(p["email"]) if random.random() < 0.7 else None
    phone = p["phone"] if random.random() < 0.3 else None
    anonymous_id = f"anon_{random.randint(1, 1200):05d}"
    event_type = random.choice(["page_view","product_view","add_to_cart","checkout_start"])
    web_rows.append((event_id, anonymous_id, event_type, email, phone, rand_ts()))

web_schema = "event_id string, anonymous_id string, event_type string, email string, phone string, event_ts timestamp"
web_df = spark.createDataFrame(web_rows, schema=web_schema)
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{DIG}.web_events")
web_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{DIG}.web_events")

# COMMAND ----------
# store_visits (watermark: visit_ts)
visit_rows = []
for i in range(1, N_STORE_VISITS+1):
    p = random.choice(people_local)
    visit_id = f"V{i:08d}"
    store_id = f"S{random.randint(1,250):04d}"
    email = maybe_case_variant(p["email"]) if random.random() < 0.6 else None
    phone = p["phone"] if random.random() < 0.6 else None
    loyalty_id = p["loyalty_id"] if random.random() < 0.8 else None
    visit_rows.append((visit_id, store_id, email, phone, loyalty_id, rand_ts()))

visit_schema = "visit_id string, store_id string, email string, phone string, loyalty_id string, visit_ts timestamp"
visit_df = spark.createDataFrame(visit_rows, schema=visit_schema)
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{STORE}.store_visits")
visit_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{STORE}.store_visits")

# COMMAND ----------
print("✅ Created tables:",
      f"{CATALOG}.{CRM}.customer, {CATALOG}.{SALES}.transactions, {CATALOG}.{LOY}.loyalty_accounts, {CATALOG}.{DIG}.web_events, {CATALOG}.{STORE}.store_visits")
