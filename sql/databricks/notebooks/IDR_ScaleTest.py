# Databricks notebook source
# MAGIC %md
# MAGIC # IDR Scale Test & Benchmark
# MAGIC 
# MAGIC Generate millions of rows of realistic test data and benchmark identity resolution performance.
# MAGIC 
# MAGIC **Cluster Sizing Recommendations:**
# MAGIC | Rows per Table | Total Rows | Cluster Size | Workers | Expected Duration |
# MAGIC |----------------|------------|--------------|---------|-------------------|
# MAGIC | 100K | 500K | Small | 2-4 | 2-5 min |
# MAGIC | 500K | 2.5M | Medium | 4-8 | 5-15 min |
# MAGIC | 1M | 5M | Large | 8-16 | 15-30 min |
# MAGIC | 5M | 25M | XL | 16-32 | 30-60 min |
# MAGIC | 10M | 50M | 2XL | 32+ | 1-2 hours |

# COMMAND ----------
from datetime import datetime, timedelta
import time
import uuid
import random
import string
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
dbutils.widgets.dropdown("SCALE", "1M", ["100K", "500K", "1M", "5M", "10M"])
dbutils.widgets.text("CATALOG", "main")
dbutils.widgets.text("SEED", "42")
dbutils.widgets.dropdown("OVERLAP_RATE", "0.6", ["0.3", "0.4", "0.5", "0.6", "0.7"])

SCALE_MAP = {"100K": 100000, "500K": 500000, "1M": 1000000, "5M": 5000000, "10M": 10000000}
SCALE = dbutils.widgets.get("SCALE")
N_ROWS = SCALE_MAP[SCALE]
CATALOG = dbutils.widgets.get("CATALOG").strip()
SEED = int(dbutils.widgets.get("SEED"))
OVERLAP_RATE = float(dbutils.widgets.get("OVERLAP_RATE"))

random.seed(SEED)

print(f"📊 Scale Test Configuration")
print(f"   Scale: {SCALE} ({N_ROWS:,} rows per table)")
print(f"   Total rows: {N_ROWS * 5:,}")
print(f"   Overlap rate: {OVERLAP_RATE} (higher = more identity matches)")

# COMMAND ----------
# Schema names
CRM = "crm"
SALES = "sales"
LOY = "loyalty"
DIG = "digital"
STORE = "store"

# Create schemas
for schema in [CRM, SALES, LOY, DIG, STORE]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Person Pool (Shared Identities)

# COMMAND ----------
# Person pool size determines identity density
# More people = sparser identities, fewer clusters
# Fewer people = denser identities, larger clusters
N_PEOPLE = int(N_ROWS * OVERLAP_RATE)

print(f"🔧 Generating person pool: {N_PEOPLE:,} unique identities")

first_names = ['Anil','Rashmi','Asha','Rohan','Maya','Vikram','Neha','Arjun','Priya','Kiran',
               'Raj','Sita','Kavita','Suresh','Anita','Deepak','Meera','Rahul','Pooja','Amit']
last_names = ['Kulkarni','Sharma','Patel','Reddy','Gupta','Iyer','Nair','Singh','Das','Khan',
              'Verma','Joshi','Rao','Kumar','Mehta','Shah','Pandey','Thakur','Yadav','Mishra']
domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'example.com', 'company.com']

@udf(returnType=StringType())
def gen_email(person_id, first_name, last_name):
    domain = domains[hash(str(person_id)) % len(domains)]
    return f"{first_name.lower()}.{last_name.lower()}{person_id}@{domain}"

@udf(returnType=StringType())
def gen_phone(person_id):
    random.seed(person_id)
    return '9' + ''.join([str(random.randint(0, 9)) for _ in range(9)])

@udf(returnType=StringType())
def gen_loyalty(person_id):
    return f"L{100000 + person_id}"

# Generate person pool using Spark
person_df = (
    spark.range(1, N_PEOPLE + 1)
    .withColumnRenamed("id", "person_id")
    .withColumn("first_name", F.element_at(F.array(*[F.lit(n) for n in first_names]), (F.col("person_id") % len(first_names) + 1).cast("int")))
    .withColumn("last_name", F.element_at(F.array(*[F.lit(n) for n in last_names]), ((F.col("person_id") / len(first_names)).cast("int") % len(last_names) + 1).cast("int")))
    .withColumn("email", gen_email("person_id", "first_name", "last_name"))
    .withColumn("phone", gen_phone("person_id"))
    .withColumn("loyalty_id", gen_loyalty("person_id"))
    .cache()
)

person_df.count()  # Materialize
print(f"✅ Person pool created: {person_df.count():,} identities")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Source Tables

# COMMAND ----------
start_date = datetime.utcnow() - timedelta(days=45)

@udf(returnType=StringType())
def case_variant(email):
    if email is None:
        return None
    h = hash(email) % 100
    if h < 35:
        return email.upper() if h < 17 else email.title()
    return email

def generate_table(table_name, schema, n_rows, person_df, columns_config):
    """Generate a table with configurable columns and null rates."""
    print(f"📊 Generating {table_name}...")
    start = time.time()
    
    # Sample from person pool with replacement
    df = (
        spark.range(1, n_rows + 1)
        .withColumnRenamed("id", "row_id")
        .withColumn("person_id", (F.rand(SEED) * N_PEOPLE).cast("long") + 1)
        .join(F.broadcast(person_df), "person_id")
    )
    
    # Add table-specific columns
    for col_name, col_config in columns_config.items():
        if col_config["type"] == "id":
            df = df.withColumn(col_name, F.concat(F.lit(col_config["prefix"]), F.lpad(F.col("row_id").cast("string"), col_config["width"], "0")))
        elif col_config["type"] == "timestamp":
            df = df.withColumn(col_name, F.from_unixtime(F.unix_timestamp(F.lit(start_date)) + (F.rand(SEED + hash(col_name)) * 45 * 86400).cast("long")))
        elif col_config["type"] == "email":
            df = df.withColumn(col_name, F.when(F.rand(SEED) < col_config.get("null_rate", 0), F.lit(None)).otherwise(case_variant(F.col("email"))))
        elif col_config["type"] == "phone":
            df = df.withColumn(col_name, F.when(F.rand(SEED) < col_config.get("null_rate", 0), F.lit(None)).otherwise(F.col("phone")))
        elif col_config["type"] == "loyalty_id":
            df = df.withColumn(col_name, F.when(F.rand(SEED) < col_config.get("null_rate", 0), F.lit(None)).otherwise(F.col("loyalty_id")))
        elif col_config["type"] == "name":
            df = df.withColumn(col_name, F.col(col_config["source"]))
        elif col_config["type"] == "random_choice":
            df = df.withColumn(col_name, F.element_at(F.array(*[F.lit(v) for v in col_config["choices"]]), (F.rand(SEED) * len(col_config["choices"])).cast("int") + 1))
        elif col_config["type"] == "random_int":
            df = df.withColumn(col_name, (F.rand(SEED) * col_config["max"]).cast("int"))
    
    # Select only output columns
    output_cols = list(columns_config.keys())
    df = df.select(output_cols)
    
    # Write table
    table_fqn = f"{CATALOG}.{schema}.{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    df.write.mode("overwrite").saveAsTable(table_fqn)
    
    duration = time.time() - start
    count = spark.table(table_fqn).count()
    print(f"   ✅ {table_fqn}: {count:,} rows in {duration:.1f}s")
    return count

# COMMAND ----------
# Customer table
generate_table("customer", CRM, N_ROWS, person_df, {
    "customer_id": {"type": "id", "prefix": "C", "width": 8},
    "first_name": {"type": "name", "source": "first_name"},
    "last_name": {"type": "name", "source": "last_name"},
    "email": {"type": "email", "null_rate": 0.05},
    "phone": {"type": "phone", "null_rate": 0.1},
    "loyalty_id": {"type": "loyalty_id", "null_rate": 0.15},
    "rec_create_dt": {"type": "timestamp"},
    "rec_update_dt": {"type": "timestamp"},
})

# Transactions table
generate_table("transactions", SALES, N_ROWS, person_df, {
    "order_id": {"type": "id", "prefix": "O", "width": 10},
    "line_id": {"type": "random_choice", "choices": ["1", "2", "3"]},
    "customer_email": {"type": "email", "null_rate": 0.15},
    "customer_phone": {"type": "phone", "null_rate": 0.2},
    "load_ts": {"type": "timestamp"},
})

# Loyalty accounts table
generate_table("loyalty_accounts", LOY, N_ROWS, person_df, {
    "loyalty_account_id": {"type": "id", "prefix": "LA", "width": 10},
    "loyalty_id": {"type": "loyalty_id", "null_rate": 0.0},
    "email": {"type": "email", "null_rate": 0.2},
    "phone": {"type": "phone", "null_rate": 0.2},
    "tier": {"type": "random_choice", "choices": ["BRONZE", "SILVER", "GOLD", "PLATINUM"]},
    "points": {"type": "random_int", "max": 50000},
    "updated_at": {"type": "timestamp"},
})

# Web events table
generate_table("web_events", DIG, N_ROWS, person_df, {
    "event_id": {"type": "id", "prefix": "E", "width": 12},
    "anonymous_id": {"type": "id", "prefix": "anon_", "width": 6},
    "event_type": {"type": "random_choice", "choices": ["page_view", "product_view", "add_to_cart", "checkout_start", "purchase"]},
    "email": {"type": "email", "null_rate": 0.35},
    "phone": {"type": "phone", "null_rate": 0.7},
    "event_ts": {"type": "timestamp"},
})

# Store visits table
generate_table("store_visits", STORE, N_ROWS, person_df, {
    "visit_id": {"type": "id", "prefix": "V", "width": 12},
    "store_id": {"type": "id", "prefix": "S", "width": 4},
    "email": {"type": "email", "null_rate": 0.4},
    "phone": {"type": "phone", "null_rate": 0.4},
    "loyalty_id": {"type": "loyalty_id", "null_rate": 0.25},
    "visit_ts": {"type": "timestamp"},
})

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run Benchmark

# COMMAND ----------
print("🚀 Running IDR benchmark...")
benchmark_start = time.time()

# Run IDR
result = dbutils.notebook.run(
    "./IDR_Run",
    timeout_seconds=7200,  # 2 hours max
    arguments={"RUN_MODE": "FULL", "MAX_ITERS": "50"}
)

benchmark_duration = time.time() - benchmark_start
print(f"✅ Benchmark complete: {benchmark_duration:.1f}s ({benchmark_duration/60:.1f} min)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Benchmark Results

# COMMAND ----------
# Get metrics
run_history = spark.sql("""
    SELECT * FROM idr_out.run_history
    ORDER BY started_at DESC
    LIMIT 1
""").first()

stage_metrics = spark.sql("""
    SELECT stage_name, duration_seconds, rows_out
    FROM idr_out.stage_metrics
    WHERE run_id = (SELECT MAX(run_id) FROM idr_out.run_history)
    ORDER BY stage_order
""").collect()

edges_count = spark.sql("SELECT COUNT(*) FROM idr_out.identity_edges_current").first()[0]
clusters_count = spark.sql("SELECT COUNT(*) FROM idr_out.identity_clusters_current").first()[0]
membership_count = spark.sql("SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current").first()[0]

# Print results
print("=" * 70)
print("BENCHMARK RESULTS")
print("=" * 70)
print(f"Scale: {SCALE} ({N_ROWS * 5:,} total rows)")
print(f"Run ID: {run_history['run_id']}")
print(f"Status: {run_history['status']}")
print(f"Duration: {run_history['duration_seconds']}s ({run_history['duration_seconds']/60:.1f} min)")
print(f"LP Iterations: {run_history['lp_iterations']}")
print()
print("Outputs:")
print(f"  Entities processed: {run_history['entities_processed']:,}")
print(f"  Edges created: {edges_count:,}")
print(f"  Clusters: {clusters_count:,}")
print(f"  Membership: {membership_count:,}")
print()
print("Stage Timing:")
for stage in stage_metrics:
    print(f"  {stage['stage_name']}: {stage['duration_seconds']}s")
print("=" * 70)

# COMMAND ----------
# Cluster size distribution
display(spark.sql("""
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
  SUM(cluster_size) AS total_entities,
  AVG(cluster_size) AS avg_size
FROM idr_out.identity_clusters_current
GROUP BY 1
ORDER BY 1
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC 
# MAGIC Uncomment to remove test data:

# COMMAND ----------
# # Uncomment to cleanup
# for schema in [CRM, SALES, LOY, DIG, STORE]:
#     spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{schema} CASCADE")
