# Databricks notebook source
# MAGIC %md
# MAGIC # IDR_Run (Databricks)
# MAGIC Deterministic identity resolution runner (SQL-first).
# MAGIC
# MAGIC **What it does**
# MAGIC - Reads config from `idr_meta.*`
# MAGIC - Builds `idr_work.entities_delta`, `idr_work.identifiers_delta`
# MAGIC - Builds incremental edges and merges into `idr_out.identity_edges_current`
# MAGIC - Computes connected components on the impacted subgraph via iterative label propagation
# MAGIC - Updates `membership_current`, `clusters_current`, `golden_profile_current`, and `run_state`
# MAGIC
# MAGIC **Prereqs**
# MAGIC - Run `sql/common/00_ddl_meta.sql` and `sql/common/01_ddl_outputs.sql`
# MAGIC - Load metadata samples into `idr_meta.*`
# MAGIC - Create source tables referenced by `idr_meta.source_table.table_fqn`

# COMMAND ----------
from datetime import datetime
import uuid

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------
dbutils.widgets.text("RUN_MODE", "INCR")   # INCR | FULL
dbutils.widgets.text("RUN_ID", "")         # optional
dbutils.widgets.text("MAX_ITERS", "30")    # label propagation max iters
dbutils.widgets.text("REPO_PATH", "")      # Workspace repo path containing identity-resolution folder

RUN_MODE = dbutils.widgets.get("RUN_MODE").strip().upper()
RUN_ID = dbutils.widgets.get("RUN_ID").strip() or f"run_{uuid.uuid4().hex}"
MAX_ITERS = int(dbutils.widgets.get("MAX_ITERS"))
REPO_PATH = dbutils.widgets.get("REPO_PATH").strip()

if not REPO_PATH:
    raise ValueError("Set REPO_PATH widget to your Workspace repo path that contains /identity-resolution/sql/...")

RUN_TS = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")  # UTC
print({"RUN_MODE": RUN_MODE, "RUN_ID": RUN_ID, "RUN_TS_UTC": RUN_TS, "MAX_ITERS": MAX_ITERS, "REPO_PATH": REPO_PATH})

# COMMAND ----------
def q(sql: str):
    return spark.sql(sql)

def collect(sql: str):
    return q(sql).collect()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load metadata + ensure run_state rows exist

# COMMAND ----------
q("""
MERGE INTO idr_meta.run_state tgt
USING (SELECT table_id FROM idr_meta.source_table WHERE is_active = true) src
ON tgt.table_id = src.table_id
WHEN NOT MATCHED THEN INSERT (table_id, last_watermark_value, last_run_id, last_run_ts)
VALUES (src.table_id, TIMESTAMP('1900-01-01 00:00:00'), NULL, NULL)
""")

source_rows = collect("""
SELECT st.table_id, st.table_fqn, st.entity_key_expr, st.watermark_column, st.watermark_lookback_minutes,
       rs.last_watermark_value
FROM idr_meta.source_table st
JOIN idr_meta.run_state rs ON rs.table_id = st.table_id
WHERE st.is_active = true
""")

mapping_rows = collect("SELECT table_id, identifier_type, identifier_value_expr, is_hashed FROM idr_meta.identifier_mapping")
attr_rows = collect("SELECT table_id, attribute_name, attribute_expr FROM idr_meta.entity_attribute_mapping")

print(f"Active tables={len(source_rows)} mappings={len(mapping_rows)} attr_mappings={len(attr_rows)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## SQL builders from metadata

# COMMAND ----------
def build_where(wm_col, last_wm, lookback_min):
    if RUN_MODE == "FULL":
        return "1=1"
    last = f"TIMESTAMP('{last_wm}')" if last_wm is not None else "TIMESTAMP('1900-01-01 00:00:00')"
    lb = int(lookback_min or 0)
    if lb > 0:
        return f"{wm_col} >= ({last} - INTERVAL {lb} MINUTES)"
    return f"{wm_col} > {last}"

def build_entities_delta_sql(rows):
    sels = []
    for r in rows:
        where = build_where(r['watermark_column'], r['last_watermark_value'], r['watermark_lookback_minutes'])
        sels.append(f"""
SELECT
  '{RUN_ID}' AS run_id,
  '{r['table_id']}' AS table_id,
  CONCAT('{r['table_id']}', ':', CAST(({r['entity_key_expr']}) AS STRING)) AS entity_key,
  CAST({r['watermark_column']} AS TIMESTAMP) AS watermark_value
FROM {r['table_fqn']}
WHERE {where}
""")
    return "\nUNION ALL\n".join(sels) if sels else "SELECT NULL as run_id, NULL as table_id, NULL as entity_key, NULL as watermark_value WHERE 1=0"

def build_identifiers_sql(rows, maps, delta_only=True):
    by_table = {}
    for m in maps:
        by_table.setdefault(m['table_id'], []).append(m)

    sels = []
    for r in rows:
        t = r['table_id']
        if t not in by_table:
            continue
        where = "1=1" if (RUN_MODE=="FULL" or not delta_only) else build_where(r['watermark_column'], r['last_watermark_value'], r['watermark_lookback_minutes'])
        for m in by_table[t]:
            is_h = "true" if m['is_hashed'] else "false"
            sels.append(f"""
SELECT
  '{t}' AS table_id,
  CONCAT('{t}', ':', CAST(({r['entity_key_expr']}) AS STRING)) AS entity_key,
  '{m['identifier_type']}' AS identifier_type,
  CAST(({m['identifier_value_expr']}) AS STRING) AS identifier_value,
  {is_h} AS is_hashed
FROM {r['table_fqn']}
WHERE {where}
""")
    return "\nUNION ALL\n".join(sels) if sels else "SELECT NULL as table_id, NULL as entity_key, NULL as identifier_type, NULL as identifier_value, false as is_hashed WHERE 1=0"

def build_entities_all_sql(rows, attrs):
    attr_map = {}
    for a in attrs:
        attr_map.setdefault(a['table_id'], {})[a['attribute_name']] = a['attribute_expr']

    required = ['record_updated_at','first_name','last_name','email_raw','phone_raw']
    sels = []
    for r in rows:
        amap = attr_map.get(r['table_id'], {})
        expr = {k: amap.get(k, 'NULL') for k in required}
        sels.append(f"""
SELECT
  '{r['table_id']}' AS table_id,
  CONCAT('{r['table_id']}', ':', CAST(({r['entity_key_expr']}) AS STRING)) AS entity_key,
  CAST(({expr['record_updated_at']}) AS TIMESTAMP) AS record_updated_at,
  CAST(({expr['first_name']}) AS STRING) AS first_name,
  CAST(({expr['last_name']}) AS STRING) AS last_name,
  CAST(({expr['email_raw']}) AS STRING) AS email_raw,
  CAST(({expr['phone_raw']}) AS STRING) AS phone_raw
FROM {r['table_fqn']}
""")
    return "\nUNION ALL\n".join(sels) if sels else "SELECT NULL as table_id, NULL as entity_key, NULL as record_updated_at, NULL as first_name, NULL as last_name, NULL as email_raw, NULL as phone_raw WHERE 1=0"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Build entities_delta + identifiers_delta + identifiers_all

# COMMAND ----------
q(f"CREATE OR REPLACE TABLE idr_work.entities_delta AS {build_entities_delta_sql(source_rows)}")

q(f"CREATE OR REPLACE TABLE idr_work.identifiers_extracted AS {build_identifiers_sql(source_rows, mapping_rows, delta_only=True)}")

q(f"""
CREATE OR REPLACE TABLE idr_work.identifiers_delta AS
SELECT
  '{RUN_ID}' AS run_id,
  e.table_id,
  e.entity_key,
  r.identifier_type,
  CASE WHEN r.canonicalize='LOWERCASE' THEN LOWER(i.identifier_value) ELSE i.identifier_value END AS identifier_value_norm,
  i.is_hashed,
  e.watermark_value
FROM idr_work.entities_delta e
JOIN idr_work.identifiers_extracted i
  ON i.table_id=e.table_id AND i.entity_key=e.entity_key
JOIN idr_meta.rule r
  ON r.is_active=true AND r.identifier_type=i.identifier_type
WHERE (r.require_non_null=false OR i.identifier_value IS NOT NULL)
""")

# full identifiers for edge expansion
q(f"CREATE OR REPLACE TABLE idr_work.identifiers_all_raw AS {build_identifiers_sql(source_rows, mapping_rows, delta_only=False)}")

q("""
CREATE OR REPLACE TABLE idr_work.identifiers_all AS
SELECT
  table_id,
  entity_key,
  identifier_type,
  CASE WHEN r.canonicalize='LOWERCASE' THEN LOWER(identifier_value) ELSE identifier_value END AS identifier_value_norm,
  is_hashed
FROM idr_work.identifiers_all_raw
JOIN idr_meta.rule r
  ON r.is_active=true AND r.identifier_type=identifier_type
WHERE identifier_value IS NOT NULL
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Build edges incrementally and MERGE to current

# COMMAND ----------
edges_sql = open(f"{REPO_PATH}/identity-resolution/sql/common/20_build_edges_incremental.sql").read().replace("${RUN_TS}", RUN_TS)
q(edges_sql)

q(f"""
MERGE INTO idr_out.identity_edges_current tgt
USING idr_work.edges_new src
ON tgt.rule_id=src.rule_id
AND tgt.left_entity_key=src.left_entity_key
AND tgt.right_entity_key=src.right_entity_key
AND tgt.identifier_type=src.identifier_type
AND tgt.identifier_value_norm=src.identifier_value_norm
WHEN MATCHED THEN UPDATE SET tgt.last_seen_ts=src.last_seen_ts
WHEN NOT MATCHED THEN INSERT (rule_id,left_entity_key,right_entity_key,identifier_type,identifier_value_norm,first_seen_ts,last_seen_ts)
VALUES (src.rule_id,src.left_entity_key,src.right_entity_key,src.identifier_type,src.identifier_value_norm,src.first_seen_ts,src.last_seen_ts)
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Build impacted subgraph + label propagation loop

# COMMAND ----------
q(open(f"{REPO_PATH}/identity-resolution/sql/common/30_build_impacted_subgraph.sql").read())

q("""
CREATE OR REPLACE TABLE idr_work.lp_labels AS
SELECT entity_key, entity_key AS label
FROM idr_work.subgraph_nodes
""")

for i in range(MAX_ITERS):
    step = open(f"{REPO_PATH}/identity-resolution/sql/common/31_label_propagation_step.sql").read()
    step = step.replace("${RUN_ID}", RUN_ID)
    q(step)
    delta = q("SELECT delta_changed FROM idr_work.lp_stats").first()[0]
    print(f"iter={i+1} delta_changed={delta}")
    if delta == 0:
        break
    q("ALTER TABLE idr_work.lp_labels RENAME TO lp_labels_old")
    q("ALTER TABLE idr_work.lp_labels_next RENAME TO lp_labels")
    q("DROP TABLE idr_work.lp_labels_old")
else:
    raise RuntimeError(f"Did not converge in {MAX_ITERS} iterations")


# COMMAND ----------
# MAGIC %md
# MAGIC ## Update membership + clusters

# COMMAND ----------
q(open(f"{REPO_PATH}/identity-resolution/sql/common/40_update_membership_current.sql").read())

q("""
MERGE INTO idr_out.identity_resolved_membership_current tgt
USING idr_work.membership_updates src
ON tgt.entity_key=src.entity_key
WHEN MATCHED THEN UPDATE SET tgt.resolved_id=src.resolved_id, tgt.updated_ts=src.updated_ts
WHEN NOT MATCHED THEN INSERT (entity_key,resolved_id,updated_ts)
VALUES (src.entity_key,src.resolved_id,src.updated_ts)
""")

q(open(f"{REPO_PATH}/identity-resolution/sql/common/41_update_clusters_current.sql").read())

q("""
MERGE INTO idr_out.identity_clusters_current tgt
USING idr_work.cluster_sizes_updates src
ON tgt.resolved_id=src.resolved_id
WHEN MATCHED THEN UPDATE SET tgt.cluster_size=src.cluster_size, tgt.updated_ts=src.updated_ts
WHEN NOT MATCHED THEN INSERT (resolved_id,cluster_size,updated_ts)
VALUES (src.resolved_id,src.cluster_size,src.updated_ts)
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Golden profile (optional mappings)

# COMMAND ----------
q(f"CREATE OR REPLACE TABLE idr_work.entities_all AS {build_entities_all_sql(source_rows, attr_rows)}")

q(open(f"{REPO_PATH}/identity-resolution/sql/common/50_build_golden_profile_incremental.sql").read())

q("""
MERGE INTO idr_out.golden_profile_current tgt
USING idr_work.golden_updates src
ON tgt.resolved_id=src.resolved_id
WHEN MATCHED THEN UPDATE SET
  tgt.email_primary=src.email_primary,
  tgt.phone_primary=src.phone_primary,
  tgt.first_name=src.first_name,
  tgt.last_name=src.last_name,
  tgt.updated_ts=src.updated_ts
WHEN NOT MATCHED THEN INSERT (resolved_id,email_primary,phone_primary,first_name,last_name,updated_ts)
VALUES (src.resolved_id,src.email_primary,src.phone_primary,src.first_name,src.last_name,src.updated_ts)
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Update run_state + audit

# COMMAND ----------
q("""
CREATE OR REPLACE TABLE idr_work.watermark_updates AS
SELECT table_id, MAX(watermark_value) AS new_watermark_value
FROM idr_work.entities_delta
GROUP BY table_id
""")

q(f"""
MERGE INTO idr_meta.run_state tgt
USING idr_work.watermark_updates src
ON tgt.table_id=src.table_id
WHEN MATCHED THEN UPDATE SET
  tgt.last_watermark_value=src.new_watermark_value,
  tgt.last_run_id='{RUN_ID}',
  tgt.last_run_ts=TIMESTAMP('{RUN_TS}')
""")

audit = open(f"{REPO_PATH}/identity-resolution/sql/common/70_write_audit.sql").read().replace("${RUN_ID}", RUN_ID).replace("${RUN_TS}", RUN_TS)
q(audit)

q("""
MERGE INTO idr_out.rule_match_audit_current tgt
USING idr_work.audit_edges_created src
ON tgt.run_id=src.run_id AND tgt.rule_id=src.rule_id
WHEN MATCHED THEN UPDATE SET tgt.edges_created=src.edges_created, tgt.started_at=src.started_at, tgt.ended_at=src.ended_at
WHEN NOT MATCHED THEN INSERT (run_id,rule_id,edges_created,started_at,ended_at)
VALUES (src.run_id,src.rule_id,src.edges_created,src.started_at,src.ended_at)
""")

print("✅ Done", {"RUN_ID": RUN_ID, "RUN_MODE": RUN_MODE})
