# Databricks notebook source
# MAGIC %md
# MAGIC # IDR_Run (Databricks)
# MAGIC Deterministic identity resolution runner (SQL-first).
# MAGIC
# MAGIC **End-user contract**
# MAGIC - Users configure `idr_meta.*`
# MAGIC - Users run this notebook (or a Databricks Job) with `RUN_MODE` and `MAX_ITERS`

# COMMAND ----------
from datetime import datetime
import uuid

# COMMAND ----------
dbutils.widgets.text("RUN_MODE", "INCR")   # INCR | FULL
dbutils.widgets.text("RUN_ID", "")         # optional
dbutils.widgets.text("MAX_ITERS", "30")    # label propagation max iters

RUN_MODE = dbutils.widgets.get("RUN_MODE").strip().upper()
RUN_ID = dbutils.widgets.get("RUN_ID").strip() or f"run_{uuid.uuid4().hex}"
MAX_ITERS = int(dbutils.widgets.get("MAX_ITERS"))

RUN_TS = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")  # UTC
print({"RUN_MODE": RUN_MODE, "RUN_ID": RUN_ID, "RUN_TS_UTC": RUN_TS, "MAX_ITERS": MAX_ITERS})

# COMMAND ----------
# Auto-detect repo root (no user input)
nb_path = dbutils.notebook.getContext().notebookPath().get()
marker = "/sql/databricks/notebooks/"
if marker not in nb_path:
    raise RuntimeError(f"Unexpected notebook path: {nb_path}. Expected to contain '{marker}'")

repo_root = "/Workspace" + nb_path.split(marker)[0]
sql_common_root = f"{repo_root}/sql/common"
print({"notebook_path": nb_path, "repo_root": repo_root, "sql_common_root": sql_common_root})

# COMMAND ----------
def q(sql: str):
    return spark.sql(sql)

def collect(sql: str):
    return q(sql).collect()

def run_sql_text(sql_text: str):
    # Multi-statement safe execution (repo SQL does not contain semicolons inside string literals)
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    for stmt in statements:
        q(stmt)

def run_sql_file(path: str, replacements=None):
    sql_text = open(path).read()
    if replacements:
        for k, v in replacements.items():
            sql_text = sql_text.replace(k, v)
    run_sql_text(sql_text)

# COMMAND ----------
# Preflight validation
def table_exists(fqn: str) -> bool:
    parts = fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"table_fqn must be <catalog>.<schema>.<table>. Got: {fqn}")
    catalog, schema, table = parts
    rows = collect(f"SHOW TABLES IN {catalog}.{schema} LIKE '{table}'")
    return len(rows) > 0

run_sql_text('''
MERGE INTO idr_meta.run_state tgt
USING (SELECT table_id FROM idr_meta.source_table WHERE is_active = true) src
ON tgt.table_id = src.table_id
WHEN NOT MATCHED THEN INSERT (table_id, last_watermark_value, last_run_id, last_run_ts)
VALUES (src.table_id, TIMESTAMP('1900-01-01 00:00:00'), NULL, NULL)
''')

source_rows = collect('''
SELECT st.table_id, st.table_fqn, st.entity_key_expr, st.watermark_column, st.watermark_lookback_minutes,
       rs.last_watermark_value
FROM idr_meta.source_table st
JOIN idr_meta.run_state rs ON rs.table_id = st.table_id
WHERE st.is_active = true
''')
if not source_rows:
    raise RuntimeError("No active source tables found in idr_meta.source_table (is_active=true).")

mapping_rows = collect("SELECT table_id, identifier_type, identifier_value_expr, is_hashed FROM idr_meta.identifier_mapping")
rule_rows = collect("SELECT identifier_type FROM idr_meta.rule WHERE is_active = true")

active_table_ids = {r['table_id'] for r in source_rows}
active_rule_types = {r['identifier_type'] for r in rule_rows}

missing = []
for r in source_rows:
    if not table_exists(r['table_fqn']):
        missing.append(f"{r['table_id']} -> {r['table_fqn']}")
if missing:
    raise RuntimeError("Source tables not found (create them or fix table_fqn):\n" + "\n".join(missing))

bad_maps = [f"{m['table_id']}:{m['identifier_type']}" for m in mapping_rows if m['table_id'] not in active_table_ids]
if bad_maps:
    raise RuntimeError("identifier_mapping contains table_id(s) not present in active source_table:\n" + "\n".join(bad_maps))

bad_types = sorted({m['identifier_type'] for m in mapping_rows if m['identifier_type'] not in active_rule_types})
if bad_types:
    raise RuntimeError("identifier_mapping contains identifier_type(s) with no active rule in idr_meta.rule:\n" + ", ".join(bad_types))

print("✅ Preflight OK", {"active_tables": len(source_rows), "mappings": len(mapping_rows), "active_rules": len(active_rule_types)})

# COMMAND ----------
# Builders
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
    return "\nUNION ALL\n".join(sels)

def build_identifiers_sql(rows, maps, delta_only=True):
    by_table = {}
    for m in maps:
        by_table.setdefault(m['table_id'], []).append(m)

    sels = []
    for r in rows:
        t = r['table_id']
        if t not in by_table:
            continue
        where = "1=1" if (RUN_MODE == "FULL" or not delta_only) else build_where(r['watermark_column'], r['last_watermark_value'], r['watermark_lookback_minutes'])
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

attr_rows = collect("SELECT table_id, attribute_name, attribute_expr FROM idr_meta.entity_attribute_mapping")
def build_entities_all_sql(rows):
    attr_map = {}
    for a in attr_rows:
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
# Build delta tables
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

q(f"CREATE OR REPLACE TABLE idr_work.identifiers_all_raw AS {build_identifiers_sql(source_rows, mapping_rows, delta_only=False)}")

q('''
CREATE OR REPLACE TABLE idr_work.identifiers_all AS
SELECT
  i.table_id,
  i.entity_key,
  i.identifier_type,
  CASE WHEN r.canonicalize='LOWERCASE' THEN LOWER(i.identifier_value) ELSE i.identifier_value END AS identifier_value_norm,
  i.is_hashed
FROM idr_work.identifiers_all_raw i
JOIN idr_meta.rule r
  ON r.is_active=true AND r.identifier_type=i.identifier_type
WHERE i.identifier_value IS NOT NULL
''')

# COMMAND ----------
# Edges + merge
run_sql_file(f"{sql_common_root}/20_build_edges_incremental.sql", replacements={"${RUN_TS}": RUN_TS})

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
# Subgraph + label propagation
run_sql_file(f"{sql_common_root}/30_build_impacted_subgraph.sql")

q('''
CREATE OR REPLACE TABLE idr_work.lp_labels AS
SELECT entity_key, entity_key AS label
FROM idr_work.subgraph_nodes
''')

for i in range(MAX_ITERS):
    run_sql_file(f"{sql_common_root}/31_label_propagation_step.sql", replacements={"${RUN_ID}": RUN_ID})
    delta = q("SELECT delta_changed FROM idr_work.lp_stats").first()[0]
    print(f"iter={i+1} delta_changed={delta}")
    if delta == 0:
        break
    q("ALTER TABLE idr_work.lp_labels RENAME TO lp_labels_old")
    q("ALTER TABLE idr_work.lp_labels_next RENAME TO lp_labels")
    q("DROP TABLE idr_work.lp_labels_old")
else:
    raise RuntimeError(f"Label propagation did not converge in {MAX_ITERS} iterations")

# COMMAND ----------
# Membership + clusters
run_sql_file(f"{sql_common_root}/40_update_membership_current.sql")

q('''
MERGE INTO idr_out.identity_resolved_membership_current tgt
USING idr_work.membership_updates src
ON tgt.entity_key=src.entity_key
WHEN MATCHED THEN UPDATE SET tgt.resolved_id=src.resolved_id, tgt.updated_ts=src.updated_ts
WHEN NOT MATCHED THEN INSERT (entity_key,resolved_id,updated_ts)
VALUES (src.entity_key,src.resolved_id,src.updated_ts)
''')

run_sql_file(f"{sql_common_root}/41_update_clusters_current.sql")

q('''
MERGE INTO idr_out.identity_clusters_current tgt
USING idr_work.cluster_sizes_updates src
ON tgt.resolved_id=src.resolved_id
WHEN MATCHED THEN UPDATE SET tgt.cluster_size=src.cluster_size, tgt.updated_ts=src.updated_ts
WHEN NOT MATCHED THEN INSERT (resolved_id,cluster_size,updated_ts)
VALUES (src.resolved_id,src.cluster_size,src.updated_ts)
''')

# COMMAND ----------
# Golden profile
q(f"CREATE OR REPLACE TABLE idr_work.entities_all AS {build_entities_all_sql(source_rows)}")
run_sql_file(f"{sql_common_root}/50_build_golden_profile_incremental.sql")

q('''
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
''')

# COMMAND ----------
# Run-state + audit
q('''
CREATE OR REPLACE TABLE idr_work.watermark_updates AS
SELECT table_id, MAX(watermark_value) AS new_watermark_value
FROM idr_work.entities_delta
GROUP BY table_id
''')

q(f"""
MERGE INTO idr_meta.run_state tgt
USING idr_work.watermark_updates src
ON tgt.table_id=src.table_id
WHEN MATCHED THEN UPDATE SET
  tgt.last_watermark_value=src.new_watermark_value,
  tgt.last_run_id='{RUN_ID}',
  tgt.last_run_ts=TIMESTAMP('{RUN_TS}')
""")

run_sql_file(f"{sql_common_root}/70_write_audit.sql", replacements={"${RUN_ID}": RUN_ID, "${RUN_TS}": RUN_TS})

q('''
MERGE INTO idr_out.rule_match_audit_current tgt
USING idr_work.audit_edges_created src
ON tgt.run_id=src.run_id AND tgt.rule_id=src.rule_id
WHEN MATCHED THEN UPDATE SET tgt.edges_created=src.edges_created, tgt.started_at=src.started_at, tgt.ended_at=src.ended_at
WHEN NOT MATCHED THEN INSERT (run_id,rule_id,edges_created,started_at,ended_at)
VALUES (src.run_id,src.rule_id,src.edges_created,src.started_at,src.ended_at)
''')

print("✅ IDR run completed", {"RUN_ID": RUN_ID, "RUN_MODE": RUN_MODE})
