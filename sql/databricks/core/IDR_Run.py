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
import time
import uuid
import json

# COMMAND ----------
dbutils.widgets.text("RUN_MODE", "INCR")   # INCR | FULL
dbutils.widgets.text("RUN_ID", "")         # optional
dbutils.widgets.text("MAX_ITERS", "30")    # label propagation max iters
dbutils.widgets.text("REPO_ROOT", "")      # optional override if notebook path detection fails
dbutils.widgets.dropdown("DRY_RUN", "false", ["true", "false"])  # Preview mode - no production writes

RUN_MODE = dbutils.widgets.get("RUN_MODE").strip().upper()
DRY_RUN = dbutils.widgets.get("DRY_RUN").strip().lower() == "true"
RUN_ID = dbutils.widgets.get("RUN_ID").strip() or f"{'dry_run' if DRY_RUN else 'run'}_{uuid.uuid4().hex}"
MAX_ITERS = int(dbutils.widgets.get("MAX_ITERS"))
REPO_ROOT = dbutils.widgets.get("REPO_ROOT").strip()

RUN_TS = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")  # UTC
_run_start_ts = time.time()
print({"RUN_MODE": RUN_MODE, "RUN_ID": RUN_ID, "RUN_TS_UTC": RUN_TS, "MAX_ITERS": MAX_ITERS, "DRY_RUN": DRY_RUN})

# COMMAND ----------
# Auto-detect repo root (no user input)
nb_path = None
try:
    nb_path = dbutils.notebook.getContext().notebookPath().get()
except Exception:
    nb_path = None

marker = "/sql/databricks/notebooks/"
if REPO_ROOT:
    repo_root = REPO_ROOT
    sql_common_root = f"{repo_root}/sql/common"
    print({"notebook_path": nb_path, "repo_root": repo_root, "sql_common_root": sql_common_root, "source": "REPO_ROOT widget"})
elif nb_path and marker in nb_path:
    repo_root = "/Workspace" + nb_path.split(marker)[0]
    sql_common_root = f"{repo_root}/sql/common"
    print({"notebook_path": nb_path, "repo_root": repo_root, "sql_common_root": sql_common_root, "source": "auto-detect"})
else:
    raise RuntimeError("Cannot determine repo root. Set the REPO_ROOT widget to your repo path (e.g. /Workspace/Repos/<user>/<repo>).")
sql_common_root = f"{repo_root}/sql/common"

# COMMAND ----------
def q(sql: str):
    return spark.sql(sql)

def collect(sql: str):
    return q(sql).collect()

def run_sql_text(sql_text: str):
    # Execute multi-statement SQL safely (handles files without semicolons by splitting on blank lines).
    statements = []
    buffer = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            # Blank line or comment signals potential statement boundary.
            if buffer and not stripped:
                statements.append("\n".join(buffer))
                buffer = []
            continue
        buffer.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(buffer))
            buffer = []
    if buffer:
        statements.append("\n".join(buffer))

    for stmt in statements:
        if stmt.strip():
            q(stmt)

def run_sql_file(path: str, replacements=None):
    sql_text = open(path).read()
    if replacements:
        for k, v in replacements.items():
            sql_text = sql_text.replace(k, v)
    run_sql_text(sql_text)

def record_metric(name: str, value: float, dimensions: dict = None, metric_type: str = 'gauge'):
    """Record a metric to the metrics_export table."""
    dim_json = json.dumps(dimensions).replace("'", "''") if dimensions else None
    q(f"""
    INSERT INTO idr_out.metrics_export (run_id, metric_name, metric_value, metric_type, dimensions)
    VALUES ('{RUN_ID}', '{name}', {value}, '{metric_type}', {f"'{dim_json}'" if dim_json else 'NULL'})
    """)

def get_config(key: str, default: str = None) -> str:
    """Get configuration value from idr_meta.config."""
    rows = collect(f"SELECT config_value FROM idr_meta.config WHERE config_key = '{key}'")
    return rows[0].config_value if rows else default

# COMMAND ----------
# Observability helpers
_stage_metrics = []
_stage_order = 0

def track_stage(stage_name: str, rows_in: int = None, rows_out: int = None, notes: str = None):
    """Record timing for a processing stage."""
    global _stage_order
    _stage_order += 1
    return {
        "run_id": RUN_ID,
        "stage_name": stage_name,
        "stage_order": _stage_order,
        "started_at": datetime.utcnow(),
        "rows_in": rows_in,
        "rows_out": rows_out,
        "notes": notes
    }

def end_stage(stage: dict, rows_out: int = None):
    """Complete a stage and record metrics."""
    stage["ended_at"] = datetime.utcnow()
    stage["duration_seconds"] = int((stage["ended_at"] - stage["started_at"]).total_seconds())
    if rows_out is not None:
        stage["rows_out"] = rows_out
    _stage_metrics.append(stage)
    print(f"  ⏱️ {stage['stage_name']}: {stage['duration_seconds']}s" + 
          (f", rows_out={rows_out}" if rows_out else ""))

def save_stage_metrics():
    """Persist stage metrics to database."""
    for s in _stage_metrics:
        started_str = s["started_at"].strftime("%Y-%m-%d %H:%M:%S")
        ended_str = s["ended_at"].strftime("%Y-%m-%d %H:%M:%S")
        q(f"""
        INSERT INTO idr_out.stage_metrics (run_id, stage_name, stage_order, started_at, ended_at, duration_seconds, rows_in, rows_out, notes)
        VALUES ('{s["run_id"]}', '{s["stage_name"]}', {s["stage_order"]}, 
                TIMESTAMP'{started_str}', TIMESTAMP'{ended_str}',
                {s["duration_seconds"]}, {s.get("rows_in") or "NULL"}, {s.get("rows_out") or "NULL"}, 
                {f"'{s['notes']}'" if s.get("notes") else "NULL"})
        """)

def init_run_history():
    """Create initial run_history record with RUNNING status."""
    # Ensure observability tables exist
    q("""
    CREATE TABLE IF NOT EXISTS idr_out.run_history (
      run_id STRING, run_mode STRING, status STRING,
      started_at TIMESTAMP, ended_at TIMESTAMP, duration_seconds BIGINT,
      entities_processed BIGINT, edges_created BIGINT, edges_updated BIGINT,
      clusters_impacted BIGINT, lp_iterations INT,
      source_tables_processed INT, groups_skipped INT, values_excluded INT,
      large_clusters INT, warnings STRING,
      watermarks_json STRING, error_message STRING, error_stage STRING,
      created_at TIMESTAMP
    )""")
    
    q("""
    CREATE TABLE IF NOT EXISTS idr_out.stage_metrics (
      run_id STRING, stage_name STRING, stage_order INT,
      started_at TIMESTAMP, ended_at TIMESTAMP, duration_seconds BIGINT,
      rows_in BIGINT, rows_out BIGINT, notes STRING
    )""")
    
    # Ensure skipped_identifier_groups table exists
    q("""
    CREATE TABLE IF NOT EXISTS idr_out.skipped_identifier_groups (
      run_id STRING, identifier_type STRING, identifier_value_norm STRING,
      group_size BIGINT, max_allowed INT, sample_entity_keys STRING,
      reason STRING DEFAULT 'EXCEEDED_MAX_GROUP_SIZE', skipped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    
    # Ensure identifier_exclusion table exists
    q("""
    CREATE TABLE IF NOT EXISTS idr_meta.identifier_exclusion (
      identifier_type STRING, identifier_value_pattern STRING, match_type STRING DEFAULT 'EXACT',
      reason STRING, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, created_by STRING
    )""")
    
    q(f"""
    INSERT INTO idr_out.run_history 
    (run_id, run_mode, status, started_at, source_tables_processed, created_at)
    VALUES ('{RUN_ID}', '{RUN_MODE}', 'RUNNING', TIMESTAMP('{RUN_TS}'), 0, CURRENT_TIMESTAMP)
    """)
def finalize_run_history(status: str, entities: int, edges_created: int, edges_updated: int, 
                         clusters: int, iterations: int, sources: int, watermarks: dict,
                         groups_skipped: int = 0, values_excluded: int = 0,
                         large_clusters: int = 0, warnings: list = None,
                         error_msg: str = None, error_stage: str = None):
    """Update run_history with final metrics."""
    end_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    duration = int(time.time() - _run_start_ts)
    wm_json = json.dumps(watermarks).replace("'", "''")
    err_msg = error_msg.replace("'", "''") if error_msg else None
    warnings_json = json.dumps(warnings).replace("'", "''") if warnings else None
    
    q(f"""
    UPDATE idr_out.run_history SET
      status = '{status}',
      ended_at = TIMESTAMP('{end_ts}'),
      duration_seconds = {duration},
      entities_processed = {entities},
      edges_created = {edges_created},
      edges_updated = {edges_updated},
      clusters_impacted = {clusters},
      lp_iterations = {iterations},
      source_tables_processed = {sources},
      groups_skipped = {groups_skipped},
      values_excluded = {values_excluded},
      large_clusters = {large_clusters},
      warnings = {f"'{warnings_json}'" if warnings_json else 'NULL'},
      watermarks_json = '{wm_json}',
      error_message = {f"'{err_msg}'" if err_msg else 'NULL'},
      error_stage = {f"'{error_stage}'" if error_stage else 'NULL'}
    WHERE run_id = '{RUN_ID}'
    """)
    
    # Write stage metrics
    for s in _stage_metrics:
        q(f"""
        INSERT INTO idr_out.stage_metrics VALUES (
          '{s['run_id']}', '{s['stage_name']}', {s['stage_order']},
          TIMESTAMP('{s['started_at'].strftime('%Y-%m-%d %H:%M:%S')}'),
          TIMESTAMP('{s['ended_at'].strftime('%Y-%m-%d %H:%M:%S')}'),
          {s['duration_seconds']},
          {s.get('rows_in') or 'NULL'}, {s.get('rows_out') or 'NULL'},
          {f"'{s.get('notes')}'" if s.get('notes') else 'NULL'}
        )""")

# COMMAND ----------
# Preflight validation
q("CREATE SCHEMA IF NOT EXISTS idr_meta")
q("CREATE SCHEMA IF NOT EXISTS idr_work")
q("CREATE SCHEMA IF NOT EXISTS idr_out")

def table_exists(fqn: str) -> bool:
    parts = fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"table_fqn must be <catalog>.<schema>.<table>. Got: {fqn}")
    catalog, schema, table = parts
    rows = collect(f"SHOW TABLES IN {catalog}.{schema} LIKE '{table}'")
    return len(rows) > 0

def table_in_schema(schema: str, table: str) -> bool:
    rows = collect(f"SHOW TABLES IN {schema} LIKE '{table}'")
    return len(rows) > 0

required_meta_tables = [
    "source_table",
    "rule",
    "identifier_mapping",
    "source",
    "entity_attribute_mapping",
    "survivorship_rule",
]
missing_meta = [t for t in required_meta_tables if not table_in_schema("idr_meta", t)]
if missing_meta:
    raise RuntimeError("Missing metadata tables in idr_meta.* (run 00_ddl_meta.sql): " + ", ".join(missing_meta))

required_out_tables = [
    "identity_edges_current",
    "identity_resolved_membership_current",
    "identity_clusters_current",
    "golden_profile_current",
    "rule_match_audit_current",
]
missing_out = [t for t in required_out_tables if not table_in_schema("idr_out", t)]
if missing_out:
    raise RuntimeError("Missing output tables in idr_out.* (run 01_ddl_outputs.sql): " + ", ".join(missing_out))

meta_counts = {}
empty_meta = []
for t in required_meta_tables:
    c = q(f"SELECT COUNT(*) AS c FROM idr_meta.{t}").first()["c"]
    meta_counts[t] = c
    if c == 0:
        empty_meta.append(t)
if empty_meta:
    raise RuntimeError("Metadata tables are empty; load metadata first: " + ", ".join(empty_meta))

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
print("ℹ️ Metadata counts", meta_counts)

# Initialize run history tracking
init_run_history()
print("📊 Run tracking started")

# COMMAND ----------
# Builders
def build_where(wm_col, last_wm, lookback_min):
    if RUN_MODE == "FULL":
        return "1=1"
    last = f"TIMESTAMP('{last_wm}')" if last_wm is not None else "TIMESTAMP('1900-01-01 00:00:00')"
    lb = int(lookback_min or 0)
    if lb > 0:
        return f"{wm_col} >= ({last} - INTERVAL {lb} MINUTES)"
    return f"{wm_col} >= {last}"

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
stage_entities = track_stage('Entity Extraction')
q(f"CREATE OR REPLACE TABLE idr_work.entities_delta AS {build_entities_delta_sql(source_rows)}")
q(f"CREATE OR REPLACE TABLE idr_work.identifiers_extracted AS {build_identifiers_sql(source_rows, mapping_rows, delta_only=True)}")
entities_delta_cnt = q("SELECT COUNT(*) FROM idr_work.entities_delta").first()[0]
end_stage(stage_entities, entities_delta_cnt)

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
stage_edges = track_stage('Edge Building')
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
edges_new_cnt = q("SELECT COUNT(*) FROM idr_work.edges_new").first()[0]
end_stage(stage_edges, edges_new_cnt)

# COMMAND ----------
# Subgraph + label propagation
stage_subgraph = track_stage('Subgraph Building')
run_sql_file(f"{sql_common_root}/30_build_impacted_subgraph.sql")
subgraph_cnt = q("SELECT COUNT(*) FROM idr_work.subgraph_nodes").first()[0]
end_stage(stage_subgraph, subgraph_cnt)

stage_lp = track_stage('Label Propagation')

q('''
CREATE OR REPLACE TABLE idr_work.lp_labels AS
SELECT entity_key, entity_key AS label
FROM idr_work.subgraph_nodes
''')

iterations = 0
for i in range(MAX_ITERS):
    iterations = i + 1
    run_sql_file(f"{sql_common_root}/31_label_propagation_step.sql", replacements={"${RUN_ID}": RUN_ID})
    delta = q("SELECT delta_changed FROM idr_work.lp_stats").first()[0]
    print(f"iter={i+1} delta_changed={delta}")
    if delta == 0:
        break
    # Swap tables - use DROP/CREATE for Unity Catalog compatibility
    q("DROP TABLE IF EXISTS idr_work.lp_labels")
    q("ALTER TABLE idr_work.lp_labels_next RENAME TO idr_work.lp_labels")
else:
    raise RuntimeError(f"Label propagation did not converge in {MAX_ITERS} iterations")
end_stage(stage_lp)

# COMMAND ----------
# Membership + clusters
stage_membership = track_stage('Membership Update')
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

entities_delta_cnt = q("SELECT COUNT(*) AS c FROM idr_work.entities_delta").first()["c"]
edges_new_cnt = q("SELECT COUNT(*) AS c FROM idr_work.edges_new").first()["c"]
membership_cnt = q("SELECT COUNT(*) AS c FROM idr_out.identity_resolved_membership_current").first()["c"]
cluster_cnt = q("SELECT COUNT(*) AS c FROM idr_out.identity_clusters_current").first()["c"]
audit_rows = [row.asDict() for row in q("SELECT rule_id, edges_created FROM idr_work.audit_edges_created").collect()]
total_edges_created = sum(r.get('edges_created', 0) for r in audit_rows)

# Count large clusters and skipped groups
large_cluster_cnt = q("SELECT COUNT(*) AS c FROM idr_out.identity_clusters_current WHERE cluster_size >= 1000").first()["c"]
skipped_groups_cnt = q(f"SELECT COUNT(*) AS c FROM idr_out.skipped_identifier_groups WHERE run_id = '{RUN_ID}'").first()["c"] if q("SHOW TABLES IN idr_out LIKE 'skipped_identifier_groups'").count() > 0 else 0
values_excluded_cnt = 0  # Placeholder - would come from exclusion filter counts

# Build watermarks dict for run history
watermarks = {}
for r in collect("SELECT table_id, new_watermark_value FROM idr_work.watermark_updates"):
    watermarks[r['table_id']] = str(r['new_watermark_value'])

# Build warnings list
run_warnings = []
if skipped_groups_cnt > 0:
    run_warnings.append(f"{skipped_groups_cnt} identifier groups skipped (exceeded max_group_size)")
if values_excluded_cnt > 0:
    run_warnings.append(f"{values_excluded_cnt} identifier values excluded (matched exclusion list)")
if large_cluster_cnt > 0:
    run_warnings.append(f"{large_cluster_cnt} large clusters detected (1000+ entities)")

# Set status based on mode
if DRY_RUN:
    run_status = 'DRY_RUN_COMPLETE'
else:
    run_status = 'SUCCESS' if not run_warnings else 'SUCCESS_WITH_WARNINGS'

# Record metrics
duration = int(time.time() - _run_start_ts)
record_metric('idr_run_duration_seconds', duration)
record_metric('idr_entities_processed', entities_delta_cnt)
record_metric('idr_edges_created', total_edges_created, metric_type='counter')
record_metric('idr_clusters_impacted', cluster_cnt)
record_metric('idr_groups_skipped', skipped_groups_cnt, metric_type='counter')
record_metric('idr_large_clusters', large_cluster_cnt)

# Finalize run history
finalize_run_history(
    status=run_status,
    entities=entities_delta_cnt,
    edges_created=total_edges_created,
    edges_updated=edges_new_cnt - total_edges_created if edges_new_cnt > total_edges_created else 0,
    clusters=cluster_cnt,
    iterations=iterations if 'iterations' in locals() else 0,
    sources=len(source_rows),
    watermarks=watermarks,
    groups_skipped=skipped_groups_cnt,
    values_excluded=values_excluded_cnt,
    large_clusters=large_cluster_cnt,
    warnings=run_warnings if run_warnings else None
)

# Enhanced run summary
print("=" * 60)
if DRY_RUN:
    print("DRY RUN SUMMARY (No changes committed)")
else:
    print("IDR RUN SUMMARY")
print("=" * 60)
print(f"Run ID:          {RUN_ID}")
print(f"Mode:            {RUN_MODE}{' (DRY RUN)' if DRY_RUN else ''}")
print(f"Status:          {run_status}")
print()
print("PROCESSING:")
print(f"  Sources:       {len(source_rows)} tables")
print(f"  Entities:      {entities_delta_cnt:,} processed")
print(f"  Edges:         {edges_new_cnt:,} total ({total_edges_created:,} new)")
print(f"  LP Iterations: {iterations if 'iterations' in locals() else 0}")
print(f"  Clusters:      {cluster_cnt:,}")
print(f"  Membership:    {membership_cnt:,}")

if run_warnings:
    print()
    print("DATA QUALITY:")
    for w in run_warnings:
        print(f"  ⚠️ {w}")

if DRY_RUN:
    print()
    print("REVIEW QUERIES:")
    print(f"  → All changes:  SELECT * FROM idr_out.dry_run_results WHERE run_id = '{RUN_ID}'")
    print(f"  → Moved only:   SELECT * FROM idr_out.dry_run_results WHERE run_id = '{RUN_ID}' AND change_type = 'MOVED'")
    print(f"  → Summary:      SELECT * FROM idr_out.dry_run_summary WHERE run_id = '{RUN_ID}'")
    print()
    print("⚠️  THIS WAS A DRY RUN - NO CHANGES COMMITTED")
elif run_warnings:
    print()
    print("ACTIONS NEEDED:")
    print(f"  → Review: SELECT * FROM idr_out.skipped_identifier_groups WHERE run_id = '{RUN_ID}'")

print("=" * 60)

# Save stage metrics to database
save_stage_metrics()

# Print stage timing summary
print()
print("STAGE TIMING:")
for s in _stage_metrics:
    print(f"  ⏱️ {s['stage_name']}: {s['duration_seconds']}s")

print("=" * 60)
if DRY_RUN:
    print("✅ IDR dry run completed", {"RUN_ID": RUN_ID, "RUN_MODE": RUN_MODE, "STATUS": run_status})
else:
    print("✅ IDR run completed", {"RUN_ID": RUN_ID, "RUN_MODE": RUN_MODE, "STATUS": run_status})

