#!/usr/bin/env python3
"""
DuckDB IDR Runner
Deterministic identity resolution for DuckDB (local/testing).

Usage:
    python idr_run.py --db=idr.duckdb --run-mode=FULL --max-iters=30
    
Prerequisites:
    pip install duckdb

Benefits:
    - No cloud setup required
    - Perfect for CI/CD testing
    - Fast local development
    - Portable .duckdb file
"""

import argparse
import json
import time
import uuid
from datetime import datetime

try:
    import duckdb
except ImportError:
    print("Please install duckdb: pip install duckdb")
    exit(1)

# ============================================
# CONFIGURATION
# ============================================
parser = argparse.ArgumentParser(description='DuckDB Identity Resolution Runner')
parser.add_argument('--db', default='idr.duckdb', help='DuckDB database file')
parser.add_argument('--run-mode', default='INCR', choices=['INCR', 'FULL'], help='Run mode')
parser.add_argument('--max-iters', type=int, default=30, help='Max label propagation iterations')
args = parser.parse_args()

DB_PATH = args.db
RUN_MODE = args.run_mode
MAX_ITERS = args.max_iters
RUN_ID = f"run_{uuid.uuid4().hex[:12]}"
RUN_TS = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
run_start = time.time()

con = duckdb.connect(DB_PATH)

def q(sql: str):
    """Execute SQL."""
    return con.execute(sql)

def collect(sql: str) -> list:
    """Execute SQL and return list of dicts."""
    result = con.execute(sql).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in result]

def collect_one(sql: str):
    """Execute SQL and return first value."""
    result = con.execute(sql).fetchone()
    return result[0] if result else None

print(f"🦆 Starting DuckDB IDR run: {RUN_ID}")
print(f"   Database: {DB_PATH}")
print(f"   Mode: {RUN_MODE}, Max iterations: {MAX_ITERS}")

# ============================================
# PREFLIGHT
# ============================================
# Check schemas exist
try:
    q("SELECT 1 FROM idr_meta.source_table LIMIT 0")
except:
    raise RuntimeError("Schemas not created. Run 00_ddl_all.sql first.")

# Init run state for new tables
q("""
INSERT INTO idr_meta.run_state (table_id, last_watermark_value, last_run_id, last_run_ts)
SELECT table_id, TIMESTAMP '1900-01-01 00:00:00', NULL, NULL
FROM idr_meta.source_table st
WHERE st.is_active = TRUE
  AND NOT EXISTS (SELECT 1 FROM idr_meta.run_state rs WHERE rs.table_id = st.table_id)
""")

source_rows = collect("""
    SELECT st.table_id, st.table_fqn, st.entity_key_expr, st.watermark_column, 
           st.watermark_lookback_minutes,
           COALESCE(rs.last_watermark_value, TIMESTAMP '1900-01-01') as last_watermark_value
    FROM idr_meta.source_table st
    LEFT JOIN idr_meta.run_state rs ON rs.table_id = st.table_id
    WHERE st.is_active = TRUE
""")

if not source_rows:
    raise RuntimeError("No active source tables found")

mapping_rows = collect("SELECT table_id, identifier_type, identifier_value_expr, is_hashed FROM idr_meta.identifier_mapping")

print(f"✅ Preflight OK: {len(source_rows)} sources, {len(mapping_rows)} mappings")

# Insert initial run history
q(f"""
INSERT INTO idr_out.run_history 
(run_id, run_mode, status, started_at, source_tables_processed, created_at)
VALUES ('{RUN_ID}', '{RUN_MODE}', 'RUNNING', TIMESTAMP '{RUN_TS}', {len(source_rows)}, CURRENT_TIMESTAMP)
""")

# ============================================
# BUILD ENTITIES DELTA
# ============================================
print("📊 Building entities delta...")

def build_where(wm_col, last_wm, lookback_min):
    if RUN_MODE == "FULL":
        return "1=1"
    last = f"TIMESTAMP '{last_wm}'" if last_wm else "TIMESTAMP '1900-01-01'"
    lb = lookback_min or 0
    if lb > 0:
        return f"{wm_col} >= {last} - INTERVAL {lb} MINUTE"
    return f"{wm_col} >= {last}"

entities_parts = []
for r in source_rows:
    where = build_where(r['watermark_column'], r['last_watermark_value'], r.get('watermark_lookback_minutes'))
    entities_parts.append(f"""
        SELECT '{RUN_ID}' AS run_id, '{r['table_id']}' AS table_id,
               '{r['table_id']}' || ':' || CAST(({r['entity_key_expr']}) AS VARCHAR) AS entity_key,
               CAST({r['watermark_column']} AS TIMESTAMP) AS watermark_value
        FROM {r['table_fqn']}
        WHERE {where}
    """)

q(f"CREATE OR REPLACE TABLE idr_work.entities_delta AS {' UNION ALL '.join(entities_parts)}")

# ============================================
# BUILD IDENTIFIERS
# ============================================
print("🔍 Extracting identifiers...")

by_table = {}
for m in mapping_rows:
    by_table.setdefault(m['table_id'], []).append(m)

identifiers_parts = []
for r in source_rows:
    for m in by_table.get(r['table_id'], []):
        identifiers_parts.append(f"""
            SELECT '{r['table_id']}' AS table_id,
                   '{r['table_id']}' || ':' || CAST(({r['entity_key_expr']}) AS VARCHAR) AS entity_key,
                   '{m['identifier_type']}' AS identifier_type,
                   CAST(({m['identifier_value_expr']}) AS VARCHAR) AS identifier_value,
                   {str(m['is_hashed']).lower()} AS is_hashed
            FROM {r['table_fqn']}
        """)

if identifiers_parts:
    q(f"CREATE OR REPLACE TABLE idr_work.identifiers_all_raw AS {' UNION ALL '.join(identifiers_parts)}")

q("""
CREATE OR REPLACE TABLE idr_work.identifiers_all AS
SELECT i.table_id, i.entity_key, i.identifier_type,
       CASE WHEN r.canonicalize='LOWERCASE' THEN LOWER(i.identifier_value) ELSE i.identifier_value END AS identifier_value_norm,
       i.is_hashed
FROM idr_work.identifiers_all_raw i
JOIN idr_meta.rule r ON r.is_active=TRUE AND r.identifier_type=i.identifier_type
WHERE i.identifier_value IS NOT NULL
""")

# ============================================
# BUILD EDGES (Anchor-based)
# ============================================
print("🔗 Building edges...")

q("""
CREATE OR REPLACE TABLE idr_work.delta_identifier_values AS
SELECT DISTINCT i.identifier_type, i.identifier_value_norm
FROM idr_work.entities_delta e
JOIN idr_work.identifiers_all i ON i.entity_key = e.entity_key
WHERE i.identifier_value_norm IS NOT NULL
""")

q("""
CREATE OR REPLACE TABLE idr_work.members_for_delta_values AS
SELECT a.table_id, a.entity_key, a.identifier_type, a.identifier_value_norm
FROM idr_work.identifiers_all a
JOIN idr_work.delta_identifier_values d
  ON a.identifier_type = d.identifier_type AND a.identifier_value_norm = d.identifier_value_norm
""")

q("""
CREATE OR REPLACE TABLE idr_work.group_anchor AS
SELECT identifier_type, identifier_value_norm, MIN(entity_key) AS anchor_entity_key
FROM idr_work.members_for_delta_values
GROUP BY identifier_type, identifier_value_norm
""")

q(f"""
CREATE OR REPLACE TABLE idr_work.edges_new AS
SELECT r.rule_id, ga.anchor_entity_key AS left_entity_key, m.entity_key AS right_entity_key,
       ga.identifier_type, ga.identifier_value_norm,
       TIMESTAMP '{RUN_TS}' AS first_seen_ts, TIMESTAMP '{RUN_TS}' AS last_seen_ts
FROM idr_work.group_anchor ga
JOIN idr_work.members_for_delta_values m
  ON m.identifier_type = ga.identifier_type AND m.identifier_value_norm = ga.identifier_value_norm
JOIN idr_meta.rule r ON r.is_active = TRUE AND r.identifier_type = ga.identifier_type
WHERE m.entity_key <> ga.anchor_entity_key
""")

# DuckDB supports INSERT OR REPLACE for upserts
q("""
INSERT INTO idr_out.identity_edges_current
SELECT * FROM idr_work.edges_new src
WHERE NOT EXISTS (
    SELECT 1 FROM idr_out.identity_edges_current tgt
    WHERE tgt.rule_id = src.rule_id
      AND tgt.left_entity_key = src.left_entity_key
      AND tgt.right_entity_key = src.right_entity_key
      AND tgt.identifier_type = src.identifier_type
      AND tgt.identifier_value_norm = src.identifier_value_norm
)
""")

q(f"""
UPDATE idr_out.identity_edges_current AS tgt
SET last_seen_ts = TIMESTAMP '{RUN_TS}'
WHERE EXISTS (
    SELECT 1 FROM idr_work.edges_new src
    WHERE tgt.rule_id = src.rule_id
      AND tgt.left_entity_key = src.left_entity_key
      AND tgt.right_entity_key = src.right_entity_key
      AND tgt.identifier_type = src.identifier_type
      AND tgt.identifier_value_norm = src.identifier_value_norm
)
""")

# ============================================
# BUILD IMPACTED SUBGRAPH
# ============================================
print("📈 Building impacted subgraph...")

q("""
CREATE OR REPLACE TABLE idr_work.impacted_nodes AS
SELECT DISTINCT left_entity_key AS entity_key FROM idr_work.edges_new
UNION
SELECT DISTINCT right_entity_key AS entity_key FROM idr_work.edges_new
""")

q("""
CREATE OR REPLACE TABLE idr_work.subgraph_nodes AS
SELECT DISTINCT entity_key FROM idr_work.impacted_nodes
UNION
SELECT DISTINCT e.left_entity_key AS entity_key
FROM idr_out.identity_edges_current e
JOIN idr_work.impacted_nodes n ON n.entity_key = e.right_entity_key
UNION
SELECT DISTINCT e.right_entity_key AS entity_key
FROM idr_out.identity_edges_current e
JOIN idr_work.impacted_nodes n ON n.entity_key = e.left_entity_key
""")

q("""
CREATE OR REPLACE TABLE idr_work.subgraph_edges AS
SELECT e.left_entity_key, e.right_entity_key
FROM idr_out.identity_edges_current e
WHERE EXISTS (SELECT 1 FROM idr_work.subgraph_nodes a WHERE a.entity_key = e.left_entity_key)
  AND EXISTS (SELECT 1 FROM idr_work.subgraph_nodes b WHERE b.entity_key = e.right_entity_key)
""")

# ============================================
# LABEL PROPAGATION
# ============================================
print("🔄 Running label propagation...")

q("""
CREATE OR REPLACE TABLE idr_work.lp_labels AS
SELECT entity_key, entity_key AS label FROM idr_work.subgraph_nodes
""")

iterations = 0
for i in range(MAX_ITERS):
    iterations = i + 1
    
    q("""
    CREATE OR REPLACE TABLE idr_work.lp_labels_next AS
    WITH undirected AS (
      SELECT left_entity_key AS src, right_entity_key AS dst FROM idr_work.subgraph_edges
      UNION ALL
      SELECT right_entity_key AS src, left_entity_key AS dst FROM idr_work.subgraph_edges
    ),
    candidate_labels AS (
      SELECT l.entity_key, l.label AS candidate_label FROM idr_work.lp_labels l
      UNION ALL
      SELECT u.src AS entity_key, l2.label AS candidate_label
      FROM undirected u
      JOIN idr_work.lp_labels l2 ON l2.entity_key = u.dst
    )
    SELECT entity_key, MIN(candidate_label) AS label
    FROM candidate_labels
    GROUP BY entity_key
    """)
    
    delta = collect_one("""
        SELECT SUM(CASE WHEN cur.label <> nxt.label THEN 1 ELSE 0 END)
        FROM idr_work.lp_labels cur
        JOIN idr_work.lp_labels_next nxt USING (entity_key)
    """)
    
    print(f"  iter={i+1} delta_changed={delta}")
    
    if delta == 0:
        break
    
    # Swap tables
    q("DROP TABLE IF EXISTS idr_work.lp_labels")
    q("ALTER TABLE idr_work.lp_labels_next RENAME TO lp_labels")

# ============================================
# UPDATE MEMBERSHIP & CLUSTERS
# ============================================
print("👥 Updating membership...")

# Include singletons (entities with no edges) - they get resolved_id = entity_key
q("""
CREATE OR REPLACE TABLE idr_work.membership_updates AS
SELECT entity_key, label AS resolved_id, CURRENT_TIMESTAMP AS updated_ts
FROM idr_work.lp_labels
UNION ALL
SELECT entity_key, entity_key AS resolved_id, CURRENT_TIMESTAMP AS updated_ts
FROM idr_work.entities_delta
WHERE entity_key NOT IN (SELECT entity_key FROM idr_work.lp_labels)
""")

# Upsert membership
q("""
INSERT INTO idr_out.identity_resolved_membership_current
SELECT * FROM idr_work.membership_updates src
WHERE NOT EXISTS (
    SELECT 1 FROM idr_out.identity_resolved_membership_current tgt
    WHERE tgt.entity_key = src.entity_key
)
""")

q("""
UPDATE idr_out.identity_resolved_membership_current AS tgt
SET resolved_id = src.resolved_id, updated_ts = src.updated_ts
FROM idr_work.membership_updates src
WHERE tgt.entity_key = src.entity_key
""")


q("""
CREATE OR REPLACE TABLE idr_work.impacted_resolved_ids AS
SELECT DISTINCT resolved_id FROM idr_work.membership_updates
""")

q("""
CREATE OR REPLACE TABLE idr_work.cluster_sizes_updates AS
SELECT resolved_id, COUNT(*) AS cluster_size, CURRENT_TIMESTAMP AS updated_ts
FROM idr_out.identity_resolved_membership_current
WHERE resolved_id IN (SELECT resolved_id FROM idr_work.impacted_resolved_ids)
GROUP BY resolved_id
""")

# Upsert clusters
q("""
DELETE FROM idr_out.identity_clusters_current
WHERE resolved_id IN (SELECT resolved_id FROM idr_work.cluster_sizes_updates)
""")
q("""
INSERT INTO idr_out.identity_clusters_current
SELECT * FROM idr_work.cluster_sizes_updates
""")

# ============================================
# BUILD GOLDEN PROFILE
# ============================================
print("🏆 Building golden profiles...")

# Build entities_all for golden profile
q("""
CREATE OR REPLACE TABLE idr_work.entities_all AS
SELECT 
    e.entity_key,
    e.table_id,
    c.email,
    c.phone,
    c.first_name,
    c.last_name,
    c.rec_update_dt AS record_updated_at
FROM idr_work.entities_delta e
LEFT JOIN crm.customer c ON e.entity_key = 'customer:' || c.customer_id
WHERE e.table_id = 'customer'
""")

# Golden profile with window functions
q("""
CREATE OR REPLACE TABLE idr_work.golden_updates AS
WITH impacted AS (
  SELECT DISTINCT resolved_id FROM idr_work.impacted_resolved_ids
),
members AS (
  SELECT m.resolved_id, m.entity_key
  FROM idr_out.identity_resolved_membership_current m
  JOIN impacted i ON i.resolved_id = m.resolved_id
),
ent_ranked AS (
  SELECT 
    m.resolved_id,
    e.email AS email_raw,
    e.phone AS phone_raw,
    e.first_name,
    e.last_name,
    COALESCE(e.record_updated_at, TIMESTAMP '1900-01-01') AS ru
  FROM members m
  LEFT JOIN idr_work.entities_all e ON e.entity_key = m.entity_key
),
email_ranked AS (
  SELECT resolved_id, email_raw,
         ROW_NUMBER() OVER (PARTITION BY resolved_id ORDER BY ru DESC) AS rn
  FROM ent_ranked WHERE email_raw IS NOT NULL
),
phone_ranked AS (
  SELECT resolved_id, phone_raw,
         ROW_NUMBER() OVER (PARTITION BY resolved_id ORDER BY ru DESC) AS rn
  FROM ent_ranked WHERE phone_raw IS NOT NULL
),
first_name_ranked AS (
  SELECT resolved_id, first_name,
         ROW_NUMBER() OVER (PARTITION BY resolved_id ORDER BY ru DESC) AS rn
  FROM ent_ranked WHERE first_name IS NOT NULL
),
last_name_ranked AS (
  SELECT resolved_id, last_name,
         ROW_NUMBER() OVER (PARTITION BY resolved_id ORDER BY ru DESC) AS rn
  FROM ent_ranked WHERE last_name IS NOT NULL
)
SELECT
  i.resolved_id,
  e.email_raw AS email_primary,
  p.phone_raw AS phone_primary,
  f.first_name,
  l.last_name,
  CURRENT_TIMESTAMP AS updated_ts
FROM impacted i
LEFT JOIN email_ranked e ON e.resolved_id = i.resolved_id AND e.rn = 1
LEFT JOIN phone_ranked p ON p.resolved_id = i.resolved_id AND p.rn = 1
LEFT JOIN first_name_ranked f ON f.resolved_id = i.resolved_id AND f.rn = 1
LEFT JOIN last_name_ranked l ON l.resolved_id = i.resolved_id AND l.rn = 1
""")

# Upsert golden profiles
q("""
DELETE FROM idr_out.golden_profile_current
WHERE resolved_id IN (SELECT resolved_id FROM idr_work.golden_updates)
""")
q("""
INSERT INTO idr_out.golden_profile_current
SELECT * FROM idr_work.golden_updates
""")

# ============================================
# UPDATE RUN STATE
# ============================================
print("💾 Updating run state...")

q("""
CREATE OR REPLACE TABLE idr_work.watermark_updates AS
SELECT table_id, MAX(watermark_value) AS new_watermark_value
FROM idr_work.entities_delta
GROUP BY table_id
""")

q(f"""
UPDATE idr_meta.run_state AS tgt
SET last_watermark_value = src.new_watermark_value,
    last_run_id = '{RUN_ID}',
    last_run_ts = TIMESTAMP '{RUN_TS}'
FROM idr_work.watermark_updates src
WHERE tgt.table_id = src.table_id
""")

# ============================================
# FINALIZE
# ============================================
entities_cnt = collect_one("SELECT COUNT(*) FROM idr_work.entities_delta") or 0
edges_cnt = collect_one("SELECT COUNT(*) FROM idr_work.edges_new") or 0
clusters_cnt = collect_one("SELECT COUNT(*) FROM idr_out.identity_clusters_current") or 0
duration = int(time.time() - run_start)

q(f"""
UPDATE idr_out.run_history
SET status = 'SUCCESS',
    ended_at = CURRENT_TIMESTAMP,
    duration_seconds = {duration},
    entities_processed = {entities_cnt},
    edges_created = {edges_cnt},
    clusters_impacted = {clusters_cnt},
    lp_iterations = {iterations}
WHERE run_id = '{RUN_ID}'
""")

con.close()

print(f"\n✅ DuckDB IDR run completed!")
print(f"   Run ID: {RUN_ID}")
print(f"   Database: {DB_PATH}")
print(f"   Entities processed: {entities_cnt}")
print(f"   Edges created: {edges_cnt}")
print(f"   Clusters: {clusters_cnt}")
print(f"   LP iterations: {iterations}")
print(f"   Duration: {duration}s")
