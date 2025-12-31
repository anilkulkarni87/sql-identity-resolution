#!/usr/bin/env python3
"""
BigQuery IDR Runner
Deterministic identity resolution for Google BigQuery.

Usage:
    python idr_run.py --project=my-project --run-mode=FULL --max-iters=30
    
Prerequisites:
    pip install google-cloud-bigquery
    
Environment:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
"""

import argparse
import json
import time
import uuid
from datetime import datetime
from google.cloud import bigquery

# ============================================
# CONFIGURATION
# ============================================
parser = argparse.ArgumentParser(description='BigQuery Identity Resolution Runner')
parser.add_argument('--project', required=True, help='GCP project ID')
parser.add_argument('--run-mode', default='INCR', choices=['INCR', 'FULL'], help='Run mode')
parser.add_argument('--max-iters', type=int, default=30, help='Max label propagation iterations')
args = parser.parse_args()

PROJECT = args.project
RUN_MODE = args.run_mode
MAX_ITERS = args.max_iters
RUN_ID = f"run_{uuid.uuid4().hex[:12]}"
RUN_TS = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
run_start = time.time()

client = bigquery.Client(project=PROJECT)

def q(sql: str):
    """Execute SQL and return results."""
    return client.query(sql).result()

def collect(sql: str) -> list:
    """Execute SQL and return list of dicts."""
    return [dict(row) for row in client.query(sql).result()]

def collect_one(sql: str):
    """Execute SQL and return first value."""
    for row in client.query(sql).result():
        return row[0]
    return None

print(f"🚀 Starting IDR run: {RUN_ID}")
print(f"   Mode: {RUN_MODE}, Max iterations: {MAX_ITERS}")

# ============================================
# PREFLIGHT
# ============================================
# Init run state
q(f"""
MERGE INTO `{PROJECT}.idr_meta.run_state` tgt
USING (SELECT table_id FROM `{PROJECT}.idr_meta.source_table` WHERE is_active = TRUE) src
ON tgt.table_id = src.table_id
WHEN NOT MATCHED THEN INSERT (table_id, last_watermark_value, last_run_id, last_run_ts)
VALUES (src.table_id, TIMESTAMP('1900-01-01 00:00:00'), NULL, NULL)
""")

source_rows = collect(f"""
    SELECT st.table_id, st.table_fqn, st.entity_key_expr, st.watermark_column, 
           st.watermark_lookback_minutes,
           COALESCE(rs.last_watermark_value, TIMESTAMP('1900-01-01')) as last_watermark_value
    FROM `{PROJECT}.idr_meta.source_table` st
    LEFT JOIN `{PROJECT}.idr_meta.run_state` rs ON rs.table_id = st.table_id
    WHERE st.is_active = TRUE
""")

if not source_rows:
    raise RuntimeError("No active source tables found")

mapping_rows = collect(f"SELECT table_id, identifier_type, identifier_value_expr, is_hashed FROM `{PROJECT}.idr_meta.identifier_mapping`")

print(f"✅ Preflight OK: {len(source_rows)} sources, {len(mapping_rows)} mappings")

# Insert initial run history
q(f"""
INSERT INTO `{PROJECT}.idr_out.run_history` 
(run_id, run_mode, status, started_at, source_tables_processed, created_at)
VALUES ('{RUN_ID}', '{RUN_MODE}', 'RUNNING', TIMESTAMP('{RUN_TS}'), {len(source_rows)}, CURRENT_TIMESTAMP())
""")

# ============================================
# BUILD ENTITIES DELTA
# ============================================
print("📊 Building entities delta...")

def build_where(wm_col, last_wm, lookback_min):
    if RUN_MODE == "FULL":
        return "1=1"
    last = f"TIMESTAMP('{last_wm}')" if last_wm else "TIMESTAMP('1900-01-01')"
    lb = lookback_min or 0
    if lb > 0:
        return f"{wm_col} >= TIMESTAMP_SUB({last}, INTERVAL {lb} MINUTE)"
    return f"{wm_col} >= {last}"

entities_parts = []
for r in source_rows:
    where = build_where(r['watermark_column'], r['last_watermark_value'], r.get('watermark_lookback_minutes'))
    entities_parts.append(f"""
        SELECT '{RUN_ID}' AS run_id, '{r['table_id']}' AS table_id,
               CONCAT('{r['table_id']}', ':', CAST(({r['entity_key_expr']}) AS STRING)) AS entity_key,
               CAST({r['watermark_column']} AS TIMESTAMP) AS watermark_value
        FROM `{r['table_fqn']}`
        WHERE {where}
    """)

q(f"CREATE OR REPLACE TABLE `{PROJECT}.idr_work.entities_delta` AS {' UNION ALL '.join(entities_parts)}")

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
                   CONCAT('{r['table_id']}', ':', CAST(({r['entity_key_expr']}) AS STRING)) AS entity_key,
                   '{m['identifier_type']}' AS identifier_type,
                   CAST(({m['identifier_value_expr']}) AS STRING) AS identifier_value,
                   {str(m['is_hashed']).upper()} AS is_hashed
            FROM `{r['table_fqn']}`
        """)

if identifiers_parts:
    q(f"CREATE OR REPLACE TABLE `{PROJECT}.idr_work.identifiers_all_raw` AS {' UNION ALL '.join(identifiers_parts)}")

q(f"""
CREATE OR REPLACE TABLE `{PROJECT}.idr_work.identifiers_all` AS
SELECT i.table_id, i.entity_key, i.identifier_type,
       CASE WHEN r.canonicalize='LOWERCASE' THEN LOWER(i.identifier_value) ELSE i.identifier_value END AS identifier_value_norm,
       i.is_hashed
FROM `{PROJECT}.idr_work.identifiers_all_raw` i
JOIN `{PROJECT}.idr_meta.rule` r ON r.is_active=TRUE AND r.identifier_type=i.identifier_type
WHERE i.identifier_value IS NOT NULL
""")

# ============================================
# BUILD EDGES (Anchor-based)
# ============================================
print("🔗 Building edges...")

q(f"""
CREATE OR REPLACE TABLE `{PROJECT}.idr_work.delta_identifier_values` AS
SELECT DISTINCT i.identifier_type, i.identifier_value_norm
FROM `{PROJECT}.idr_work.entities_delta` e
JOIN `{PROJECT}.idr_work.identifiers_all` i ON i.entity_key = e.entity_key
WHERE i.identifier_value_norm IS NOT NULL
""")

q(f"""
CREATE OR REPLACE TABLE `{PROJECT}.idr_work.members_for_delta_values` AS
SELECT a.table_id, a.entity_key, a.identifier_type, a.identifier_value_norm
FROM `{PROJECT}.idr_work.identifiers_all` a
JOIN `{PROJECT}.idr_work.delta_identifier_values` d
  ON a.identifier_type = d.identifier_type AND a.identifier_value_norm = d.identifier_value_norm
""")

q(f"""
CREATE OR REPLACE TABLE `{PROJECT}.idr_work.group_anchor` AS
SELECT identifier_type, identifier_value_norm, MIN(entity_key) AS anchor_entity_key
FROM `{PROJECT}.idr_work.members_for_delta_values`
GROUP BY identifier_type, identifier_value_norm
""")

q(f"""
CREATE OR REPLACE TABLE `{PROJECT}.idr_work.edges_new` AS
SELECT r.rule_id, ga.anchor_entity_key AS left_entity_key, m.entity_key AS right_entity_key,
       ga.identifier_type, ga.identifier_value_norm,
       TIMESTAMP('{RUN_TS}') AS first_seen_ts, TIMESTAMP('{RUN_TS}') AS last_seen_ts
FROM `{PROJECT}.idr_work.group_anchor` ga
JOIN `{PROJECT}.idr_work.members_for_delta_values` m
  ON m.identifier_type = ga.identifier_type AND m.identifier_value_norm = ga.identifier_value_norm
JOIN `{PROJECT}.idr_meta.rule` r ON r.is_active = TRUE AND r.identifier_type = ga.identifier_type
WHERE m.entity_key <> ga.anchor_entity_key
""")

# Merge edges
q(f"""
MERGE INTO `{PROJECT}.idr_out.identity_edges_current` tgt
USING `{PROJECT}.idr_work.edges_new` src
ON tgt.rule_id=src.rule_id AND tgt.left_entity_key=src.left_entity_key 
   AND tgt.right_entity_key=src.right_entity_key AND tgt.identifier_type=src.identifier_type
   AND tgt.identifier_value_norm=src.identifier_value_norm
WHEN MATCHED THEN UPDATE SET last_seen_ts=src.last_seen_ts
WHEN NOT MATCHED THEN INSERT ROW
""")

# ============================================
# BUILD IMPACTED SUBGRAPH
# ============================================
print("📈 Building impacted subgraph...")

q(f"""
CREATE OR REPLACE TABLE `{PROJECT}.idr_work.impacted_nodes` AS
SELECT DISTINCT left_entity_key AS entity_key FROM `{PROJECT}.idr_work.edges_new`
UNION DISTINCT
SELECT DISTINCT right_entity_key AS entity_key FROM `{PROJECT}.idr_work.edges_new`
""")

q(f"""
CREATE OR REPLACE TABLE `{PROJECT}.idr_work.subgraph_nodes` AS
SELECT DISTINCT entity_key FROM `{PROJECT}.idr_work.impacted_nodes`
UNION DISTINCT
SELECT DISTINCT e.left_entity_key AS entity_key
FROM `{PROJECT}.idr_out.identity_edges_current` e
JOIN `{PROJECT}.idr_work.impacted_nodes` n ON n.entity_key = e.right_entity_key
UNION DISTINCT
SELECT DISTINCT e.right_entity_key AS entity_key
FROM `{PROJECT}.idr_out.identity_edges_current` e
JOIN `{PROJECT}.idr_work.impacted_nodes` n ON n.entity_key = e.left_entity_key
""")

q(f"""
CREATE OR REPLACE TABLE `{PROJECT}.idr_work.subgraph_edges` AS
SELECT e.left_entity_key, e.right_entity_key
FROM `{PROJECT}.idr_out.identity_edges_current` e
JOIN `{PROJECT}.idr_work.subgraph_nodes` a ON a.entity_key = e.left_entity_key
JOIN `{PROJECT}.idr_work.subgraph_nodes` b ON b.entity_key = e.right_entity_key
""")

# ============================================
# LABEL PROPAGATION
# ============================================
print("🔄 Running label propagation...")

q(f"""
CREATE OR REPLACE TABLE `{PROJECT}.idr_work.lp_labels` AS
SELECT entity_key, entity_key AS label FROM `{PROJECT}.idr_work.subgraph_nodes`
""")

iterations = 0
for i in range(MAX_ITERS):
    iterations = i + 1
    
    q(f"""
    CREATE OR REPLACE TABLE `{PROJECT}.idr_work.lp_labels_next` AS
    WITH undirected AS (
      SELECT left_entity_key AS src, right_entity_key AS dst FROM `{PROJECT}.idr_work.subgraph_edges`
      UNION ALL
      SELECT right_entity_key AS src, left_entity_key AS dst FROM `{PROJECT}.idr_work.subgraph_edges`
    ),
    candidate_labels AS (
      SELECT l.entity_key, l.label AS candidate_label FROM `{PROJECT}.idr_work.lp_labels` l
      UNION ALL
      SELECT u.src AS entity_key, l2.label AS candidate_label
      FROM undirected u
      JOIN `{PROJECT}.idr_work.lp_labels` l2 ON l2.entity_key = u.dst
    )
    SELECT entity_key, MIN(candidate_label) AS label
    FROM candidate_labels
    GROUP BY entity_key
    """)
    
    delta = collect_one(f"""
        SELECT SUM(CASE WHEN cur.label <> nxt.label THEN 1 ELSE 0 END)
        FROM `{PROJECT}.idr_work.lp_labels` cur
        JOIN `{PROJECT}.idr_work.lp_labels_next` nxt USING (entity_key)
    """)
    
    print(f"  iter={i+1} delta_changed={delta}")
    
    if delta == 0:
        break
    
    # Swap tables (BigQuery doesn't have SWAP, so we recreate)
    q(f"CREATE OR REPLACE TABLE `{PROJECT}.idr_work.lp_labels` AS SELECT * FROM `{PROJECT}.idr_work.lp_labels_next`")

# ============================================
# UPDATE MEMBERSHIP & CLUSTERS
# ============================================
print("👥 Updating membership...")

# Include singletons (entities with no edges) - they get resolved_id = entity_key
q(f"""
CREATE OR REPLACE TABLE `{PROJECT}.idr_work.membership_updates` AS
SELECT entity_key, label AS resolved_id, CURRENT_TIMESTAMP() AS updated_ts
FROM `{PROJECT}.idr_work.lp_labels`
UNION ALL
SELECT entity_key, entity_key AS resolved_id, CURRENT_TIMESTAMP() AS updated_ts
FROM `{PROJECT}.idr_work.entities_delta`
WHERE entity_key NOT IN (SELECT entity_key FROM `{PROJECT}.idr_work.lp_labels`)
""")

q(f"""
MERGE INTO `{PROJECT}.idr_out.identity_resolved_membership_current` tgt
USING `{PROJECT}.idr_work.membership_updates` src ON tgt.entity_key=src.entity_key
WHEN MATCHED THEN UPDATE SET resolved_id=src.resolved_id, updated_ts=src.updated_ts
WHEN NOT MATCHED THEN INSERT ROW
""")

q(f"""
CREATE OR REPLACE TABLE `{PROJECT}.idr_work.impacted_resolved_ids` AS
SELECT DISTINCT resolved_id FROM `{PROJECT}.idr_work.membership_updates`
""")

q(f"""
CREATE OR REPLACE TABLE `{PROJECT}.idr_work.cluster_sizes_updates` AS
SELECT resolved_id, COUNT(*) AS cluster_size, CURRENT_TIMESTAMP() AS updated_ts
FROM `{PROJECT}.idr_out.identity_resolved_membership_current`
WHERE resolved_id IN (SELECT resolved_id FROM `{PROJECT}.idr_work.impacted_resolved_ids`)
GROUP BY resolved_id
""")

q(f"""
MERGE INTO `{PROJECT}.idr_out.identity_clusters_current` tgt
USING `{PROJECT}.idr_work.cluster_sizes_updates` src ON tgt.resolved_id=src.resolved_id
WHEN MATCHED THEN UPDATE SET cluster_size=src.cluster_size, updated_ts=src.updated_ts
WHEN NOT MATCHED THEN INSERT ROW
""")

# ============================================
# BUILD GOLDEN PROFILE
# ============================================
print("🏆 Building golden profiles...")

q(f"""
CREATE OR REPLACE TABLE `{PROJECT}.idr_work.entities_all` AS
SELECT 
    e.entity_key, e.table_id,
    c.email, c.phone, c.first_name, c.last_name,
    c.rec_update_dt AS record_updated_at
FROM `{PROJECT}.idr_work.entities_delta` e
LEFT JOIN `{PROJECT}.crm.customer` c ON e.entity_key = CONCAT('customer:', c.customer_id)
WHERE e.table_id = 'customer'
""")

q(f"""
CREATE OR REPLACE TABLE `{PROJECT}.idr_work.golden_updates` AS
WITH impacted AS (
  SELECT DISTINCT resolved_id FROM `{PROJECT}.idr_work.impacted_resolved_ids`
),
members AS (
  SELECT m.resolved_id, m.entity_key
  FROM `{PROJECT}.idr_out.identity_resolved_membership_current` m
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
  LEFT JOIN `{PROJECT}.idr_work.entities_all` e ON e.entity_key = m.entity_key
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
  CURRENT_TIMESTAMP() AS updated_ts
FROM impacted i
LEFT JOIN email_ranked e ON e.resolved_id = i.resolved_id AND e.rn = 1
LEFT JOIN phone_ranked p ON p.resolved_id = i.resolved_id AND p.rn = 1
LEFT JOIN first_name_ranked f ON f.resolved_id = i.resolved_id AND f.rn = 1
LEFT JOIN last_name_ranked l ON l.resolved_id = i.resolved_id AND l.rn = 1
""")

q(f"""
MERGE INTO `{PROJECT}.idr_out.golden_profile_current` tgt
USING `{PROJECT}.idr_work.golden_updates` src ON tgt.resolved_id=src.resolved_id
WHEN MATCHED THEN UPDATE SET 
  email_primary=src.email_primary, phone_primary=src.phone_primary,
  first_name=src.first_name, last_name=src.last_name, updated_ts=src.updated_ts
WHEN NOT MATCHED THEN INSERT ROW
""")

# ============================================
# UPDATE RUN STATE
# ============================================
print("💾 Updating run state...")

q(f"""
CREATE OR REPLACE TABLE `{PROJECT}.idr_work.watermark_updates` AS
SELECT table_id, MAX(watermark_value) AS new_watermark_value
FROM `{PROJECT}.idr_work.entities_delta`
GROUP BY table_id
""")

q(f"""
MERGE INTO `{PROJECT}.idr_meta.run_state` tgt
USING `{PROJECT}.idr_work.watermark_updates` src ON tgt.table_id=src.table_id
WHEN MATCHED THEN UPDATE SET 
  last_watermark_value=src.new_watermark_value,
  last_run_id='{RUN_ID}',
  last_run_ts=TIMESTAMP('{RUN_TS}')
""")

# ============================================
# FINALIZE
# ============================================
entities_cnt = collect_one(f"SELECT COUNT(*) FROM `{PROJECT}.idr_work.entities_delta`")
edges_cnt = collect_one(f"SELECT COUNT(*) FROM `{PROJECT}.idr_work.edges_new`")
clusters_cnt = collect_one(f"SELECT COUNT(*) FROM `{PROJECT}.idr_out.identity_clusters_current`")
duration = int(time.time() - run_start)

q(f"""
UPDATE `{PROJECT}.idr_out.run_history` SET
  status = 'SUCCESS',
  ended_at = CURRENT_TIMESTAMP(),
  duration_seconds = {duration},
  entities_processed = {entities_cnt},
  edges_created = {edges_cnt},
  clusters_impacted = {clusters_cnt},
  lp_iterations = {iterations}
WHERE run_id = '{RUN_ID}'
""")

print(f"\n✅ IDR run completed!")
print(f"   Run ID: {RUN_ID}")
print(f"   Entities processed: {entities_cnt}")
print(f"   Edges created: {edges_cnt}")
print(f"   Clusters: {clusters_cnt}")
print(f"   LP iterations: {iterations}")
print(f"   Duration: {duration}s")
