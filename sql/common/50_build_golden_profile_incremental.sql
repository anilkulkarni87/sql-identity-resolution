-- Incremental golden profile rebuild for impacted resolved_ids.
-- Uses survivorship metadata. V1 outputs a small set of attributes.
-- Assumes a canonical entities table `idr_work.entities_all` exists with entity attributes:
--   entity_key, table_id, record_updated_at, first_name, last_name, email_raw, phone_raw
--
-- OPTIMIZATION: Uses window functions instead of correlated subqueries for better performance
-- at scale (large clusters with many member entities).

DROP TABLE IF EXISTS idr_work.golden_updates;

CREATE TABLE idr_work.golden_updates AS
WITH impacted AS (
  SELECT DISTINCT resolved_id FROM idr_work.impacted_resolved_ids
),
members AS (
  SELECT m.resolved_id, m.entity_key
  FROM idr_out.identity_resolved_membership_current m
  JOIN impacted i ON i.resolved_id = m.resolved_id
),
ent AS (
  SELECT e.*, m.resolved_id
  FROM idr_work.entities_all e
  JOIN members m ON m.entity_key = e.entity_key
),
ranked_sources AS (
  SELECT s.table_id, s.trust_rank
  FROM idr_meta.source s
  WHERE s.is_active = TRUE
),
-- Pre-join entities with source rankings once
ent_ranked AS (
  SELECT 
    e.resolved_id,
    e.table_id,
    e.email_raw,
    e.phone_raw,
    e.first_name,
    e.last_name,
    e.record_updated_at,
    COALESCE(rs.trust_rank, 9999) AS trust_rank,
    COALESCE(e.record_updated_at, TIMESTAMP '1900-01-01') AS ru
  FROM ent e
  LEFT JOIN ranked_sources rs ON rs.table_id = e.table_id
),
-- Rank email by trust then recency using window functions
email_ranked AS (
  SELECT 
    resolved_id,
    email_raw,
    ROW_NUMBER() OVER (
      PARTITION BY resolved_id 
      ORDER BY trust_rank ASC, ru DESC
    ) AS rn
  FROM ent_ranked
  WHERE email_raw IS NOT NULL
),
-- Rank phone by trust then recency
phone_ranked AS (
  SELECT 
    resolved_id,
    phone_raw,
    ROW_NUMBER() OVER (
      PARTITION BY resolved_id 
      ORDER BY trust_rank ASC, ru DESC
    ) AS rn
  FROM ent_ranked
  WHERE phone_raw IS NOT NULL
),
-- Rank first_name by recency only
first_name_ranked AS (
  SELECT 
    resolved_id,
    first_name,
    ROW_NUMBER() OVER (
      PARTITION BY resolved_id 
      ORDER BY ru DESC
    ) AS rn
  FROM ent_ranked
  WHERE first_name IS NOT NULL
),
-- Rank last_name by recency only
last_name_ranked AS (
  SELECT 
    resolved_id,
    last_name,
    ROW_NUMBER() OVER (
      PARTITION BY resolved_id 
      ORDER BY ru DESC
    ) AS rn
  FROM ent_ranked
  WHERE last_name IS NOT NULL
)
-- Final join to get best values per resolved_id
SELECT
  i.resolved_id,
  e.email_raw AS email_primary,
  p.phone_raw AS phone_primary,
  f.first_name,
  l.last_name,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS updated_ts
FROM impacted i
LEFT JOIN email_ranked e ON e.resolved_id = i.resolved_id AND e.rn = 1
LEFT JOIN phone_ranked p ON p.resolved_id = i.resolved_id AND p.rn = 1
LEFT JOIN first_name_ranked f ON f.resolved_id = i.resolved_id AND f.rn = 1
LEFT JOIN last_name_ranked l ON l.resolved_id = i.resolved_id AND l.rn = 1;
