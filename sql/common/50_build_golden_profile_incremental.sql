-- Incremental golden profile rebuild for impacted resolved_ids.
-- Uses survivorship metadata. V1 outputs a small set of attributes.
-- Assumes a canonical entities table `idr_work.entities_all` exists with entity attributes:
--   entity_key, table_id, record_updated_at, first_name, last_name, email_raw, phone_raw

DROP TABLE IF EXISTS idr_work.golden_updates;

-- Example strategies implemented:
-- - email_primary: SOURCE_PRIORITY then MOST_RECENT (fallback)
-- - phone_primary: SOURCE_PRIORITY then MOST_RECENT
-- - first/last name: MOST_RECENT non-null
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
)
SELECT
  resolved_id,
  -- Email: prefer trusted source, then most recent
  (SELECT email_raw FROM (
     SELECT
       e.email_raw,
       COALESCE(rs.trust_rank, 9999) AS tr,
       COALESCE(e.record_updated_at, TIMESTAMP '1900-01-01') AS ru
     FROM ent e
     LEFT JOIN ranked_sources rs ON rs.table_id = e.table_id
     WHERE e.email_raw IS NOT NULL
     ORDER BY tr ASC, ru DESC
     LIMIT 1
   ) t) AS email_primary,
  (SELECT phone_raw FROM (
     SELECT
       e.phone_raw,
       COALESCE(rs.trust_rank, 9999) AS tr,
       COALESCE(e.record_updated_at, TIMESTAMP '1900-01-01') AS ru
     FROM ent e
     LEFT JOIN ranked_sources rs ON rs.table_id = e.table_id
     WHERE e.phone_raw IS NOT NULL
     ORDER BY tr ASC, ru DESC
     LIMIT 1
   ) t) AS phone_primary,
  (SELECT first_name FROM (
     SELECT e.first_name, COALESCE(e.record_updated_at, TIMESTAMP '1900-01-01') AS ru
     FROM ent e WHERE e.first_name IS NOT NULL
     ORDER BY ru DESC
     LIMIT 1
   ) t) AS first_name,
  (SELECT last_name FROM (
     SELECT e.last_name, COALESCE(e.record_updated_at, TIMESTAMP '1900-01-01') AS ru
     FROM ent e WHERE e.last_name IS NOT NULL
     ORDER BY ru DESC
     LIMIT 1
   ) t) AS last_name,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS updated_ts
FROM impacted;
