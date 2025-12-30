-- After convergence, lp_labels contains final labels for subgraph nodes.
-- We define resolved_id = label (deterministic for current graph state).
-- Update membership_current only for subgraph_nodes (incremental).
DROP TABLE IF EXISTS idr_work.membership_updates;
CREATE TABLE idr_work.membership_updates AS
SELECT
  l.entity_key,
  l.label AS resolved_id,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS updated_ts
FROM idr_work.lp_labels l;

-- Adapter should MERGE membership_updates into idr_out.identity_resolved_membership_current
-- using entity_key as the key.
