# Architecture (V1)

## Inputs
Data is already landed in Databricks or Snowflake tables. The framework does not ingest raw files.

Users provide (or the framework builds views for) these logical contracts:
- **Entities**: rows that represent "things we might resolve" (PERSON in V1)
- **Identifiers**: identifier values attached to those entities (EMAIL/PHONE/LOYALTY_ID/etc.)

## Canonical entity key
Each source table config defines `entity_key_expr`. The framework constructs:
- `entity_key = table_id || ':' || CAST(<entity_key_expr> AS STRING)`

`table_id` is a stable logical namespace. Physical table locations (`table_fqn`) can change without breaking identity keys.

## Matching
Deterministic-only, exact match:
- Per rule, we canonicalize identifier values (EMAIL is lowercased in V1).
- Build edges within each identifier group using an **anchor** to avoid quadratic explosion.

## Graph -> clusters
Connected components computed via **label propagation**:
- Initialize label for each node as itself.
- Repeat: label := MIN(label of self and neighbors).
- Stop when labels converge (no changes) or max iterations.

## Incremental mode
1. Compute `entities_delta` and `identifiers_delta` per `source_table` watermark.
2. Compute new edges for identifier values present in the delta (and any existing members sharing those values).
3. Compute impacted subgraph (impacted nodes + 1-hop neighbors).
4. Run label propagation only on impacted subgraph.
5. Merge impacted membership into `*_current` tables.
6. Recompute clusters and golden profile for impacted resolved_ids.

## Full rerun mode
Rebuild edges, clusters, membership, and golden profile from full sources (ignoring watermarks).
