# Runbook (Databricks-first)

## Configure metadata
Users define:
- `idr_meta.source_table` (per table_id): `table_fqn`, `entity_key_expr`, `watermark_column`, optional lookback
- `idr_meta.identifier_mapping`: extraction expressions per identifier_type
- `idr_meta.rule`: identifier types + canonicalization (EMAIL lowercased)
- `idr_meta.entity_attribute_mapping` (optional): expressions for golden profile attributes
- `idr_meta.source` + `idr_meta.survivorship_rule` (optional): survivorship preferences

State:
- `idr_meta.run_state` tracks last processed watermark per table_id

## Databricks runner notebook
Use: `sql/databricks/notebooks/IDR_Run.py`

Widgets:
- `RUN_MODE`: INCR | FULL
- `RUN_ID`: optional (auto-generated if blank)
- `MAX_ITERS`: label propagation max iterations (default 30)
- `REPO_PATH`: Workspace path to your repo root that contains `identity-resolution/`

## Notes
- The runner generates per-table SQL from metadata (no manual UNIONs).
- Connected components are computed on the impacted subgraph via iterative label propagation in Python.
- FULL mode ignores watermarks and processes all rows.
