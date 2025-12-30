# Identity Resolution (SQL-first) — Databricks + Snowflake

Deterministic identity resolution framework that runs in **Databricks SQL** or **Snowflake SQL**.

## Key ideas
- **Metadata-driven**: users configure source tables, keys, watermarks, rules, survivorship in `idr_meta.*`
- **Incremental by default**: process only deltas using user-selected watermark columns
- **Full rerun mode**: rebuild graph and clusters end-to-end when required (e.g., rule changes, unmerge correctness)
- **SQL-first**: common SQL modules + thin platform adapters

## Schemas (recommended)
- `idr_meta` — configuration + run state
- `idr_work` — run-scoped working tables
- `idr_out` — current outputs consumed by downstream

## Quickstart
1. Run `sql/common/00_ddl_meta.sql` and `sql/common/01_ddl_outputs.sql`
2. Seed sample metadata from `metadata_samples/`
3. Run an incremental cycle using:
   - Snowflake: `sql/snowflake/run_loop_example.sql`
   - Databricks: `sql/databricks/run_loop_example.sql`

## Outputs
- `idr_out.identity_edges_current`
- `idr_out.identity_resolved_membership_current`
- `idr_out.identity_clusters_current`
- `idr_out.golden_profile_current`
- `idr_out.rule_match_audit_current`

> Note: This repo ships clustering as **iterative label propagation**. Your orchestrator executes the "step" SQL repeatedly until convergence.
