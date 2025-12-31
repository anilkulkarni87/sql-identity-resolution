# Runbook (Databricks-first)

## Quickstart (Databricks)
1. Run `sql/databricks/notebooks/IDR_LoadMetadata_Simple.py` (point `META_ROOT` to your metadata folder or volume; defaults to repo `metadata_samples/`).
2. Optionally generate sample source data with `sql/databricks/notebooks/IDR_SampleData_Generate.py`.
3. Run `IDR_Run` with params:
   - `RUN_MODE`: INCR | FULL
   - `MAX_ITERS`: default 30
4. (Optional) Run `sql/databricks/notebooks/IDR_SmokeTest.py` to assert outputs are populated and run_state was updated.

## End-user workflow
1. Run DDL once: `sql/common/00_ddl_meta.sql` (metadata tables) and `sql/common/01_ddl_outputs.sql` (work/output schemas).
2. Load metadata into `idr_meta.*`:
   - `source_table`, `run_state` (watermark tracking)
   - `source` (trust ranks for survivorship), `rule` (identifier rules)
   - `identifier_mapping` (per-table identifier extracts), `entity_attribute_mapping` (attributes for golden profile)
   - `survivorship_rule` (strategies for golden profile fields)
3. Run `IDR_Run` with params above.

The runner:
- auto-detects repo root (no REPO_PATH)
- executes multi-statement SQL scripts safely
- performs preflight checks and fails fast with clear errors
