# Runbook (Databricks-first)

## End-user workflow
1. Load metadata into `idr_meta.*`
2. Run `IDR_Run` with params:
   - `RUN_MODE`: INCR | FULL
   - `MAX_ITERS`: default 30

The runner:
- auto-detects repo root (no REPO_PATH)
- executes multi-statement SQL scripts safely
- performs preflight checks and fails fast with clear errors
