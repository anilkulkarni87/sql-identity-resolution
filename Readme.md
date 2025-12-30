# sql-identity-resolution

Deterministic, metadata-driven **Identity Resolution** for **Databricks (first)** and **Snowflake (next)**.

## End-user workflow
1) Run DDL once: `sql/common/00_ddl_meta.sql` and `sql/common/01_ddl_outputs.sql`  
2) Load metadata into `idr_meta.*` (use `sql/databricks/notebooks/IDR_LoadMetadata.py` to ingest `metadata_samples/*.csv`), or supply your own.  
3) Run **one** notebook/job: `sql/databricks/notebooks/IDR_Run.py`
4) (Optional) Verify outputs with `sql/databricks/notebooks/IDR_SmokeTest.py`

## Notes
- `idr_meta.source_table.table_fqn` must be fully qualified: `<catalog>.<schema>.<table>`
- Runner hides all SQL plumbing (multi-statement scripts, clustering loop) from end users.

## Sample data
Run `sql/databricks/notebooks/IDR_SampleData_Generate.py` to create 5 demo source tables (row counts configurable).
