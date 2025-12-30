# sql-identity-resolution

Deterministic, metadata-driven **Identity Resolution** for **Databricks (first)** and **Snowflake (next)**.

## End-user workflow
1) Configure metadata in `idr_meta.*`  
2) Run **one** notebook/job: `sql/databricks/notebooks/IDR_Run.py`

## Notes
- `idr_meta.source_table.table_fqn` must be fully qualified: `<catalog>.<schema>.<table>`
- Runner hides all SQL plumbing (multi-statement scripts, clustering loop) from end users.

## Sample data
Run `sql/databricks/notebooks/IDR_SampleData_Generate.py` to create 5 demo source tables (row counts configurable).
