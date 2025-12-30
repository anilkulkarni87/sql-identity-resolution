# Sample data (Databricks)

Run `sql/databricks/notebooks/IDR_SampleData_Generate.py` to create **5 demo source tables**.
Row counts are configurable via widgets (defaults: **2000** per table).

Tables created:
- `main.crm.customer` (watermark: `rec_create_dt`)
- `main.sales.transactions` (watermark: `load_ts`)
- `main.loyalty.loyalty_accounts` (watermark: `updated_at`)
- `main.digital.web_events` (watermark: `event_ts`)
- `main.store.store_visits` (watermark: `visit_ts`)

Use the matching metadata templates in `metadata_samples/` (source_table, identifier_mapping, entity_attribute_mapping, etc.) to run `IDR_Run` against these demo tables.
For a turnkey setup, run `sql/databricks/notebooks/IDR_LoadMetadata.py` to ingest the sample CSVs into `idr_meta.*` before executing `IDR_Run.py`.
