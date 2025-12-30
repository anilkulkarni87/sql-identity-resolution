# Sample data (Databricks)

Run `sql/databricks/notebooks/IDR_SampleData_Generate.py` to create **5 demo source tables**.
Row counts are configurable via widgets (defaults: **2000** per table).

Tables created:
- `main.crm.customer` (watermark: `rec_create_dt`)
- `main.sales.transactions` (watermark: `load_ts`)
- `main.loyalty.loyalty_accounts` (watermark: `updated_at`)
- `main.digital.web_events` (watermark: `event_ts`)
- `main.store.store_visits` (watermark: `visit_ts`)
