# sql-identity-resolution

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**Deterministic, metadata-driven Identity Resolution** for **Databricks**, **Snowflake**, **BigQuery**, and **DuckDB**.

Unify customer identities from multiple sources (CRM, transactions, web events, loyalty) using email, phone, and other identifiers—**no ML required**. Designed for **scale**: millions of records across dozens of sources.

## Supported Platforms

| Platform | Status | Quick Start |
|----------|--------|-------------|
| **Databricks** | ✅ Full | `IDR_QuickStart.py` notebook |
| **Snowflake** | ✅ Full | `CALL idr_run('FULL', 30);` |
| **BigQuery** | ✅ Full | `python idr_run.py --project=... --run-mode=FULL` |
| **DuckDB** | ✅ Full | `python idr_run.py --db=idr.duckdb --run-mode=FULL` |

## Key Features

- **Metadata-driven**: Configure sources and matching rules via tables, no code changes needed
- **Scalable**: Anchor-based edge building (O(N) vs O(N²)), incremental watermark processing
- **Deterministic**: Same inputs always produce same outputs—auditable and reproducible
- **SQL-first**: Core logic in portable SQL, orchestrated by Python/SQL

## Quick Start

### Databricks
```bash
# Full demo (recommended)
Run: sql/databricks/notebooks/IDR_QuickStart.py
```

### Snowflake
```sql
-- 1. Setup
\i sql/snowflake/00_ddl_all.sql
\i sql/snowflake/IDR_SampleData_Generate.sql

-- 2. Run
CALL idr_run('FULL', 30);
```

### BigQuery
```bash
# 1. Setup
bq query < sql/bigquery/00_ddl_all.sql
bq query < sql/bigquery/idr_sample_data.sql

# 2. Run
pip install google-cloud-bigquery
python sql/bigquery/idr_run.py --project=your-project --run-mode=FULL
```

### DuckDB (Local/Testing)
```bash
# 1. Setup
pip install duckdb
duckdb idr.duckdb < sql/duckdb/00_ddl_all.sql
python sql/duckdb/idr_sample_data.py --db=idr.duckdb

# 2. Run
python sql/duckdb/idr_run.py --db=idr.duckdb --run-mode=FULL
```

## Documentation

- [Architecture & Concepts](docs/architecture.md) — Data model, algorithms, and design decisions
- [Scale Considerations](docs/scale_considerations.md) — Performance at millions of rows
- [Runbook](docs/runbook.md) — Operational guide

## How It Works

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Sources   │───▶│  Metadata   │───▶│  IDR Run    │
│ (CRM, POS,  │    │ (rules,     │    │ (edges,     │
│  Web, etc.) │    │  mappings)  │    │  clusters)  │
└─────────────┘    └─────────────┘    └─────────────┘
                                              │
                         ┌────────────────────┴────────────────────┐
                         ▼                    ▼                    ▼
                  ┌────────────┐      ┌────────────┐      ┌────────────┐
                  │   Edges    │      │ Membership │      │  Golden    │
                  │  (links)   │      │ (clusters) │      │  Profile   │
                  └────────────┘      └────────────┘      └────────────┘
```

1. **Extract identifiers** from each source (email, phone, loyalty ID)
2. **Build edges** between entities sharing the same identifier
3. **Cluster** connected entities using label propagation
4. **Generate golden profiles** using survivorship rules

## Configuration

| Table | Purpose |
|-------|---------|
| `idr_meta.source_table` | Source tables to process |
| `idr_meta.rule` | Identifier types and matching rules |
| `idr_meta.identifier_mapping` | How to extract identifiers from each source |
| `idr_meta.survivorship_rule` | Which value wins for golden profile |

See [metadata_samples/](metadata_samples/) for examples.

## Requirements

| Platform | Requirements |
|----------|-------------|
| **Databricks** | Runtime 13.0+ with Unity Catalog |
| **Snowflake** | Standard tier or higher |
| **BigQuery** | Standard tier, `google-cloud-bigquery` Python package |
| **DuckDB** | v0.9+, `duckdb` Python package |

## License

Apache 2.0 — see [LICENSE](LICENSE)

