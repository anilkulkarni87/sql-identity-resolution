# 🔗 SQL Identity Resolution

[![Tests](https://github.com/anilkulkarni87/sql-identity-resolution/actions/workflows/test.yml/badge.svg)](https://github.com/anilkulkarni87/sql-identity-resolution/actions/workflows/test.yml)
[![Docs](https://github.com/anilkulkarni87/sql-identity-resolution/actions/workflows/docs.yml/badge.svg)](https://anilkulkarni87.github.io/sql-identity-resolution/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Platforms](https://img.shields.io/badge/Platforms-DuckDB%20%7C%20Snowflake%20%7C%20BigQuery%20%7C%20Databricks-green.svg)](#supported-platforms)

**Production-grade deterministic identity resolution** for modern data warehouses. Unify customer identities across CRM, transactions, web events, and loyalty data—**no ML required**.

## ⚡ 60-Second Demo

```bash
# Clone and run
git clone https://github.com/anilkulkarni87/sql-identity-resolution.git
cd sql-identity-resolution
make demo
```

That's it! Open `demo_results.html` to see clustered identities.

<details>
<summary>🐳 Docker one-liner (no Python required)</summary>

```bash
docker run -it --rm -v $(pwd)/output:/output ghcr.io/anilkulkarni87/sql-identity-resolution:demo
```

</details>

## 🎯 Why SQL Identity Resolution?

| Challenge | Our Solution |
|-----------|--------------|
| **Expensive CDPs** | Open source, runs on your warehouse |
| **Black-box ML** | Deterministic rules, fully auditable |
| **Vendor lock-in** | Same logic across 4 platforms |
| **Scale limits** | Tested to 100M+ rows |

## 🏗️ Supported Platforms

| Platform | Status | Quickstart |
|----------|--------|------------|
| **DuckDB** | ✅ Full | `make demo` (local) |
| **Snowflake** | ✅ Full | `CALL idr_run('FULL', 30, FALSE);` |
| **BigQuery** | ✅ Full | `python sql/bigquery/idr_run.py --project=...` |
| **Databricks** | ✅ Full | Run `IDR_QuickStart.py` notebook |

## ✨ Key Features

- **🔒 Dry Run Mode** - Preview changes before committing
- **📊 Metrics Export** - Prometheus, DataDog, webhook support
- **🛡️ Data Quality Controls** - max_group_size, exclusion lists
- **📈 Incremental Processing** - Watermark-based efficiency
- **🔍 Full Audit Trail** - Every decision is traceable

## 📊 Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Sources   │────▶│  Configure  │────▶│  IDR Run    │────▶│   Output    │
│             │     │             │     │             │     │             │
│ • CRM       │     │ • Rules     │     │ • Extract   │     │ • Clusters  │
│ • POS       │     │ • Mappings  │     │ • Match     │     │ • Profiles  │
│ • Web       │     │ • Sources   │     │ • Cluster   │     │ • Metrics   │
│ • Mobile    │     │             │     │             │     │             │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

**4 Steps:**
1. **Configure** - Register sources and identifier mappings
2. **Extract** - Pull identifiers (email, phone, loyalty ID)
3. **Match** - Build edges between entities sharing identifiers
4. **Cluster** - Label propagation to find connected components

## 🚀 Getting Started

### Option 1: Local Demo (DuckDB)

```bash
make demo
```

### Option 2: Platform-Specific

<details>
<summary><b>Snowflake</b></summary>

```sql
-- 1. Create objects
\i sql/snowflake/00_ddl_all.sql

-- 2. Configure and run
CALL idr_run('FULL', 30, FALSE);  -- FALSE = live run
CALL idr_run('FULL', 30, TRUE);   -- TRUE = dry run (preview)
```

</details>

<details>
<summary><b>BigQuery</b></summary>

```bash
# 1. Setup
bq query < sql/bigquery/00_ddl_all.sql

# 2. Run
pip install google-cloud-bigquery
python sql/bigquery/idr_run.py --project=your-project --run-mode=FULL
```

</details>

<details>
<summary><b>Databricks</b></summary>

1. Import `sql/databricks/notebooks/IDR_QuickStart.py`
2. Run all cells
3. Check `idr_out.identity_resolved_membership_current`

</details>

## 📖 Documentation

**[📚 Full Documentation](https://anilkulkarni87.github.io/sql-identity-resolution/)**

| Guide | Description |
|-------|-------------|
| [Quick Start](https://anilkulkarni87.github.io/sql-identity-resolution/getting-started/quickstart/) | Get running in 5 minutes |
| [Configuration](https://anilkulkarni87.github.io/sql-identity-resolution/guides/configuration/) | Set up sources and rules |
| [Dry Run Mode](https://anilkulkarni87.github.io/sql-identity-resolution/guides/dry-run-mode/) | Preview before committing |
| [Production Hardening](https://anilkulkarni87.github.io/sql-identity-resolution/guides/production-hardening/) | Enterprise best practices |
| [Architecture](https://anilkulkarni87.github.io/sql-identity-resolution/concepts/architecture/) | How it works |

## 🏭 Industry Templates

Pre-built configurations for common use cases:

| Template | Use Case | Identifiers |
|----------|----------|-------------|
| [Retail](examples/templates/retail/) | Nike, Lululemon style | email, phone, loyalty_id, address |
| [Healthcare](examples/templates/healthcare/) | Patient matching | MRN, SSN, name+DOB |
| [Financial](examples/templates/financial/) | Account linking | account_id, email, SSN |
| [B2B SaaS](examples/templates/b2b_saas/) | Lead deduplication | email, domain, company_name |

## 📊 Performance

Tested on retail customer data (10M rows):

| Platform | Duration | Cost | Clusters |
|----------|----------|------|----------|
| DuckDB | 143s | Free | 1.84M |
| Snowflake | 168s | ~$0.25 | 1.84M |
| BigQuery | 295s | ~$0.50 | 1.84M |
| Databricks | 317s | TBD | 1.84M |

See [benchmarks/](benchmarks/) for full testing suite and results.

## 🤝 Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

```bash
# Run tests locally
make test

# Generate docs locally
make docs
```

## 📜 License

Apache 2.0 — see [LICENSE](LICENSE)

---

<p align="center">
  <b>⭐ Star this repo if you find it useful!</b>
</p>
