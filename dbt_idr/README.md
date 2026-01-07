# dbt_idr - SQL Identity Resolution for dbt

A cross-platform dbt package for deterministic identity resolution. Unify customer identities across multiple data sources using configurable matching rules.

## Features

- ✅ **Fully Dynamic** - SQL auto-generates from seed configuration
- ✅ **Confidence Scoring** - Quality metrics for each cluster
- ✅ **Dry Run Mode** - Preview changes before committing
- ✅ **Cross-Platform** - Snowflake, BigQuery, Databricks, DuckDB
- ✅ **Observability** - Run history, stage metrics, audit trails

## Platform Support

| Platform | Status | Notes |
|----------|--------|-------|
| Snowflake | ✅ | Recursive CTE for label propagation |
| BigQuery | ✅ | Iterative approach (3-pass) |
| Databricks | ✅ | Recursive CTE for label propagation |
| DuckDB | ✅ | Tested with integration suite |

## Installation

Add to your `packages.yml`:

```yaml
packages:
  - git: "https://github.com/anilkulkarni87/sql-identity-resolution"
    subdirectory: "dbt_idr"
    revision: main
```

Then run:
```bash
dbt deps
```

## Quick Start

### 1. Configure Your Sources (Seeds)

Copy and customize the seed CSVs:

**seeds/idr_sources.csv** - Register your source tables:
```csv
source_id,source_name,database,schema,table_name,entity_key_column,watermark_column,is_active
crm,CRM,my_db,crm_schema,customers,customer_id,updated_at,true
orders,Orders,my_db,ecom_schema,orders,order_id,order_date,true
```

> **Note:** Leave `database` empty for DuckDB. Use full database name for Snowflake/BigQuery/Databricks.

**seeds/idr_identifier_mappings.csv** - Map columns to identifier types:
```csv
source_id,identifier_type,column_name,is_hashed
crm,EMAIL,email,false
crm,PHONE,phone,false
orders,EMAIL,customer_email,false
```

**seeds/idr_rules.csv** - Define matching rules:
```csv
rule_id,identifier_type,priority,is_active,canonicalize,max_group_size
email_exact,EMAIL,1,true,LOWERCASE,10000
phone_exact,PHONE,2,true,NONE,5000
```

**seeds/idr_exclusions.csv** - Exclude invalid identifiers:
```csv
identifier_type,identifier_value_pattern,match_type,reason
EMAIL,test@%,LIKE,Test emails
PHONE,0000000000,EXACT,Invalid phone
```

**seeds/idr_survivorship_rules.csv** - How to pick winning attribute values:
```csv
attribute_name,strategy,source_priority_list,recency_field
email,MOST_RECENT,,record_updated_at
phone,MOST_RECENT,,record_updated_at
first_name,MOST_RECENT,,record_updated_at
```

**seeds/idr_attribute_mappings.csv** - Map source columns to profile attributes:
```csv
source_id,attribute_name,column_expr
crm,email,email
crm,phone,phone
crm,first_name,first_name
crm,record_updated_at,updated_at
```

### 2. Run the Pipeline

```bash
# Load configuration seeds
dbt seed --select dbt_idr

# Run all IDR models
dbt run --select dbt_idr
```

**That's it!** SQL is auto-generated from your seed configurations.

## Dry Run Mode

Preview changes before committing to production:

```bash
# Run in dry run mode
dbt run --select dbt_idr --vars '{"idr_dry_run": true}'
```

When `idr_dry_run=true`:
- Models write to **preview schemas** (e.g., `idr_preview_out`)
- `dry_run_results` shows per-entity changes (NEW, MOVED, REMOVED)
- `dry_run_summary` provides aggregate statistics

## Output Models

| Model | Schema | Description |
|-------|--------|-------------|
| `identity_membership` | idr_out | Entity → cluster mapping |
| `identity_clusters` | idr_out | Clusters with confidence scores |
| `golden_profiles` | idr_out | Best-record profiles (dynamic columns) |
| `identity_edges` | idr_out | Persistent identity edges |
| `run_history` | idr_out | Execution metadata |
| `stage_metrics` | idr_out | Row counts per stage |
| `rule_match_audit` | idr_out | Edges created per rule |
| `skipped_identifier_groups` | idr_out | Groups exceeding max_group_size |
| `dry_run_results` | idr_preview | Per-entity changes (dry run only) |
| `dry_run_summary` | idr_preview | Aggregate stats (dry run only) |

## Confidence Scoring

Each cluster gets a quality score (0.0-1.0):

| Column | Description |
|--------|-------------|
| `edge_diversity` | Number of distinct identifier types |
| `match_density` | Actual edges / max possible edges |
| `confidence_score` | Weighted: 50% diversity + 35% density + 15% size |
| `primary_reason` | Human-readable explanation |

## Configuration Variables

Override in your `dbt_project.yml`:

```yaml
vars:
  idr_dry_run: false              # Set true to preview changes
  idr_max_lp_iterations: 30       # Max label propagation iterations
  idr_large_cluster_threshold: 5000
  idr_default_max_group_size: 10000
  idr_enable_confidence_scoring: true
```

## Model Dependencies

```
stg_identifiers (extracts from sources via seeds)
      │
      ▼
  int_edges (builds edges between entities)
      │
      ▼
  int_labels (label propagation → connected components)
      │
      ├──────────────────┐
      ▼                  ▼
identity_membership  identity_clusters
      │
      ▼
golden_profiles (survivorship rules)
```

## Integration Tests

Run the included DuckDB integration tests:

```bash
pip install dbt-duckdb
cd dbt_idr/integration_tests
./run_tests.sh
```

This creates sample data, runs IDR, and validates results.

## Profile Examples

### Snowflake
```yaml
my_profile:
  target: prod
  outputs:
    prod:
      type: snowflake
      account: xxx.us-east-1
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      database: ANALYTICS
      schema: PUBLIC
      warehouse: COMPUTE_WH
```

### BigQuery
```yaml
my_profile:
  target: prod
  outputs:
    prod:
      type: bigquery
      method: oauth
      project: my-gcp-project
      dataset: idr
      location: US
```

### Databricks
```yaml
my_profile:
  target: prod
  outputs:
    prod:
      type: databricks
      host: xxx.cloud.databricks.com
      http_path: /sql/1.0/warehouses/xxx
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      catalog: main
      schema: idr
```

## License

Apache 2.0 - Same as the parent project.
