# Scale Testing: Unified Data Generator Plan

## Objective

Create a synthetic data generator that produces **identical datasets** across all 4 platforms for fair 10-20M row scale testing.

---

## Key Requirements

| Requirement | Details |
|-------------|---------|
| **Reproducibility** | Same seed → same data on any platform |
| **Scale** | 10M, 20M, 50M row configurations |
| **Realistic Patterns** | Varying cluster sizes, chain depths, singletons |
| **Export Formats** | Parquet (universal), CSV, platform-native |
| **Benchmark Metrics** | Duration, memory, iterations, cluster quality |

---

## Data Model

### Entity Profiles
```
entity_id       : UUID (deterministic from seed + index)
email           : Synthetic email (may be shared for clustering)
phone           : Synthetic phone (may be shared for clustering)
first_name      : Random from name list
last_name       : Random from name list
address         : Synthetic address
created_at      : Random timestamp
```

### Distribution Parameters

| Parameter | Value | Notes |
|-----------|-------|-------|
| **Singleton Rate** | 40% | Entities with no matches |
| **Pair Rate** | 30% | 2-entity clusters |
| **Small Cluster Rate** | 20% | 3-10 entity clusters |
| **Medium Cluster Rate** | 8% | 11-100 entity clusters |
| **Large Cluster Rate** | 2% | 100-1000 entity clusters |
| **Email Match Rate** | 60% | Proportion of matches via email |
| **Phone Match Rate** | 30% | Proportion of matches via phone |
| **Chain Match Rate** | 10% | Transitive chains (A-B + B-C) |

---

## Proposed Changes

### [NEW] `tools/scale_test/data_generator.py`

Main generator with:
- Deterministic UUID generation from seed
- Configurable row counts and distributions
- Exports to Parquet/CSV
- Platform-specific upload helpers

### [NEW] `tools/scale_test/benchmark.py`

Benchmark runner with:
- Platform adapters
- Timing instrumentation
- Metrics collection
- Report generation

### [NEW] `tools/scale_test/configs/`

Scale test configurations:
- `10m_standard.yaml`
- `20m_standard.yaml`
- `stress_test.yaml`

### [NEW] `tools/load_metadata.py`

Universal metadata loader:
- Reads `idr_config.yaml`
- Connects to target platform (DuckDB/Snowflake/BigQuery/Databricks)
- Validates config integrity
- Truncates and INSERTs into `idr_meta.*` tables
- Usable by dbt, Airflow, or manually

---

## Data Generation Algorithm

```
1. Initialize RNG with seed
2. For each target cluster size bucket:
   a. Generate N clusters of that size
   b. For each cluster:
      - Generate anchor entity
      - Generate (size-1) connected entities
      - Assign shared identifiers based on match rate
3. Generate singleton entities (no shared identifiers)
4. Shuffle all entities
5. Export to Parquet
```

---

## Export Targets

| Platform | Format | Upload Method |
|----------|--------|---------------|
| DuckDB | Parquet | `READ_PARQUET()` |
| Snowflake | Parquet → Stage | `PUT` + `COPY INTO` |
| BigQuery | Parquet → GCS | `bq load` |
| Databricks | Parquet → Volume | `spark.read.parquet()` |

---

## Benchmark Metrics

| Metric | Description |
|--------|-------------|
| `total_duration_seconds` | End-to-end runtime |
| `entity_extraction_seconds` | Phase 1 timing |
| `edge_building_seconds` | Phase 2 timing |
| `label_propagation_seconds` | Phase 3 timing |
| `lp_iterations` | Convergence iterations |
| `edges_created` | Total edges |
| `clusters_created` | Resulting clusters |
| `largest_cluster` | Max cluster size |
| `singleton_count` | Single-entity clusters |
| `memory_peak_mb` | Peak memory (where measurable) |

---

## Verification Plan

1. Generate 10K row sample locally
2. Verify cluster distribution matches target
3. Generate full 10M dataset
4. Load to each platform
5. Run IDR pipeline on each
6. Compare:
   - Cluster counts match
   - Largest cluster sizes match
   - Membership tables are equivalent

---

## Questions

1. **Target scale?** 10M, 20M, or both?
2. **Include address matching?** Or just email/phone?
3. **Run on actual cloud accounts?** (Snowflake, GCP, Databricks)
