# IDR Performance Benchmark Results

> **Status**: Testing in progress  
> **Dataset**: Retail customer data (deterministic seed: 42)  
> **Last Updated**: _To be filled after testing_

---

## Test Environment Summary

| Platform | Configuration | Instance Type | Notes |
|----------|--------------|---------------|-------|
| DuckDB | Local / Docker | _TBD_ | Single-node |
| Snowflake | _TBD_ | _Warehouse size_ | |
| BigQuery | _TBD_ | _On-demand/Slots_ | |
| Databricks | _TBD_ | _Cluster config_ | |

---

## 20 Million Rows

### Timing Results

| Platform | Data Load | Entity Extract | Edge Build | Label Prop | Output Gen | **Total** |
|----------|-----------|----------------|------------|------------|------------|-----------|
| DuckDB | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |
| Snowflake | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |
| BigQuery | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |
| Databricks | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |

### Metrics Results

| Platform | Entities | Edges | Clusters | Largest | Singletons | LP Iters |
|----------|----------|-------|----------|---------|------------|----------|
| DuckDB | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| Snowflake | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| BigQuery | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| Databricks | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |

### Consistency Check  
- [ ] All platforms produced same cluster count
- [ ] Largest cluster size matches across platforms
- [ ] Singleton count matches across platforms

---

## 50 Million Rows

### Timing Results

| Platform | Data Load | Entity Extract | Edge Build | Label Prop | Output Gen | **Total** |
|----------|-----------|----------------|------------|------------|------------|-----------|
| DuckDB | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |
| Snowflake | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |
| BigQuery | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |
| Databricks | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |

### Metrics Results

| Platform | Entities | Edges | Clusters | Largest | Singletons | LP Iters |
|----------|----------|-------|----------|---------|------------|----------|
| DuckDB | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| Snowflake | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| BigQuery | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| Databricks | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |

### Consistency Check  
- [ ] All platforms produced same cluster count
- [ ] Largest cluster size matches across platforms
- [ ] Singleton count matches across platforms

---

## 100 Million Rows

### Timing Results

| Platform | Data Load | Entity Extract | Edge Build | Label Prop | Output Gen | **Total** |
|----------|-----------|----------------|------------|------------|------------|-----------|
| DuckDB | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |
| Snowflake | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |
| BigQuery | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |
| Databricks | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | **_TBD_** |

### Metrics Results

| Platform | Entities | Edges | Clusters | Largest | Singletons | LP Iters |
|----------|----------|-------|----------|---------|------------|----------|
| DuckDB | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| Snowflake | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| BigQuery | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| Databricks | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |

### Consistency Check  
- [ ] All platforms produced same cluster count
- [ ] Largest cluster size matches across platforms
- [ ] Singleton count matches across platforms

---

## Performance Charts

### Total Duration by Platform & Scale

```
Platform     | 20M      | 50M      | 100M
-------------|----------|----------|----------
DuckDB       | ████     | ████████ | ████████████
Snowflake    | ████     | ████████ | ████████████
BigQuery     | ████     | ████████ | ████████████
Databricks   | ████     | ████████ | ████████████
```
_(To be replaced with actual Chart.js visualization)_

### Cost Analysis (Cloud Only)

| Platform | 20M Cost | 50M Cost | 100M Cost | Notes |
|----------|----------|----------|-----------|-------|
| Snowflake | $_TBD_ | $_TBD_ | $_TBD_ | Warehouse: _TBD_ |
| BigQuery | $_TBD_ | $_TBD_ | $_TBD_ | On-demand pricing |
| Databricks | $_TBD_ | $_TBD_ | $_TBD_ | DBU cost |

---

## Observations & Insights

### DuckDB
- _To be filled_

### Snowflake
- _To be filled_

### BigQuery
- _To be filled_

### Databricks
- _To be filled_

---

## Recommendations

### When to Use Each Platform

| Use Case | Recommended Platform | Rationale |
|----------|---------------------|-----------|
| < 10M rows, local dev | DuckDB | Fast, free, no infra |
| 10-50M rows, batch | _TBD_ | Based on testing |
| 50-100M rows | _TBD_ | Based on testing |
| > 100M rows | _TBD_ | Based on testing |
| Real-time/streaming | _TBD_ | Based on latency needs |

---

## Test Commands

```bash
# Generate 20M dataset
python tools/scale_test/data_generator.py --rows=20000000 --seed=42 --output=data/

# Run DuckDB benchmark
python tools/scale_test/benchmark.py \
    --platform=duckdb \
    --data=data/retail_customers_20m.parquet \
    --rows=20000000 \
    --db=idr_benchmark.duckdb

# Generate 50M dataset
python tools/scale_test/data_generator.py --rows=50000000 --seed=42 --output=data/

# Generate 100M dataset
python tools/scale_test/data_generator.py --rows=100000000 --seed=42 --output=data/
```

---

## Appendix: Test Data Distribution

Target distribution (retail industry standard):

| Cluster Size | Percentage | @ 20M | @ 50M | @ 100M |
|--------------|------------|-------|-------|--------|
| 1 (singleton) | 35% | 7M | 17.5M | 35M |
| 2 (pairs) | 25% | 5M | 12.5M | 25M |
| 3-5 (small) | 20% | 4M | 10M | 20M |
| 6-15 (medium) | 12% | 2.4M | 6M | 12M |
| 16-50 (large) | 5% | 1M | 2.5M | 5M |
| 51-200 (v.large) | 2% | 400K | 1M | 2M |
| 201-1000 (massive) | 1% | 200K | 500K | 1M |

Identifier match rates:
- Email: 55%
- Phone: 25%
- Loyalty ID: 10%
- Address: 10%
- Chain patterns: 15%
