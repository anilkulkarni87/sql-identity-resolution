# Refactoring Plan: Separating Benchmarks from Production Code

**Created:** 2025-01-05  
**Status:** Planned (execute after Snowflake/Databricks testing complete)

---

## Objective

Separate scale testing and benchmark code from production code to keep the main codebase lean and focused on what users need for adoption.

## Recommended Structure

### Before (Current)
```
sql-identity-resolution/
├── sql/
│   ├── duckdb/
│   │   ├── core/           # Production code
│   │   └── testing/        # Scale tests (to move)
│   ├── bigquery/
│   │   ├── core/           # Production code
│   │   └── testing/        # Scale tests (to move)
│   ├── snowflake/
│   │   ├── core/           # Production code
│   │   └── testing/        # Scale tests (to move)
│   └── databricks/
│       ├── core/           # Production code
│       └── testing/        # Scale tests (to move)
├── tools/
│   ├── load_metadata.py    # Keep - production tool
│   └── cross_platform_loader.py  # Move - testing only
└── docs/performance/       # Move - benchmark results
```

### After (Target)
```
sql-identity-resolution/
├── sql/
│   ├── duckdb/core/        # Production only
│   ├── bigquery/core/      # Production only
│   ├── snowflake/core/     # Production only
│   └── databricks/core/    # Production only
├── tools/
│   └── load_metadata.py    # Production tool
├── docs/                   # User documentation only
├── examples/
│   └── sample_config.yaml  # Quick start example
└── benchmarks/             # NEW - All testing/benchmarks
    ├── README.md
    ├── data/               # Generated test data (gitignored)
    ├── duckdb/
    │   ├── idr_scale_test.py
    │   └── scale_test.duckdb (gitignored)
    ├── bigquery/
    │   ├── scale_test_setup.sql
    │   └── scale_test_metadata.sql
    ├── snowflake/
    │   └── IDR_ScaleTest.sql
    ├── databricks/
    │   └── IDR_ScaleTest.py
    ├── tools/
    │   └── cross_platform_loader.py
    └── results/
        └── benchmark-results.md
```

---

## Files to Move

### Testing Scripts
| Source | Destination |
|--------|-------------|
| `sql/duckdb/testing/` | `benchmarks/duckdb/` |
| `sql/bigquery/testing/` | `benchmarks/bigquery/` |
| `sql/snowflake/testing/` | `benchmarks/snowflake/` |
| `sql/databricks/testing/` | `benchmarks/databricks/` |

### Tools
| Source | Destination |
|--------|-------------|
| `tools/cross_platform_loader.py` | `benchmarks/tools/` |

### Documentation
| Source | Destination |
|--------|-------------|
| `docs/performance/benchmark-results.md` | `benchmarks/results/` |

### Generated Files (Add to .gitignore)
- `sql/duckdb/scale_test.duckdb`
- `benchmarks/data/*.parquet`
- `benchmarks/duckdb/*.duckdb`

---

## Execution Steps

### Step 1: Create benchmarks structure
```bash
mkdir -p benchmarks/{duckdb,bigquery,snowflake,databricks,tools,results,data}
```

### Step 2: Move testing files
```bash
# DuckDB
mv sql/duckdb/testing/* benchmarks/duckdb/

# BigQuery
mv sql/bigquery/testing/* benchmarks/bigquery/

# Snowflake
mv sql/snowflake/testing/* benchmarks/snowflake/

# Databricks
mv sql/databricks/testing/* benchmarks/databricks/
```

### Step 3: Move supporting files
```bash
mv tools/cross_platform_loader.py benchmarks/tools/
mv docs/performance/benchmark-results.md benchmarks/results/
```

### Step 4: Remove empty testing directories
```bash
rmdir sql/duckdb/testing sql/bigquery/testing sql/snowflake/testing sql/databricks/testing
```

### Step 5: Update .gitignore
```
# Benchmark generated files
benchmarks/data/
benchmarks/duckdb/*.duckdb
```

### Step 6: Create benchmarks/README.md
Document how to run benchmarks

### Step 7: Update main README.md
Add section pointing to /benchmarks for testing

### Step 8: Update mkdocs.yml
Remove performance from main docs, or link externally

---

## Benefits

1. **Cleaner production codebase** - Users see only what they need
2. **Faster clone** - Benchmark data excluded by default
3. **Clear separation** - Contributors know where tests belong
4. **Version aligned** - Tests stay in sync with production code
5. **Easy CI/CD** - Single repo, no cross-repo coordination

---

## Prerequisites (Complete First)

- [ ] Complete Snowflake scale testing
- [ ] Complete Databricks scale testing
- [ ] Verify all benchmark results documented
- [ ] Ensure all platform stage timing works

---

## Notes

- Keep scale testing in same repo (not separate) for version alignment
- Benchmark results should still be referenced from main docs via link
- Consider adding a simple "quick test" example in `examples/` for adoption
