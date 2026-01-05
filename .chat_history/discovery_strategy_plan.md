# Discovery Strategy Plan: sql-identity-resolution

> **Goal**: Increase visibility and adoption through enterprise channels, GitHub discovery, and content marketing

---

## 1. Snowflake Marketplace Native App

### Overview
Package sql-identity-resolution as a **Snowflake Native App** for one-click enterprise deployment.

### Requirements

| Requirement | Current Status | Action Needed |
|-------------|----------------|---------------|
| Snowflake Partner Account | ❓ Unknown | Apply at partner.snowflake.com |
| Native App manifest | ❌ Missing | Create `manifest.yml` |
| Setup script | ✅ Partial | Adapt `00_ddl_all.sql` |
| Application logic | ✅ Ready | `IDR_Run.sql` stored procedure |
| Consumer privileges | ❌ Missing | Define required grants |
| Documentation | ✅ Ready | Adapt existing docs |
| Listing metadata | ❌ Missing | Create description, screenshots |

### Implementation Steps

#### Phase 1: Native App Package (2-3 days)

```
sql-identity-resolution/
├── snowflake_native_app/
│   ├── manifest.yml           # App definition
│   ├── setup_script.sql       # Schema + table creation
│   ├── README.md              # Marketplace listing content
│   └── scripts/
│       ├── install.sql        # One-time setup
│       ├── idr_run.sql        # Main procedure
│       └── uninstall.sql      # Cleanup
```

**manifest.yml template:**
```yaml
manifest_version: 1

artifacts:
  setup_script: setup_script.sql
  readme: README.md
  
version:
  name: sql-identity-resolution
  label: "1.0"
  
privileges:
  - CREATE SCHEMA
  - CREATE TABLE
  - CREATE PROCEDURE
  
references:
  - source_tables:
      label: "Source Tables"
      description: "Access to customer source tables for identity resolution"
      object_type: TABLE
      multi_valued: true
```

#### Phase 2: Partner Application (1-2 weeks)

1. **Apply for Snowflake Partner Network** → partner.snowflake.com
2. **Complete App Validation** → Automated security scan
3. **Submit for Review** → ~5-10 business days
4. **Publish Listing** → Live on marketplace

#### Phase 3: Listing Optimization

**Listing Content:**
```
Title: SQL Identity Resolution - Customer 360 in Your Warehouse

Short Description:
Production-grade deterministic identity resolution. Unify customer 
identities across CRM, web, mobile, and transactions—no ML required, 
no data leaves Snowflake.

Key Features:
✓ Zero data movement
✓ Auditable deterministic matching
✓ Incremental processing
✓ Dry-run mode for safe testing
✓ 10M records in under 3 minutes

Categories: Data Engineering, Customer Data Platform, Data Quality
```

---

## 2. GitHub Topics + README Optimization

### Current State
- Topics: ❓ Check/update
- Badges: ❓ Limited
- README: ✅ Good but can improve discoverability

### GitHub Topics (Add These)

```
identity-resolution
customer-360
data-engineering
snowflake
bigquery
databricks
duckdb
etl
data-quality
deterministic-matching
entity-resolution
record-linkage
customer-data-platform
composable-cdp
master-data-management
```

### README Badges (Add to top)

```markdown
<!-- Discoverability badges -->
[![GitHub stars](https://img.shields.io/github/stars/anilkulkarni87/sql-identity-resolution?style=social)](https://github.com/anilkulkarni87/sql-identity-resolution)
[![GitHub forks](https://img.shields.io/github/forks/anilkulkarni87/sql-identity-resolution?style=social)](https://github.com/anilkulkarni87/sql-identity-resolution/fork)
[![GitHub watchers](https://img.shields.io/github/watchers/anilkulkarni87/sql-identity-resolution?style=social)](https://github.com/anilkulkarni87/sql-identity-resolution)

<!-- Activity badges -->
[![GitHub last commit](https://img.shields.io/github/last-commit/anilkulkarni87/sql-identity-resolution)](https://github.com/anilkulkarni87/sql-identity-resolution/commits/main)
[![GitHub issues](https://img.shields.io/github/issues/anilkulkarni87/sql-identity-resolution)](https://github.com/anilkulkarni87/sql-identity-resolution/issues)

<!-- Platform badges -->
[![Snowflake](https://img.shields.io/badge/Snowflake-✓-29B5E8?logo=snowflake)](https://www.snowflake.com/)
[![BigQuery](https://img.shields.io/badge/BigQuery-✓-4285F4?logo=google-cloud)](https://cloud.google.com/bigquery)
[![Databricks](https://img.shields.io/badge/Databricks-✓-FF3621?logo=databricks)](https://databricks.com/)
[![DuckDB](https://img.shields.io/badge/DuckDB-✓-FFF000?logo=duckdb)](https://duckdb.org/)
```

### README Improvements

Add these sections for discoverability:

```markdown
## Why sql-identity-resolution?

| vs CDPs | vs ML-based (Zingg, Dedupe) | vs dbt packages |
|---------|---------------------------|-----------------|
| Free vs $5K-50K/mo | No cluster needed | More complete pipeline |
| No vendor lock-in | No ML training | Production-hardened |
| Data stays in warehouse | Deterministic output | Multi-platform |

## Who is this for?

- **SMBs** wanting customer 360 without CDP costs
- **Data engineers** building composable CDPs
- **Analysts** who prefer SQL over Python/Spark
- **Compliance teams** needing auditable matching logic
```

### GitHub Actions: Star History Badge

Add to track growth:
```markdown
[![Star History Chart](https://api.star-history.com/svg?repos=anilkulkarni87/sql-identity-resolution&type=Date)](https://star-history.com/#anilkulkarni87/sql-identity-resolution&Date)
```

---

## 3. Blog Series: Warehouse-Native Identity Resolution

### SEO Target Keywords

| Primary | Secondary | Long-tail |
|---------|-----------|-----------|
| identity resolution | customer 360 | warehouse-native identity resolution |
| entity resolution | data matching | snowflake identity resolution |
| record linkage | deterministic matching | bigquery customer matching |
| customer data unification | composable cdp | open source identity resolution |

### Blog Series Plan (8 Posts)

---

#### Post 1: "What is Warehouse-Native Identity Resolution?"
**SEO Target**: "warehouse-native identity resolution", "composable CDP identity"

**Outline**:
1. Problem: Customer data fragmented across systems
2. Traditional solutions: CDPs, ETL, manual matching
3. New approach: Keep data in warehouse, resolve identity there
4. Benefits: Cost, control, transparency
5. CTA: Try sql-identity-resolution

**Word count**: 1,500-2,000
**Publish**: Week 1

---

#### Post 2: "Deterministic vs Probabilistic Identity Resolution: When to Use What"
**SEO Target**: "deterministic vs probabilistic matching", "identity resolution methods"

**Outline**:
1. What is deterministic matching? (exact match on identifiers)
2. What is probabilistic matching? (ML-based fuzzy matching)
3. Comparison table: accuracy, auditability, complexity
4. When deterministic is enough (60-80% of use cases)
5. When you need probabilistic (fuzzy names, addresses)
6. Hybrid approaches
7. CTA: Start with deterministic (link to project)

**Word count**: 2,000-2,500
**Publish**: Week 2

---

#### Post 3: "Building Customer 360 in Snowflake with SQL"
**SEO Target**: "snowflake customer 360", "snowflake identity resolution"

**Outline**:
1. What is Customer 360?
2. Traditional CDP approach vs warehouse-native
3. Step-by-step walkthrough with sql-identity-resolution
   - Setup schemas
   - Configure sources
   - Run identity resolution
   - Query golden profiles
4. Performance benchmarks (168s for 10M rows)
5. CTA: GitHub link + demo

**Word count**: 2,500-3,000 (tutorial style)
**Publish**: Week 3

---

#### Post 4: "The Hidden Cost of CDPs (and the $0 Alternative)"
**SEO Target**: "CDP cost", "customer data platform pricing", "CDP alternative"

**Outline**:
1. What CDPs cost: $5K-$50K/month for SMBs
2. Hidden costs: implementation, training, data egress
3. Do you really need a 'platform'?
4. The modular/composable alternative
5. Cost comparison: CDP vs warehouse-native
6. When CDPs make sense vs when they don't
7. CTA: Try the free alternative

**Word count**: 1,800-2,200
**Publish**: Week 4

---

#### Post 5: "How Label Propagation Creates Identity Clusters (Technical Deep Dive)"
**SEO Target**: "label propagation algorithm", "connected components identity"

**Outline**:
1. The graph representation of identity
2. What is label propagation?
3. Step-by-step worked example
4. Complexity analysis: O(E × D)
5. Edge cases: mega-clusters, convergence
6. SQL implementation walkthrough
7. CTA: View the source code

**Word count**: 2,500-3,000 (technical)
**Publish**: Week 5

---

#### Post 6: "Dry Run Your Identity Resolution Before Committing"
**SEO Target**: "identity resolution testing", "data quality preview"

**Outline**:
1. Why previewing matters (prod safety)
2. What can go wrong: mega-merges, false matches
3. Dry run mode explained
4. How to analyze dry run output
5. Queries to investigate changes
6. Decision workflow: when to proceed vs investigate
7. CTA: Always dry run first

**Word count**: 1,500-2,000
**Publish**: Week 6

---

#### Post 7: "Comparing Open Source Identity Resolution Tools: Zingg vs Dedupe vs SQL"
**SEO Target**: "zingg vs dedupe", "open source identity resolution comparison"

**Outline**:
1. The open source landscape
2. Zingg: Spark-based, ML, enterprise scale
3. Dedupe: Python, active learning, medium scale
4. sql-identity-resolution: SQL-native, deterministic, multi-platform
5. Comparison table: features, requirements, use cases
6. Decision guide: which to choose when
7. CTA: Try SQL-native approach

**Word count**: 2,500-3,000
**Publish**: Week 7

---

#### Post 8: "From Zero to Customer 360 in 60 Minutes (Complete Tutorial)"
**SEO Target**: "customer 360 tutorial", "identity resolution tutorial"

**Outline**:
1. Prerequisites (5 min)
2. Install and setup (10 min)
3. Load sample data (10 min)
4. Configure sources and mappings (15 min)
5. Run dry run (10 min)
6. Commit and validate (10 min)
7. Query your golden profiles
8. Next steps: incremental, monitoring

**Word count**: 3,000-3,500 (hands-on tutorial)
**Publish**: Week 8

---

### Publishing Schedule

| Week | Post | Platform |
|------|------|----------|
| 1 | What is Warehouse-Native Identity Resolution? | Medium, Dev.to, LinkedIn |
| 2 | Deterministic vs Probabilistic | Medium, Dev.to |
| 3 | Building Customer 360 in Snowflake | Medium, Snowflake Community |
| 4 | The Hidden Cost of CDPs | Medium, LinkedIn |
| 5 | Label Propagation Deep Dive | Dev.to, Hacker News |
| 6 | Dry Run Your Identity Resolution | Medium, Dev.to |
| 7 | Comparing Open Source Tools | Medium, Reddit r/dataengineering |
| 8 | Zero to Customer 360 Tutorial | Medium, YouTube (video version) |

### Cross-Promotion

- Each post links back to GitHub
- Each post has schema.org Article markup
- Create Twitter/LinkedIn thread versions
- Repurpose as YouTube videos (Post 3, 8)

---

## Timeline Summary

| Week | Snowflake Marketplace | GitHub | Blog |
|------|----------------------|--------|------|
| 1 | Create Native App package | Add topics + badges | Post 1: What is Warehouse-Native |
| 2 | Submit to partner portal | Update README sections | Post 2: Deterministic vs Probabilistic |
| 3 | Complete validation | Monitor star growth | Post 3: Customer 360 in Snowflake |
| 4 | Respond to review feedback | | Post 4: Hidden Cost of CDPs |
| 5 | Go live on marketplace | | Post 5: Label Propagation |
| 6 | | | Post 6: Dry Run Mode |
| 7 | | | Post 7: Tool Comparison |
| 8 | | | Post 8: Complete Tutorial |

---

## Success Metrics

| Channel | Metric | Target (3 months) |
|---------|--------|-------------------|
| Snowflake Marketplace | Listings | 1 published |
| Snowflake Marketplace | Trial installs | 50+ |
| GitHub | Stars | +200-500 |
| GitHub | Forks | +20-50 |
| Blog | Total views | 10,000+ |
| Blog | GitHub referrals | 500+ |
