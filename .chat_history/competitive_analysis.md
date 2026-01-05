# Competitive Analysis: sql-identity-resolution

> **Date**: January 2026  
> **Market Size**: $1.5B (2023) → $5.8B projected by 2032 (16.2% CAGR)

---

## Executive Summary

**sql-identity-resolution occupies a unique niche** in the identity resolution landscape:

| Dimension | This Project | Competition |
|-----------|-------------|-------------|
| **Approach** | SQL-first, deterministic | ML/probabilistic or SaaS |
| **Where it runs** | YOUR data warehouse | Vendor cloud or your Spark cluster |
| **Cost** | Free + warehouse compute | $5K-$50K+/mo or vendor lock-in |
| **Setup** | 1-2 weeks (technical) | Weeks-months + contract |
| **Learning curve** | SQL required | Various |

**Opportunity:** There's no widely-recognized open-source solution that is:
1. SQL-native (not Python/Spark/Java)
2. Multi-platform (Snowflake, BigQuery, Databricks, DuckDB)
3. Deterministic-first (auditable, no ML black box)
4. Zero-cost (beyond warehouse compute)

---

## Competitive Landscape

### Category 1: Open-Source Identity Resolution Tools

| Tool | Technology | Approach | Scale | Key Differentiator |
|------|------------|----------|-------|-------------------|
| **sql-identity-resolution** | SQL | Deterministic | 10M+ tested | Multi-platform, zero dependencies |
| **Zingg** | Spark (Java) | ML/Active Learning | Large | Probabilistic, requires Spark cluster |
| **Dedupe** | Python | ML/Active Learning | Medium | Python native, fuzzy matching |
| **Senzing** | API/Container | ML | Enterprise | Real-time, requires infrastructure |
| **Neo4j** | Graph DB | Graph algorithms | Large | Requires Neo4j deployment |

**sql-identity-resolution advantage:**
- ✅ No Spark cluster required
- ✅ No Python environment required  
- ✅ Works in your EXISTING warehouse
- ✅ No ML training required
- ✅ Deterministic = auditable

**sql-identity-resolution gap:**
- ❌ No fuzzy/probabilistic matching (yet)
- ❌ Less known (marketing gap)

---

### Category 2: Warehouse-Native Solutions (Composable CDP)

| Solution | Type | Platforms | Cost | Key Feature |
|----------|------|-----------|------|-------------|
| **Hightouch Identity Resolution** | Commercial | Snowflake, BigQuery | $$$ | Full CDP integration |
| **Census** | Commercial | Multi | $$$ | Reverse ETL focus |
| **RudderStack** | Open Source/Commercial | Multi | $ - $$$ | Event collection + ID |
| **dbt ID Stitching packages** | Open Source | Multi | Free | Community-maintained |

**sql-identity-resolution vs dbt packages:**
- More complete pipeline (edges → LP → clusters → golden profiles)
- Production-hardened (dry-run, max_group_size, exclusions)
- Better documentation for operationalization

**sql-identity-resolution vs Commercial:**
- 100x cheaper (free vs $5K-50K/mo)
- Same core functionality for deterministic matching
- Less polished UI (SQL-only)

---

### Category 3: Traditional CDPs

| CDP | Type | Cost Range | Identity Resolution |
|-----|------|------------|---------------------|
| Adobe Real-Time CDP | Enterprise | $$$$$$ | Black-box built-in |
| Salesforce Data Cloud | Enterprise | $$$$$$ | Black-box built-in |
| Segment | Mid-Market | $$$$ | Basic stitching |
| Tealium | Enterprise | $$$$$ | Probabilistic focus |
| Amperity | Mid-Enterprise | $$$$ | ML-driven AmpID |

**sql-identity-resolution opportunity:**
For companies that DON'T want/need a full CDP but DO need identity resolution:
- SMBs priced out of CDPs
- Companies with existing warehouses
- Teams that want control/transparency
- Use cases requiring audit trails

---

## Market Opportunity Assessment

### Why This Project Could Succeed

**1. "Composable CDP" Trend is Real**
- Companies building Customer 360 in their warehouse
- Don't want data leaving their cloud
- Prefer modular tools over monoliths
- sql-identity-resolution fits perfectly here

**2. Pricing Gap in Market**
```
                      sql-identity-resolution
                             ↓
|---------|------------|------------|------------|----------|
$0        $1K          $10K         $50K         $100K+
          dbt packages  Commercial   Mid-market   Enterprise
          (limited)     composable   CDPs         CDPs
```
No good option between "free but basic dbt packages" and "$10K+/mo commercial"

**3. Deterministic is Often Enough**
- 60-80% of identity resolution use cases don't need fuzzy
- Email + phone + loyalty covers most retail scenarios
- Auditable matching required for compliance-heavy industries

**4. SQL is the Right Language**
- Data teams already know SQL
- No Python/Spark/ML expertise required
- Easy to customize and extend

### Why This Project Might Struggle

**1. Discovery Problem**
- Not listed on dbt hub
- No Product Hunt / HN launch
- Competes for attention with bigger names (Zingg, Dedupe)

**2. "ML" Marketing**
- Many buyers assume ML = better
- Deterministic can sound "old school"
- Need to reframe as "transparent" and "auditable"

**3. Single Maintainer Risk**
- Open source sustainability concern
- Enterprise buyers want vendor support
- Could be addressed with community/commercial hybrid

**4. Missing Enterprise Features**
- No GUI/dashboard
- No data lineage integration
- No built-in scheduling

---

## Recognition Opportunities

### Short-Term (1-3 months)

| Action | Effort | Impact |
|--------|--------|--------|
| **dbt Hub listing** | Low | High - instant discoverability |
| **Product Hunt launch** | Low | Medium - tech audience |
| **Hacker News Show HN** | Low | Medium - developer credibility |
| **Blog post on warehouse-native ID resolution** | Medium | Medium - SEO |

### Medium-Term (3-6 months)

| Action | Effort | Impact |
|--------|--------|--------|
| **Conference talk (dbt Coalesce, Snowflake Summit)** | Medium | High - enterprise visibility |
| **Case study with SMB customer** | Medium | High - social proof |
| **Comparison blog posts vs Zingg, Hightouch** | Medium | Medium - SEO capture |
| **Integration with dbt** | High | Very High - ecosystem fit |

### Long-Term (6-12 months)

| Action | Effort | Impact |
|--------|--------|--------|
| **Cloud marketplace listing (Snowflake, BigQuery)** | High | Very High - enterprise discovery |
| **Commercial support tier** | High | Medium - sustainability |
| **Fuzzy matching extension** | Very High | High - feature parity |

---

## Recommendations

### Position As:
> "The SQLite of identity resolution - simple, transparent, runs anywhere"

### Key Messages:
1. **"No ML, no black box, no surprises"** - Deterministic = auditable
2. **"Runs in YOUR warehouse"** - No data movement, no vendor lock-in
3. **"10M rows in 3 minutes, for free"** - Performance + cost story
4. **"SQL you can read and modify"** - Transparency and control

### Immediate Actions:
1. ✅ Publish to dbt Hub as a package
2. ✅ Create a comparison page in docs (vs Zingg, Dedupe, Hightouch)
3. ✅ Write a "Composable CDP Identity Layer" blog post
4. ✅ Add GitHub badges/stars request to README

---

## Conclusion

**sql-identity-resolution has a real opportunity** in the identity resolution space:

- **Unique positioning:** SQL-native, multi-platform, deterministic, free
- **Market timing:** Composable CDP trend is accelerating
- **Pricing gap:** Nothing good between free dbt packages and $10K+/mo SaaS
- **Technical quality:** Production-hardened with dry-run, observability, audit

**The main challenge is visibility**, not capability. With the right marketing/community efforts, this could become the go-to open-source identity resolution layer for warehouse-first companies.

> **Recognition potential: HIGH** if positioned correctly as the "warehouse-native identity layer for the composable CDP stack"
