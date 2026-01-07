# Open Source Identity Resolution Landscape

*Exploring the available tools*

**Tags:** `identity-resolution` `open-source` `zingg` `dedupe` `data-engineering`

**Reading time:** 5 minutes

---

> **TL;DR:** The open source ecosystem offers several approaches to identity resolution. Zingg uses Spark for ML-based matching at scale. Dedupe provides Python-native probabilistic matching. sql-identity-resolution focuses on warehouse-native deterministic matching.

After building [CDP Atlas](https://cdpatlas.vercel.app/) to help teams evaluate CDP solutions, one question comes up frequently: "What open-source identity resolution tools are available?"

In this post, I'll share an overview of the landscape and the different approaches each tool takes.

## The Open Source Landscape

Identity resolution is a hard problem, and different tools have emerged with different philosophies:

| Tool | Technology | Approach | Best For |
|------|------------|----------|----------|
| **Zingg** | Apache Spark | ML + Active Learning | Large-scale fuzzy matching |
| **Dedupe** | Python | ML + Active Learning | Python workflows |
| **sql-identity-resolution** | SQL | Deterministic | Warehouse-native workflows |
| **Splink** | Spark/DuckDB | Probabilistic | Record linkage at scale |
| **RecordLinkage (R)** | R | Statistical | Research, small datasets |

## Zingg

[Zingg](https://github.com/zingg/zingg) is built on Apache Spark and uses machine learning with active learning for entity resolution.

### Capabilities
- **Active learning** - Interactive model training with minimal labeled data
- **Distributed processing** - Built on Spark for horizontal scalability
- **Multiple match types** - Supports both fuzzy and exact matching
- **Cloud integrations** - Works with Snowflake, Databricks, AWS Glue
- **Enterprise option** - Commercial support available

### Technical Approach
Zingg uses blocking to reduce comparisons and trains models interactively by asking users to label pairs as matches or non-matches.

### Resources
- [GitHub](https://github.com/zingg/zingg)
- [Documentation](https://docs.zingg.ai/)

---

## Dedupe

[Dedupe](https://github.com/dedupeio/dedupe) is a Python library for fuzzy matching and deduplication.

### Capabilities
- **Python-native** - Integrates with pandas, SQL databases
- **Active learning** - Train models by labeling examples
- **Probabilistic** - Provides confidence scores
- **Flexible** - Works with any Python data source
- **Blocking** - Efficient candidate pair generation

### Technical Approach
Dedupe learns from user feedback to build a classifier that predicts whether records match.

### Resources
- [GitHub](https://github.com/dedupeio/dedupe)
- [Documentation](https://docs.dedupe.io/)

---

## Splink

[Splink](https://github.com/moj-analytical-services/splink) is a probabilistic record linkage library developed by the UK Ministry of Justice.

### Capabilities
- **Multi-backend** - DuckDB, Spark, Athena, PostgreSQL
- **Probabilistic** - Fellegi-Sunter model
- **Explainable** - Waterfall charts showing match contributions
- **Scalable** - Handles billions of comparisons
- **Interactive** - Jupyter-friendly visualizations

### Technical Approach
Uses the Fellegi-Sunter model with expectation-maximization to estimate match weights.

### Resources
- [GitHub](https://github.com/moj-analytical-services/splink)
- [Documentation](https://moj-analytical-services.github.io/splink/)

---

## sql-identity-resolution

[sql-identity-resolution](https://github.com/anilkulkarni87/sql-identity-resolution) is the tool I built for warehouse-native deterministic matching.

### Capabilities
- **SQL-native** - Runs inside your data warehouse
- **Deterministic** - Exact match on identifiers
- **Multi-platform** - Snowflake, BigQuery, Databricks, DuckDB
- **dbt integration** - Native dbt package available
- **Production features** - Dry run, incremental, audit trail

### Technical Approach
Uses label propagation on a graph of shared identifiers to cluster entities.

### Resources
- [GitHub](https://github.com/anilkulkarni87/sql-identity-resolution)
- [Documentation](https://anilkulkarni87.github.io/sql-identity-resolution/)

---

## Choosing the Right Approach

Different scenarios call for different tools:

### When ML-Based Matching Helps
- Data with typos, abbreviations, variations
- Name and address as primary identifiers
- Training data available or can be generated

### When Deterministic Matching Suffices
- Strong identifiers available (email, phone, IDs)
- Internal data sources with consistent formatting
- Audit trail and explainability required

### Hybrid Approaches
Some organizations use multiple tools:
1. Start with deterministic matching on strong identifiers
2. Enhance with probabilistic matching on remaining records
3. Human review for low-confidence matches

## Connection to CDP Patterns

On [CDP Atlas Patterns](https://cdpatlas.vercel.app/patterns), I've documented different architectural approaches for identity resolution within a CDP context. The choice of tool often depends on your broader data architecture.

## Further Reading

- [Zingg Blog](https://www.zingg.ai/blog)
- [Splink Demos](https://moj-analytical-services.github.io/splink/demos.html)
- [Entity Resolution Survey Paper](https://arxiv.org/abs/2009.06420)

---

*This is post 7 of 8 in the warehouse-native identity resolution series.*

**Next:** [From Zero to Customer 360 in 60 Minutes](08_zero_to_customer_360_tutorial.md)

If you found this helpful:
- ⭐ Star the [GitHub repo](https://github.com/anilkulkarni87/sql-identity-resolution)
- 📖 Check out [CDP Atlas](https://cdpatlas.vercel.app/) for CDP evaluation tools
- 💬 Questions? [Open an issue](https://github.com/anilkulkarni87/sql-identity-resolution/issues)
