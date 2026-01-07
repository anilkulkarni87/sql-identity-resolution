# What is Warehouse-Native Identity Resolution?

*Building Customer 360 without the CDP price tag*

**Tags:** `identity-resolution` `customer-360` `cdp-alternative` `data-warehouse` `sql`

**Reading time:** 5 minutes

---

> **TL;DR:** Warehouse-native identity resolution runs matching logic inside your existing data warehouse (Snowflake, BigQuery, Databricks) instead of an external CDP. It's 99% cheaper, fully transparent, and you control the logic. It helps businesses create a complete view of each customer by matching and merging information from different sources. After working with CDPs like Treasure Data and similar platforms, I've come to appreciate how critical this capability is—but also how inaccessible it can be for many teams.

In the coming weeks, I intend to write about an alternative approach: **warehouse-native identity resolution**. This post introduces the concept and sets the foundation for deeper technical dives.

## The Problem We're Solving

If you've worked with customer data, you've encountered this scenario:

- CRM has customer Jane with email `jane@gmail.com`
- E-commerce has a transaction from `+1-555-123-4567`
- Loyalty system has member ID `12345`

Same person. Three different records. No link between them.

Traditional CDPs solve this by becoming the centralized platform that manages all customer data. Treasure Data, for example, strikes a balance between data integration, segmentation, and activation. But CDPs come with significant cost—typically $5,000-$50,000/month for mid-sized businesses.

## What is Warehouse-Native Identity Resolution?

The warehouse-native approach keeps identity resolution logic inside your existing data warehouse (Snowflake, BigQuery, Databricks) rather than sending data to an external platform.

Here's how I think about the key differences:

| Aspect | Traditional CDP | Warehouse-Native |
|--------|----------------|------------------|
| **Data Location** | Vendor's platform | Your warehouse |
| **Cost Model** | License fees | Compute costs |
| **Matching Logic** | Black box | SQL you control |
| **Integration Effort** | Significant | Minimal |

## How It Works

The technical approach involves four main components:

```mermaid
flowchart LR
    A[Source Tables] --> B[Entity Extraction]
    B --> C[Edge Building]
    C --> D[Label Propagation]
    D --> E[Golden Profiles]
    
    style A fill:#e1f5fe
    style E fill:#c8e6c9
```

1. **Entity Extraction** - Pull identifiers (email, phone, loyalty ID) from source tables
2. **Edge Building** - Create links between entities sharing common identifiers  
3. **Clustering** - Use graph algorithms (label propagation) to find connected components
4. **Profile Unification** - Build golden records using survivorship rules

This is conceptually similar to what CDPs do internally, but implemented as SQL procedures that run in your warehouse.

## My Approach

I have developed an open-source solution that implements this pattern: [sql-identity-resolution](https://github.com/anilkulkarni87/sql-identity-resolution).

Key design decisions:

- **Deterministic matching** - Exact match on identifiers, fully auditable
- **Multi-platform support** - Same logic works on Snowflake, BigQuery, Databricks, DuckDB
- **Production-ready features** - Dry-run mode, incremental processing, metrics export

The documentation can be found [here](https://anilkulkarni87.github.io/sql-identity-resolution/). Below are the generalized steps for getting started:

1. Install/clone the repository
2. Run the DDL scripts to create required schemas
3. Configure your source tables in metadata
4. Execute the IDR run procedure
5. Query golden profiles and cluster memberships

## Performance Observations

In my testing with benchmark datasets:

| Platform | 10M Records | Time |
|----------|-------------|------|
| Snowflake (X-Small) | 10M | 168s |
| BigQuery | 10M | 189s |
| Databricks | 10M | 215s |
| DuckDB (Local) | 10M | 145s |

Cost for the Snowflake run: approximately $0.50.

## Advantages (From My Perspective)

- **Cost efficiency** - 99% cheaper than CDP licensing
- **Transparency** - Every match decision is traceable
- **Flexibility** - Modify matching logic as needed
- **No vendor lock-in** - Switch warehouses without losing logic

## Next Steps and Limitations

In the coming posts, I'll cover:

- Deterministic vs probabilistic matching approaches
- Step-by-step Snowflake implementation walkthrough
- How label propagation creates identity clusters
- Dry-run mode for safe testing

Current limitations to be aware of:

- Fuzzy matching not yet supported (planned)
- Phone normalization requires custom SQL expressions
- No built-in UI for reviewing matches

## Series Overview

This is the first post in a series on warehouse-native identity resolution:

1. **What is Warehouse-Native Identity Resolution?** ← You are here
2. Deterministic vs Probabilistic Matching
3. Building Customer 360 in Snowflake with SQL
4. The Hidden Cost of CDPs
5. How Label Propagation Works
6. Dry Run Mode Explained
7. Comparing Open Source Identity Resolution Tools
8. From Zero to Customer 360 in 60 Minutes

In conclusion, warehouse-native identity resolution is a powerful approach for businesses that want to gain a deeper understanding of their customers without the CDP price tag. It works best when you have deterministic identifiers and a team comfortable with SQL.

## Related Resources

- [CDP Atlas Interactive Map](https://cdpatlas.vercel.app/map) - Explore the 7 CDP primitives visually
- [CDP Atlas Patterns](https://cdpatlas.vercel.app/patterns) - Architecture patterns for customer data

---

*This is post 1 of 8 in the warehouse-native identity resolution series.*

**Next:** [Deterministic vs Probabilistic Matching](02_deterministic_vs_probabilistic_matching.md)

If you found this helpful:
- ⭐ Star the [GitHub repo](https://github.com/anilkulkarni87/sql-identity-resolution)
- 📖 Check out [CDP Atlas](https://cdpatlas.vercel.app/) for CDP evaluation tools
- 💬 Questions? [Open an issue](https://github.com/anilkulkarni87/sql-identity-resolution/issues)
