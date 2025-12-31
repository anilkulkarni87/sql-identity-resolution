# Changelog

All notable changes to SQL Identity Resolution will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Dry Run Mode** - Preview changes before committing to production tables
- **Metrics Export Framework** - Export metrics to Prometheus, DataDog, or webhooks
- **Data Quality Controls**
  - `max_group_size` per identifier rule to prevent over-matching
  - Identifier exclusion lists for known bad values
  - Skipped identifier groups audit table
- **Dashboard Generator** - Static HTML dashboard with cluster visualizations
- **Scale Testing Framework** - Tools for generating and benchmarking large datasets
- **Multi-Source Data Generator** - Retail-industry synthetic data for testing
- **Industry Templates** - Pre-built configurations for:
  - Retail (Nike/Lululemon style)
  - Healthcare (patient matching)
  - Financial Services (account linking)
  - B2B SaaS (lead deduplication)
- **One-Liner Quickstart** - `make demo` for 60-second demo
- **Docker Support** - Dockerfile and docker-compose for containerized deployment
- **Comprehensive Documentation** - MkDocs Material site with guides and references

### Changed
- Updated `run_history` table with observability columns:
  - `groups_skipped` - Count of identifier groups over max_group_size
  - `values_excluded` - Count of values matching exclusion patterns
  - `large_clusters` - Count of clusters exceeding threshold
  - `warnings` - JSON array of warning messages
- Enhanced cluster size distribution in work tables

### Fixed
- GitHub Actions CI workflow (removed cache requirement)
- MkDocs emoji/icon rendering

## [1.0.0] - 2024-XX-XX

### Added
- Initial release
- DuckDB, Snowflake, BigQuery, and Databricks support
- Metadata-driven configuration
- Anchor-based edge building (O(N) complexity)
- Label propagation clustering
- Golden profile generation with survivorship rules
- Incremental processing with watermarks

---

## Version History Summary

| Version | Date | Highlights |
|---------|------|------------|
| Unreleased | - | Dry run, metrics, data quality, templates |
| 1.0.0 | TBD | Initial release |
