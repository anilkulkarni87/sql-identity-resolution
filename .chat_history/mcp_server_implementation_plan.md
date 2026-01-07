# MCP Server Implementation Plan

## Project: Identity Resolution MCP Server

A cross-platform MCP server that exposes unified identity resolution outputs to AI agents.

---

## Phase 1: MVP (2-3 days)

### Goals
- [ ] Basic MCP server with DuckDB support
- [ ] Two core tools: `lookup_customer`, `get_customer_360`
- [ ] Local testing with sample data

### Files to Create

```
mcp_server/
├── src/
│   ├── __init__.py
│   ├── server.py              # MCP server entry point
│   ├── tools/
│   │   ├── __init__.py
│   │   ├── lookup.py          # lookup_customer tool
│   │   └── customer_360.py    # get_customer_360 tool
│   └── connectors/
│       ├── __init__.py
│       ├── base.py            # Abstract connector interface
│       └── duckdb.py          # DuckDB implementation
├── pyproject.toml             # Package config
├── README.md                  # Documentation
└── tests/
    └── test_tools.py
```

### Tool Specifications

#### `lookup_customer`
```python
@app.tool()
async def lookup_customer(
    identifier_type: Literal["EMAIL", "PHONE", "CUSTOMER_ID", "ANY"],
    identifier_value: str,
    include_sources: bool = True
) -> CustomerLookupResult:
    """Find a unified customer profile by any identifier."""
```

#### `get_customer_360`
```python
@app.tool()
async def get_customer_360(
    resolved_id: str
) -> Customer360Result:
    """Get complete customer view including all source records."""
```

### Deliverables
- Working MCP server with `stdio` transport
- DuckDB connector
- Unit tests
- Basic README

---

## Phase 2: Full Tool Suite (3-4 days)

### Goals
- [ ] Add remaining tools
- [ ] Add MCP resources
- [ ] Improve error handling

### Additional Tools

#### `explain_cluster`
```python
@app.tool()
async def explain_cluster(
    resolved_id: str
) -> ClusterExplanation:
    """Explain why entities were merged into one identity."""
```

#### `search_customers`
```python
@app.tool()
async def search_customers(
    email_pattern: str = None,
    phone_pattern: str = None,
    name_pattern: str = None,
    limit: int = 10
) -> list[CustomerSummary]:
    """Search unified profiles by attributes."""
```

#### `get_cluster_health`
```python
@app.tool()
async def get_cluster_health(
    resolved_id: str = None  # None = system summary
) -> ClusterHealthReport:
    """Get confidence scores and health metrics for clusters."""
```

### MCP Resources

```python
@app.resource("idr://profiles/{resolved_id}")
async def get_profile_resource(resolved_id: str) -> Resource:
    """Individual profile as a resource."""

@app.resource("idr://clusters/large")
async def get_large_clusters() -> Resource:
    """Clusters above threshold as resource list."""

@app.resource("idr://metrics/latest")
async def get_latest_metrics() -> Resource:
    """Latest run metrics."""
```

### Deliverables
- 5 total tools
- 3 resources
- Comprehensive error handling
- Updated tests

---

## Phase 3: Multi-Platform Support (2-3 days)

### Goals
- [ ] Snowflake connector
- [ ] BigQuery connector
- [ ] Databricks connector
- [ ] Configuration system

### File Changes

```
mcp_server/
├── src/
│   └── connectors/
│       ├── base.py            # Abstract interface
│       ├── duckdb.py          # ✓ Phase 1
│       ├── snowflake.py       # NEW
│       ├── bigquery.py        # NEW
│       └── databricks.py      # NEW
├── config.yaml                # Platform configuration
```

### Connector Interface

```python
# base.py
from abc import ABC, abstractmethod

class IDRConnector(ABC):
    @abstractmethod
    def lookup_by_identifier(
        self, 
        identifier_type: str, 
        identifier_value: str
    ) -> Optional[str]:
        """Return resolved_id for identifier."""
    
    @abstractmethod
    def get_golden_profile(
        self, 
        resolved_id: str
    ) -> dict:
        """Return golden profile row."""
    
    @abstractmethod
    def get_source_records(
        self, 
        resolved_id: str
    ) -> list[dict]:
        """Return all source records in cluster."""
    
    @abstractmethod
    def get_identity_edges(
        self, 
        resolved_id: str
    ) -> list[dict]:
        """Return edges that form the cluster."""
```

### Configuration

```yaml
# config.yaml
platform: snowflake  # duckdb | snowflake | bigquery | databricks

duckdb:
  path: ./idr.duckdb

snowflake:
  account: ${SNOWFLAKE_ACCOUNT}
  user: ${SNOWFLAKE_USER}
  password: ${SNOWFLAKE_PASSWORD}
  warehouse: ${SNOWFLAKE_WAREHOUSE}
  database: ${SNOWFLAKE_DATABASE}

bigquery:
  project: ${GCP_PROJECT}
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}

databricks:
  host: ${DATABRICKS_HOST}
  http_path: ${DATABRICKS_HTTP_PATH}
  access_token: ${DATABRICKS_TOKEN}
```

### Deliverables
- 4 platform connectors
- Configuration file support
- Environment variable substitution
- Platform-specific tests

---

## Phase 4: Production Hardening (2-3 days)

### Goals
- [ ] Caching layer
- [ ] Rate limiting
- [ ] Observability
- [ ] Security

### Caching
```python
from cachetools import TTLCache

profile_cache = TTLCache(maxsize=1000, ttl=300)  # 5 min TTL

async def get_profile_cached(resolved_id: str):
    if resolved_id in profile_cache:
        return profile_cache[resolved_id]
    result = await get_profile_from_db(resolved_id)
    profile_cache[resolved_id] = result
    return result
```

### Rate Limiting
```python
from slowapi import Limiter

limiter = Limiter(key_func=get_client_id)

@app.tool()
@limiter.limit("100/minute")
async def lookup_customer(...):
    ...
```

### Observability
```python
import structlog

logger = structlog.get_logger()

@app.tool()
async def lookup_customer(identifier_type, identifier_value, ...):
    logger.info("lookup_customer", 
                identifier_type=identifier_type,
                found=result is not None)
```

### Security
- Token-based authentication
- Audit logging
- PII redaction options

### Deliverables
- Caching with TTL
- Rate limiting
- Structured logging
- Audit trail
- Security guide

---

## Phase 5: Documentation & Release (1-2 days)

### Goals
- [ ] Comprehensive README
- [ ] MkDocs integration
- [ ] MCP registry submission
- [ ] Blog post

### Documentation Structure

```
docs/guides/mcp-server.md      # Main guide
docs/reference/mcp-tools.md    # Tool reference
mcp_server/README.md           # Package README
```

### Deliverables
- Complete documentation
- Published to PyPI (optional)
- Submitted to MCP server registry
- Blog post about AI + Identity Resolution

---

## Timeline Summary

| Phase | Duration | Key Deliverable |
|-------|----------|-----------------|
| Phase 1: MVP | 2-3 days | Working server with DuckDB |
| Phase 2: Full Tools | 3-4 days | 5 tools, 3 resources |
| Phase 3: Multi-Platform | 2-3 days | All 4 platform connectors |
| Phase 4: Production | 2-3 days | Caching, security, observability |
| Phase 5: Release | 1-2 days | Documentation, registry |

**Total: 10-15 days**

---

## Dependencies

```toml
# pyproject.toml
[project]
dependencies = [
    "mcp>=1.0.0",
    "duckdb>=0.10.0",
    "snowflake-connector-python>=3.0.0",
    "google-cloud-bigquery>=3.0.0",
    "databricks-sql-connector>=3.0.0",
    "pydantic>=2.0.0",
    "cachetools>=5.0.0",
    "structlog>=24.0.0",
    "pyyaml>=6.0.0",
]
```

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Large result sets | Pagination, limit parameters |
| Slow queries | Caching, query optimization |
| Platform differences | Abstract connector interface |
| Credential management | Environment variables, secret managers |
| PII exposure | Redaction options, audit logging |

---

## Success Criteria

1. **Functional:** All 5 tools work on all 4 platforms
2. **Performance:** <500ms response time for lookups
3. **Reliability:** >99% uptime with proper error handling
4. **Security:** No credential exposure, audit trail
5. **Adoption:** Listed in MCP registry, documented
