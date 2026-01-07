# MCP Server for Identity Resolution: Feasibility Analysis

## Overview

With CDP platforms building AI agents and MCP (Model Context Protocol) integrations, there's an opportunity to expose unified identity resolution outputs through an MCP server, enabling AI agents to query and act on customer 360 data.

---

## What is MCP?

The Model Context Protocol (MCP) is a standard for connecting AI systems with external tools and data sources. It allows AI agents to:
- Query databases and APIs
- Execute tools with specific parameters
- Access structured resources

**Relevant MCP primitives for Identity Resolution:**
- **Resources**: Expose unified customer profiles as queryable resources
- **Tools**: Provide actions like "lookup customer", "find similar", "explain cluster"

---

## Proposed MCP Tools

### 1. Lookup Customer

```typescript
// mcp_idr_lookup_customer
{
  "name": "lookup_customer",
  "description": "Find a unified customer profile by any identifier",
  "parameters": {
    "identifier_type": "EMAIL | PHONE | CUSTOMER_ID | ANY",
    "identifier_value": "john@example.com",
    "include_sources": true  // Include original source records
  }
}
```

**Returns:** Unified profile with all linked identities, source records, and confidence score.

### 2. Get Customer 360

```typescript
// mcp_idr_customer_360
{
  "name": "get_customer_360",
  "description": "Get complete customer view including activity timeline",
  "parameters": {
    "resolved_id": "crm:12345"
  }
}
```

**Returns:** Golden profile + all source records + summary stats.

### 3. Find Similar Customers

```typescript
// mcp_idr_find_similar
{
  "name": "find_similar_customers",
  "description": "Find customers with matching attributes",
  "parameters": {
    "email_domain": "acme.com",
    "phone_prefix": "+1-415",
    "limit": 10
  }
}
```

### 4. Explain Cluster

```typescript
// mcp_idr_explain_cluster
{
  "name": "explain_cluster",
  "description": "Explain why entities were merged into one identity",
  "parameters": {
    "resolved_id": "crm:12345"
  }
}
```

**Returns:** Edge graph showing which identifiers linked which records.

### 5. Search Customers

```typescript
// mcp_idr_search
{
  "name": "search_customers",
  "description": "Search unified profiles by attributes",
  "parameters": {
    "query": "john smith california",
    "limit": 10
  }
}
```

---

## Proposed MCP Resources

| Resource URI | Description |
|--------------|-------------|
| `idr://profiles/{resolved_id}` | Individual unified profile |
| `idr://clusters/large` | Clusters above size threshold |
| `idr://metrics/latest` | Latest run metrics |
| `idr://schema` | Output table schemas |

---

## Benefits

### 1. **AI-Powered Customer Service**
Agents can look up customers by any identifier:
> "Agent, find the customer who called from 555-1234 and summarize their account."

### 2. **Unified Intent Understanding**
AI can understand that emails from different addresses are the same person:
> "What's the total order value for this customer across all their accounts?"

### 3. **Explainability**
Agents can explain identity decisions to users:
> "These records are linked because they share the same phone number."

### 4. **Multi-Source Queries**
Single query across all source systems via resolved identity:
> "Show me all touchpoints for customer X from CRM, web, and mobile."

### 5. **Composable AI Stack**
MCP enables building AI agents that combine:
- Identity resolution (this MCP)
- Data warehouse (Supabase MCP, etc.)
- Knowledge graphs
- Business systems

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     AI Agent Layer                       │
├─────────────────────────────────────────────────────────┤
│                    MCP Protocol                          │
├──────────────┬──────────────┬──────────────┬────────────┤
│ IDR MCP      │ Supabase MCP │ Slack MCP    │ Other MCPs │
│ Server       │ Server       │ Server       │            │
├──────────────┴──────────────┴──────────────┴────────────┤
│                  Data Warehouse                          │
│           (identity_membership, golden_profiles)         │
└─────────────────────────────────────────────────────────┘
```

---

## Implementation Approach

### Option A: Python MCP Server (Recommended)

```python
# idr_mcp_server.py
from mcp.server import Server
import duckdb  # or snowflake-connector, bigquery, etc.

app = Server("idr-server")

@app.tool()
async def lookup_customer(
    identifier_type: str,
    identifier_value: str,
    include_sources: bool = True
) -> dict:
    """Find unified customer by identifier."""
    conn = get_connection()
    
    # Find resolved_id from identifier
    result = conn.execute("""
        SELECT DISTINCT m.resolved_id
        FROM idr_out.identity_resolved_membership_current m
        JOIN idr_work.subgraph_identifiers i 
          ON m.entity_key = i.entity_key
        WHERE i.identifier_type = ? 
          AND i.identifier_value_norm = LOWER(?)
    """, [identifier_type, identifier_value]).fetchone()
    
    if not result:
        return {"found": False}
    
    resolved_id = result[0]
    
    # Get golden profile
    profile = conn.execute("""
        SELECT * FROM idr_out.golden_profile_current
        WHERE resolved_id = ?
    """, [resolved_id]).fetchdf()
    
    return {
        "found": True,
        "resolved_id": resolved_id,
        "profile": profile.to_dict(),
        "source_count": len(get_source_records(resolved_id))
    }

if __name__ == "__main__":
    app.run()
```

### Option B: Extend Supabase MCP

If using Supabase, add views that wrap identity tables:

```sql
CREATE VIEW customer_lookup AS
SELECT 
    gp.resolved_id,
    gp.email_primary,
    gp.phone_primary,
    gp.first_name,
    gp.last_name,
    c.cluster_size,
    c.confidence_score
FROM idr_out.golden_profile_current gp
JOIN idr_out.identity_clusters_current c 
  ON gp.resolved_id = c.resolved_id;
```

Then use existing Supabase MCP's `execute_sql` tool.

---

## Use Cases by Industry

| Industry | MCP Use Case |
|----------|--------------|
| **E-commerce** | "Find all orders for the customer who just emailed support" |
| **Healthcare** | "Show complete patient history across all facilities" |
| **Finance** | "Display all accounts linked to this phone number" |
| **SaaS** | "Get usage stats for this user across all product tiers" |

---

## Comparison: Direct SQL vs MCP

| Aspect | Direct SQL | MCP Server |
|--------|-----------|------------|
| Access | Data team only | AI agents, apps, services |
| Interface | SQL queries | Natural language via LLM |
| Security | DB credentials | Token-scoped access |
| Abstraction | Raw tables | Domain-specific tools |
| Discoverability | Schema documentation | Tool descriptions |

---

## Recommendation

**Yes, building an MCP server around unified identity tables is valuable.**

### Priority Order:
1. **Start with `lookup_customer`** - Most impactful, enables agent customer service
2. **Add `explain_cluster`** - Supports transparency and debugging
3. **Add `get_customer_360`** - Full view for detailed queries
4. **Add search capabilities** - Natural language customer lookup

### Implementation Effort:
- **MVP (lookup + customer_360):** ~2-3 days
- **Full MCP server:** ~1 week
- **With caching + rate limiting:** ~2 weeks

---

## Next Steps

1. **Prototype** - Build basic Python MCP server with DuckDB
2. **Test with Claude** - Validate tool definitions work as expected
3. **Document** - Add to project documentation
4. **Publish** - Consider adding to MCP server registry

---

*This analysis explores the intersection of AI agents and identity resolution—a natural fit given agents need to understand "who is this customer?" across fragmented data sources.*
