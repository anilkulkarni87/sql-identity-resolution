# Universal Graph Explorer UI: Architecture Plan

## The Challenge
We need a single User Interface to visualize identity graphs (customers, edges, merges) that works seamlessly across **Snowflake**, **BigQuery**, **Databricks**, and **DuckDB**.

## The Solution: "The Adapter Pattern"
We will leverage the exact same "Adapter" design pattern used in the Metadata Loader. By abstracting the database connection, the UI layer becomes completely ignorant of the underlying platform.

### 1. Unified Architecture

```mermaid
graph TD
    UI[Graph Explorer UI <br/> (Streamlit / React)] --> API[Unified Data API <br/> (Python)]
    API --> Adapter[Platform Adapter Interface]
    
    subgraph "Adapters (Already Built)"
        Adapter --> S[SnowflakeAdapter]
        Adapter --> B[BigQueryAdapter]
        Adapter --> D[DuckDBAdapter]
        Adapter --> DB[DatabricksAdapter]
    end
    
    S --> Snow[(Snowflake)]
    B --> BQ[(BigQuery)]
    D --> Duck[(DuckDB)]
    DB --> Data[(Databricks)]
```

### 2. Implementation Strategy (Streamlit)
We recommend **Streamlit** for the v1 implementation. It allows us to build a robust, interactive Data App entirely in Python, reusing our existing codebase.

#### Step A: Shared Library
Refactor `tools/load_metadata.py` to move the `DbAdapter` classes into a shared package `idr_core`.
*   `idr_core/adapters/base.py`
*   `idr_core/adapters/snowflake.py`
*   etc...

#### Step B: The UI Application
The Streamlit app will have a **Connection Manager** in the sidebar.

**User Flow:**
1.  Select Platform (e.g., "Snowflake").
2.  Input Credentials (or auto-load from Env Vars).
3.  Search for a User (Email, ID, Phone).
4.  **Backend Logic**:
    *   `adapter.query(f"SELECT resolved_id FROM idr_out.identity_edges_current WHERE identifier_value = '{email}'")`
    *   `adapter.query(f"SELECT * FROM idr_out.identity_edges_current WHERE resolved_id = '{resolved_id}'")`
5.  **Visualization**:
    *   Use `sreamlit-agraph` or `graphviz` to draw the nodes and edges returned by the generic SQL.
    *   Display the "Golden Profile" in a clean table.

### 3. Why this works
*   **Zero Logic Duplication**: The queries to fetch edges are standard SQL (SELECT * FROM ...).
*   **Security**: The UI runs locally or in your private cloud (ECS/App Runner). No data leaves your control.
*   **Extensibility**: Adding a 5th platform (e.g., PostgreSQL) only requires writing one new Adapter class.

## Example Query Pattern
Changes to the SQL generation aren't needed. The UI simply runs read-only queries against the `idr_out` schema.

```python
# The UI code is identical for all platforms
def get_customer_graph(adapter, customer_email):
    # 1. Find the Resolved ID
    sql = f"""
        SELECT resolved_id 
        FROM idr_out.identity_edges_current 
        WHERE identifier_value = '{customer_email}'
        LIMIT 1
    """
    resolved_id = adapter.fetch_one(sql)
    
    # 2. Get the full Cluster
    graph_sql = f"""
        SELECT left_entity_key, right_entity_key, rule_id 
        FROM idr_out.identity_edges_current 
        WHERE resolved_id = '{resolved_id}'
    """
    edges = adapter.fetch_all(graph_sql)
    return edges
```
