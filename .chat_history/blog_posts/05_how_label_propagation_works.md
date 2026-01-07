# How Label Propagation Creates Identity Clusters

*The graph algorithm powering identity resolution*

---

In previous posts, I covered what warehouse-native identity resolution is and how to implement it in Snowflake. Now let's dive into the technical heart of the system: **label propagation**.

This is the algorithm that transforms a graph of identifier relationships into unified customer clusters.

## The Problem We're Solving

After extracting identifiers and building edges, we have a graph:

```
CRM:001 ──[email]── Web:A42 ──[phone]── Mobile:X99
```

We need to assign all connected nodes to the same cluster. This seems simple, but at scale with millions of nodes and edges, it requires an efficient algorithm.

## What is Label Propagation?

Label propagation is a semi-supervised graph algorithm. Here's the concept:

1. **Initialize** - Each node gets a unique label (its own ID)
2. **Iterate** - Each node adopts the minimum label of itself and neighbors
3. **Converge** - Repeat until labels stop changing

### Worked Example

```
Initial state:
  Node A (label: A)
  Node B (label: B)  
  Node C (label: C)
  Edges: A--B, B--C

Iteration 1:
  A sees: [A, B] → adopts min → A
  B sees: [A, B, C] → adopts min → A
  C sees: [B, C] → adopts min → B

State after iter 1:
  A=A, B=A, C=B  (1 node changed)

Iteration 2:
  A sees: [A, A] → A
  B sees: [A, A, B] → A  
  C sees: [A, B] → A

State after iter 2:
  A=A, B=A, C=A  (1 node changed)

Iteration 3:
  All nodes already have label A
  No changes → CONVERGED

Result: All three nodes have label A (same cluster)
```

## SQL Implementation

The implementation in sql-identity-resolution uses iterative SQL:

```sql
-- Initialize: each node is its own label
CREATE TABLE lp_labels AS
SELECT entity_key, entity_key AS label 
FROM subgraph_nodes;

-- Iterate
WHILE changes > 0 AND iteration < max_iterations:
    
    -- Find minimum label among self and neighbors
    CREATE TABLE lp_labels_next AS
    WITH undirected AS (
      SELECT left_entity_key AS src, right_entity_key AS dst FROM edges
      UNION ALL
      SELECT right_entity_key AS src, left_entity_key AS dst FROM edges
    ),
    candidate_labels AS (
      SELECT l.entity_key, l.label AS candidate FROM lp_labels l
      UNION ALL
      SELECT u.src AS entity_key, l2.label AS candidate
      FROM undirected u
      JOIN lp_labels l2 ON l2.entity_key = u.dst
    )
    SELECT entity_key, MIN(candidate) AS label
    FROM candidate_labels
    GROUP BY entity_key;
    
    -- Count changes
    changes = (SELECT COUNT(*) WHERE old.label != new.label);
    
    -- Swap tables
    SWAP lp_labels WITH lp_labels_next;
```

## Complexity Analysis

| Factor | Complexity | Notes |
|--------|------------|-------|
| Time | O(E × D) | E = edges, D = graph diameter |
| Space | O(N) | One label per node |
| Iterations | Typically 5-15 | Depends on graph structure |

For most real-world identity graphs:
- **Diameter is small** (5-10 hops typical)
- **Converges quickly** (under 15 iterations usually)

## Performance in Practice

From my testing with 10M records:

| Metric | Value |
|--------|-------|
| Nodes | 10,000,000 |
| Edges | ~8,500,000 |
| Iterations to converge | 7 |
| Label propagation time | ~45 seconds |

## Edge Cases and Handling

### Giant Clusters

Sometimes a common identifier links too many records:

- Shared company email: `info@company.com`
- Default phone: `000-000-0000`
- Test data: `test@test.com`

sql-identity-resolution handles this with `max_group_size`:

```sql
-- In idr_meta.rule
max_group_size INTEGER DEFAULT 10000  -- Skip groups larger than this
```

Groups exceeding this threshold are logged to audit tables and skipped.

### Disconnected Subgraphs

The algorithm naturally handles disconnected components. Each component converges to its own minimum label—no additional logic needed.

### Singletons

Entities with no edges (no shared identifiers) remain as their own cluster. This is correct behavior—we don't have evidence to link them.

## Why Label Propagation vs Alternatives?

| Algorithm | Pros | Cons |
|-----------|------|------|
| **Label Propagation** | Simple, SQL-friendly, fast | Random ties possible |
| Union-Find | O(N) optimal | Harder in pure SQL |
| Connected Components (Spark) | Optimized at scale | Requires Spark |
| Recursive CTEs | SQL-native | Slow, depth limits |

Label propagation wins for warehouse-native because:
1. **Expressible in SQL** - No external dependencies
2. **Iterative** - Fits warehouse execution model
3. **Deterministic** - MIN() makes ties predictable

## My Approach

In the sql-identity-resolution implementation:

1. **Subgraph extraction** - Only process affected nodes (incremental efficiency)
2. **MAX_ITERS parameter** - Safety limit to prevent runaway iterations
3. **Metrics tracking** - Log iterations and convergence per run
4. **Bidirectional edges** - UNION ALL ensures undirected behavior

## Observability

Track convergence in the output:

```sql
SELECT 
  run_id,
  lp_iterations,
  duration_seconds
FROM idr_out.run_history
ORDER BY started_at DESC;
```

If iterations consistently hit MAX_ITERS, you may have:
- Circular dependencies
- Giant connected components
- Need to increase MAX_ITERS

## Next Steps

In the next post, I'll cover dry-run mode—how to preview identity resolution changes before committing them.

---

*This is post 5 of 8 in the series. Technical questions? Check the [documentation](https://anilkulkarni87.github.io/sql-identity-resolution/) or open an issue on [GitHub](https://github.com/anilkulkarni87/sql-identity-resolution).*
