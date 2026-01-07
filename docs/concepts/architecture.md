# Architecture

This document describes the system architecture of SQL Identity Resolution.

---

## High-Level Architecture

```mermaid
graph TB
    subgraph Sources["Source Layer"]
        S1[CRM System]
        S2[E-commerce]
        S3[Mobile App]
        S4[Support Tickets]
    end
    
    subgraph Meta["Metadata Layer"]
        M1["source_table<br/>(table registry)"]
        M2["rule<br/>(matching rules)"]
        M3["identifier_mapping<br/>(column mappings)"]
        M4["config<br/>(settings)"]
    end
    
    subgraph Process["Processing Layer"]
        P1["Extract Entities<br/>(delta detection)"]
        P2["Build Edges<br/>(identifier matching)"]
        P3["Label Propagation<br/>(connected components)"]
        P4["Assign Clusters<br/>(membership update)"]
    end
    
    subgraph Output["Output Layer"]
        O1["identity_resolved_membership_current"]
        O2["identity_clusters_current"]
        O3["golden_profile_current"]
        O4["metrics_export"]
        O5["run_history"]
    end
    
    Sources --> P1
    Meta --> P1
    P1 --> P2
    P2 --> P3
    P3 --> P4
    P4 --> Output
```

---

## Cross-Platform Design

The same core logic runs on all platforms with platform-specific adapters:

```mermaid
graph LR
    subgraph Core["Core SQL Logic"]
        A[DDL Schema]
        B[Edge Building SQL]
        C[Label Propagation SQL]
        D[Output Generation SQL]
    end
    
    subgraph Adapters["Platform Adapters"]
        DA["DuckDB<br/>Python CLI"]
        SN["Snowflake<br/>Stored Procedure"]
        BQ["BigQuery<br/>Python CLI"]
        DB["Databricks<br/>Notebook"]
    end
    
    Core --> DA
    Core --> SN
    Core --> BQ
    Core --> DB
```

---

## Data Flow

### 1. Input Processing

```mermaid
sequenceDiagram
    participant Source as Source Table
    participant Meta as Metadata
    participant Work as Work Tables
    
    Meta->>Work: Read source_table config
    Source->>Work: Extract delta (watermark filter)
    Work->>Work: Generate entity_key
    Work->>Work: Extract identifiers
    Note over Work: entities_delta table populated
```

### 2. Edge Building

```mermaid
sequenceDiagram
    participant Entities as entities_delta
    participant Rules as rule/mapping
    participant Edges as edges_new
    
    Entities->>Rules: Apply identifier extraction
    Rules->>Edges: Match on canonicalized values
    Note over Edges: Edge = (entity_a, entity_b, identifier_type)
```

### 3. Label Propagation

```mermaid
sequenceDiagram
    participant Edges as edges_new
    participant LP as Label Propagation
    participant Labels as lp_labels
    
    Edges->>LP: Initialize labels (label = entity_key)
    loop Until convergence or max_iters
        LP->>LP: Propagate MIN label along edges
    end
    LP->>Labels: Final cluster assignments
```

### 4. Output Generation

```mermaid
sequenceDiagram
    participant Labels as lp_labels
    participant Membership as membership_current
    participant Clusters as clusters_current
    participant Golden as golden_profile_current
    
    Labels->>Membership: Update resolved_id
    Membership->>Clusters: Compute cluster sizes
    Membership->>Golden: Build best-record profiles
```

---

## Schema Design

### idr_meta (Configuration)

| Table | Purpose |
|-------|---------|
| `source_table` | Registry of source tables to process |
| `rule` | Matching rules with priority and limits |
| `identifier_mapping` | Maps source columns to identifier types |
| `run_state` | Watermark tracking per source |
| `config` | Key-value configuration settings |
| `identifier_exclusion` | Values to exclude from matching |

### idr_work (Processing)

| Table | Purpose | Lifecycle |
|-------|---------|-----------|
| `entities_delta` | Entities to process this run | Per-run |
| `identifiers` | Extracted identifier values | Per-run |
| `edges_new` | Entity pairs with matching identifiers | Per-run |
| `lp_labels` | Label propagation results | Per-run |
| `membership_updates` | Proposed membership changes | Per-run |

### idr_out (Output)

| Table | Purpose |
|-------|---------|
| `identity_resolved_membership_current` | Entity → Cluster mapping |
| `identity_clusters_current` | Cluster metadata (size, confidence_score, edge_diversity, match_density, primary_reason) |
| `golden_profile_current` | Best-record profiles per cluster |
| `run_history` | Audit log of all runs |
| `stage_metrics` | Per-stage timing metrics |
| `metrics_export` | Exportable metrics |
| `dry_run_results` | Per-entity dry run changes |
| `dry_run_summary` | Aggregate dry run statistics |
| `skipped_identifier_groups` | Audit of skipped groups |

---

## Processing Modes

### Full Mode

Processes all entities from all source tables:

```mermaid
graph LR
    A[All Source Records] --> B[Full Entity Extraction]
    B --> C[Complete Edge Building]
    C --> D[Full Label Propagation]
    D --> E[Complete Membership Update]
```

### Incremental Mode

Processes only changed entities (watermark-based):

```mermaid
graph LR
    A[Source Records] --> B{updated_at > last_watermark?}
    B -->|Yes| C[Delta Entity Extraction]
    B -->|No| D[Skip]
    C --> E[Incremental Edge Building]
    E --> F[Focused Label Propagation]
    F --> G[Delta Membership Update]
```

---

## Dry Run Architecture

```mermaid
graph TB
    subgraph Normal["Normal Run"]
        N1[Process Entities]
        N2[Build Edges]
        N3[Label Propagation]
        N4[Update Membership]
        N5[Update Clusters]
        N6[Update Golden Profiles]
        N7[Update Watermarks]
    end
    
    subgraph DryRun["Dry Run"]
        D1[Process Entities]
        D2[Build Edges]
        D3[Label Propagation]
        D4[Compute Diff]
        D5[Write dry_run_results]
        D6[Write dry_run_summary]
        D7["⛔ Skip Production Writes"]
    end
```

---

## Metrics Export Architecture

```mermaid
graph LR
    subgraph Runner["IDR Runner"]
        R1[record_metric calls]
    end
    
    subgraph Storage["metrics_export table"]
        S1[(metric_name, value, dimensions)]
    end
    
    subgraph Exporter["metrics_exporter.py"]
        E1[DuckDB Adapter]
        E2[Snowflake Adapter]
        E3[BigQuery Adapter]
    end
    
    subgraph Destinations["Export Destinations"]
        D1[stdout]
        D2[Webhook]
        D3[Prometheus]
        D4[DataDog]
    end
    
    Runner --> Storage
    Storage --> Exporter
    Exporter --> Destinations
```

---

## Security Model

```mermaid
graph TB
    subgraph Access["Access Control"]
        A1[Service Account / Role]
    end
    
    subgraph Permissions["Required Permissions"]
        P1["SELECT on source tables"]
        P2["ALL on idr_meta.*"]
        P3["ALL on idr_work.*"]
        P4["ALL on idr_out.*"]
    end
    
    subgraph Data["Data Flow"]
        D1["Source Data<br/>(read-only)"]
        D2["Work Tables<br/>(transient)"]
        D3["Output Tables<br/>(persistent)"]
    end
    
    Access --> Permissions
    Permissions --> Data
```

---

## Next Steps

- [Matching Algorithm](matching-algorithm.md) - Deep dive into label propagation
- [Data Model](data-model.md) - Complete schema reference
- [Configuration](../guides/configuration.md) - Setting up rules and sources
