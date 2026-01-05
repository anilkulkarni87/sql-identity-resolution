# Fuzzy Matching Extension - Design Document

> **Status**: Proposed  
> **Author**: Technical Architect  
> **Date**: 2026-01-05  
> **Affects**: All platforms (DuckDB, Snowflake, BigQuery, Databricks)

---

## 1. Executive Summary

This document proposes extending sql-identity-resolution with **fuzzy matching capabilities** that work alongside existing deterministic rules. The extension enables matching entities with similar (but not identical) names, addresses, and other attributes while maintaining the system's deterministic audit trail.

**Key Design Principles:**
- Deterministic matches always take priority
- Fuzzy matches require blocking to limit comparisons
- All matches include a confidence score for transparency
- Fuzzy candidates below threshold are logged for human review

---

## 2. Current Architecture

### Existing Flow

```
Source Tables → Entity Extraction → Identifier Extraction → Edge Building → Label Propagation → Output
                                           ↑
                                   EXACT MATCH ONLY
                               (after canonicalization)
```

### Current Edge Creation Logic

```sql
-- Edges are created when TWO entities share the SAME identifier value
SELECT a.entity_key AS left, b.entity_key AS right, a.identifier_type, a.identifier_value_norm
FROM identifiers a
JOIN identifiers b ON a.identifier_type = b.identifier_type 
                   AND a.identifier_value_norm = b.identifier_value_norm
WHERE a.entity_key < b.entity_key
```

**Limitation:** This approach cannot match:
- `"John Smith"` ↔ `"Jon Smyth"` (typo/variant)
- `"123 Main St"` ↔ `"123 Main Street, Apt 5"` (address normalization)
- `"McDonald's"` ↔ `"McDonalds"` (punctuation)

---

## 3. Proposed Extension

### 3.1 Extended Flow

```
Source Tables → Entity Extraction → Identifier Extraction 
                                          ↓
                              ┌───────────────────────┐
                              │     EDGE BUILDING     │
                              ├───────────────────────┤
                              │ 1. Deterministic Pass │ ← Current logic (unchanged)
                              │    - Exact matches    │
                              │    - Score = 1.0      │
                              ├───────────────────────┤
                              │ 2. Fuzzy Pass         │ ← NEW
                              │    - Blocking first   │
                              │    - Similarity score │
                              │    - Threshold filter │
                              └───────────────────────┘
                                          ↓
                               Label Propagation → Output
```

### 3.2 Schema Changes

#### New Metadata Table: `idr_meta.fuzzy_rule`

```sql
CREATE TABLE idr_meta.fuzzy_rule (
    rule_id VARCHAR(50) PRIMARY KEY,
    rule_name VARCHAR(255) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    priority INT DEFAULT 100,          -- Lower = runs first
    
    -- BLOCKING (required to limit O(N²) comparisons)
    blocking_expr VARCHAR(500),         -- SQL expression for blocking key
    blocking_type VARCHAR(20),          -- 'EXACT', 'PREFIX', 'SOUNDEX'
    
    -- COMPARISON FIELDS
    compare_attributes JSON,            -- ["first_name", "last_name", "address"]
    attribute_weights JSON,             -- {"first_name": 0.3, "last_name": 0.4, "address": 0.3}
    
    -- SIMILARITY FUNCTION
    similarity_function VARCHAR(50),    -- 'JARO_WINKLER', 'LEVENSHTEIN', 'TOKEN_OVERLAP', 'SOUNDEX'
    
    -- THRESHOLDS
    min_total_score FLOAT DEFAULT 0.85, -- Combined score threshold
    min_per_attribute FLOAT DEFAULT 0.7,-- Each attribute must score at least this
    
    -- BEHAVIOR
    require_deterministic_anchor BOOLEAN DEFAULT FALSE,  -- Only fuzzy if deterministic exists
    max_candidates_per_entity INT DEFAULT 10,            -- Limit explosion
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);
```

#### Extended Edge Table

```sql
-- Add columns to existing table
ALTER TABLE idr_out.identity_edges_current ADD COLUMN IF NOT EXISTS
    match_score FLOAT DEFAULT 1.0;

ALTER TABLE idr_out.identity_edges_current ADD COLUMN IF NOT EXISTS
    match_type VARCHAR(20) DEFAULT 'DETERMINISTIC';  -- 'DETERMINISTIC' | 'FUZZY'

ALTER TABLE idr_out.identity_edges_current ADD COLUMN IF NOT EXISTS
    score_breakdown JSON;  -- {"first_name": 0.91, "last_name": 0.95, "address": 0.78}
```

#### New Audit Table: `idr_out.fuzzy_candidates`

```sql
CREATE TABLE idr_out.fuzzy_candidates (
    run_id VARCHAR(50),
    left_entity_key VARCHAR(255),
    right_entity_key VARCHAR(255),
    rule_id VARCHAR(50),
    total_score FLOAT,
    score_breakdown JSON,
    status VARCHAR(20) DEFAULT 'PENDING',  -- 'PENDING', 'APPROVED', 'REJECTED'
    reviewed_by VARCHAR(100),
    reviewed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## 4. Algorithm Details

### 4.1 Blocking Strategy

**Why blocking is required:**  
Without blocking, fuzzy matching requires N² comparisons (comparing every entity to every other). For 1M entities, that's 1 trillion comparisons.

**Blocking reduces this by grouping entities into "buckets"** and only comparing within buckets:

| Blocking Type | Example | Use Case |
|---------------|---------|----------|
| `PREFIX` | `LEFT(last_name, 3)` → "SMI" | Name matching |
| `SOUNDEX` | `SOUNDEX(last_name)` → "S530" | Phonetic similarity |
| `ZIP_CODE` | `LEFT(postal_code, 5)` | Address matching |
| `DETERMINISTIC` | Only compare if already linked | Enhance existing matches |

```sql
-- Example: Block on first 3 chars of normalized last name
SELECT 
    LEFT(UPPER(REGEXP_REPLACE(last_name, '[^A-Z]', '')), 3) AS blocking_key,
    entity_key,
    first_name,
    last_name,
    address
FROM entities
```

### 4.2 Similarity Functions

| Function | Best For | Platform Support |
|----------|----------|------------------|
| `JARO_WINKLER` | Names | Snowflake ✓, BigQuery (UDF), DuckDB (extension) |
| `LEVENSHTEIN` | Short strings | All platforms |
| `TOKEN_OVERLAP` | Addresses | SQL implementation |
| `SOUNDEX` | Phonetic names | All platforms |
| `METAPHONE` | Phonetic names | UDF required |

#### Token Overlap Implementation (SQL)

```sql
-- Token overlap for addresses
WITH tokenized AS (
    SELECT 
        entity_key,
        ARRAY_AGG(DISTINCT UPPER(token)) AS tokens
    FROM entities,
    UNNEST(SPLIT(address, ' ')) AS token
    WHERE LENGTH(token) > 2
    GROUP BY entity_key
)
SELECT 
    a.entity_key AS left_key,
    b.entity_key AS right_key,
    CARDINALITY(ARRAY_INTERSECT(a.tokens, b.tokens)) * 1.0 / 
        NULLIF(CARDINALITY(ARRAY_UNION(a.tokens, b.tokens)), 0) AS jaccard_score
FROM tokenized a, tokenized b
WHERE a.entity_key < b.entity_key
```

### 4.3 Scoring & Thresholds

**Combined Score Formula:**

```
total_score = Σ (attribute_weight[i] × similarity_score[i])
```

**Example:**

| Attribute | Weight | Value A | Value B | Similarity | Weighted |
|-----------|--------|---------|---------|------------|----------|
| first_name | 0.30 | "John" | "Jon" | 0.91 | 0.273 |
| last_name | 0.40 | "Smith" | "Smyth" | 0.89 | 0.356 |
| address | 0.30 | "123 Main St" | "123 Main Street" | 0.82 | 0.246 |
| **TOTAL** | 1.00 | | | | **0.875** |

**If min_total_score = 0.85 → Creates edge (0.875 ≥ 0.85)**

---

## 5. Integration with Deterministic Rules

### Priority & Conflict Resolution

| Scenario | Deterministic | Fuzzy | Result |
|----------|---------------|-------|--------|
| Both match | ✓ | ✓ | Keep deterministic (score=1.0) |
| Only deterministic | ✓ | ✗ | Keep deterministic |
| Only fuzzy ≥ threshold | ✗ | ✓ | Create fuzzy edge |
| Only fuzzy < threshold | ✗ | ≈ | Log to candidates table |
| Neither | ✗ | ✗ | No edge |

### Label Propagation Behavior

**Option A: Equal Treatment (simpler)**
- All edges fed to label propagation equally
- Fuzzy edges may cascade (A→B fuzzy, B→C fuzzy → A→C in same cluster)

**Option B: Weighted Propagation (more conservative)**
- Modify LP to consider edge weights
- Low-confidence edges require multiple paths to propagate
- More complex implementation

**Recommendation:** Start with Option A, add circuit breakers (max_cluster_size applies to fuzzy too).

---

## 6. Configuration Examples

### Example 1: Name + Address Matching (Retail)

```sql
INSERT INTO idr_meta.fuzzy_rule VALUES (
    'FR_NAME_ADDR',
    'Name + Address Fuzzy Match',
    TRUE,
    100,
    'LEFT(UPPER(last_name), 3)',        -- Blocking on first 3 chars
    'PREFIX',
    '["first_name", "last_name", "address_line1", "city"]',
    '{"first_name": 0.25, "last_name": 0.35, "address_line1": 0.25, "city": 0.15}',
    'JARO_WINKLER',
    0.85,                                -- 85% overall match required
    0.70,                                -- Each attribute ≥ 70%
    FALSE,                               -- Can create new links
    10,
    CURRENT_TIMESTAMP,
    NULL
);
```

### Example 2: Business Name Matching (B2B)

```sql
INSERT INTO idr_meta.fuzzy_rule VALUES (
    'FR_BUSINESS',
    'Business Name Fuzzy Match',
    TRUE,
    110,
    'LEFT(UPPER(REGEXP_REPLACE(company_name, ''[^A-Z0-9]'', '''')), 5)',
    'PREFIX',
    '["company_name"]',
    '{"company_name": 1.0}',
    'TOKEN_OVERLAP',                     -- Good for "McDonald's Corp" vs "McDonalds"
    0.80,
    0.80,
    FALSE,
    5,
    CURRENT_TIMESTAMP,
    NULL
);
```

### Example 3: Enhancement Only (Conservative)

```sql
INSERT INTO idr_meta.fuzzy_rule VALUES (
    'FR_ENHANCE',
    'Enhance Existing Clusters Only',
    TRUE,
    200,
    'resolved_id',                       -- Only compare within existing clusters
    'DETERMINISTIC',
    '["first_name", "last_name"]',
    '{"first_name": 0.5, "last_name": 0.5}',
    'JARO_WINKLER',
    0.90,                                -- Higher threshold
    0.85,
    TRUE,                                -- Only if deterministic link exists
    20,
    CURRENT_TIMESTAMP,
    NULL
);
```

---

## 7. Implementation Plan

### Phase 1: Schema & Infrastructure (2-3 days)
- [ ] Create `idr_meta.fuzzy_rule` table
- [ ] Add columns to `idr_out.identity_edges_current`
- [ ] Create `idr_out.fuzzy_candidates` table
- [ ] Add similarity UDFs per platform

### Phase 2: Blocking Implementation (2-3 days)
- [ ] Implement blocking key generation
- [ ] Add blocking index optimization
- [ ] Test blocking reduces comparisons to acceptable levels

### Phase 3: Scoring Engine (3-5 days)
- [ ] Implement similarity functions per platform
- [ ] Create weighted scoring logic
- [ ] Add per-attribute threshold checks

### Phase 4: Integration (2-3 days)
- [ ] Integrate fuzzy edges into edge building pipeline
- [ ] Ensure deterministic priority
- [ ] Add to stage metrics & run history

### Phase 5: Testing & Documentation (2-3 days)
- [ ] Unit tests for similarity functions
- [ ] Integration tests for fuzzy + deterministic
- [ ] Update documentation

**Total Estimated Effort: 2-3 weeks**

---

## 8. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Explosion of false matches | High (data quality) | Conservative thresholds, max_candidates limit |
| O(N²) performance | High (cost) | Mandatory blocking, small bucket limits |
| Fuzzy cascading | Medium (over-merging) | max_cluster_size applies to fuzzy edges |
| Platform inconsistency | Medium (different results) | Document function differences, prefer SQL-native |
| User confusion | Low (trust) | Clear match_type in output, review workflow |

---

## 9. Open Questions

1. **Should fuzzy edges be reversible?** (Allow admin to "break" a fuzzy edge)
2. **How to handle fuzzy edges in dry run?** (Preview only, or score candidates?)
3. **Should label propagation weight by score?** (More complex but more accurate)
4. **What's the maximum blocking bucket size before we skip?** (10,000? 50,000?)

---

## 10. Appendix: Platform-Specific Notes

### Snowflake
- Native `JAROWINKLER_SIMILARITY()` function
- Good UDF performance for custom functions

### BigQuery
- `LEVENSHTEIN()` available
- Jaro-Winkler requires JavaScript UDF
- Consider using ML.GENERATE_TEXT_EMBEDDING for semantic similarity

### Databricks
- Spark ML has string similarity functions
- Can leverage pandas UDFs for complex scoring

### DuckDB
- Extensions available for fuzzy matching
- Local performance excellent for testing
