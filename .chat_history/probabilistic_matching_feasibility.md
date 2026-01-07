# Probabilistic Matching - Feasibility Analysis & Implementation Plan

## Executive Summary

**Feasibility: ✅ YES** - Probabilistic matching can be added to SQL Identity Resolution as a supplementary layer on top of existing deterministic matching.

**Recommended Approach:** Phased implementation starting with fuzzy name matching.

---

## Current State vs. Probabilistic

| Aspect | Current (Deterministic) | Probabilistic |
|--------|------------------------|---------------|
| Matching | Exact on normalized values | Similarity-based |
| Confidence | Based on edge diversity | Based on match probability |
| Scale | Efficient (hash join) | O(n²) without blocking |
| False positives | Very low | Needs tuning |
| Coverage | Misses typos/variations | Catches more matches |

---

## Platform Capabilities

| Platform | Fuzzy Functions | Native Support | Feasibility |
|----------|----------------|----------------|-------------|
| **DuckDB** | `jaro_winkler_similarity()`, `levenshtein()` | ✅ Native | Excellent |
| **Snowflake** | `JAROWINKLER_SIMILARITY()`, `EDITDISTANCE()` | ✅ Native | Excellent |
| **BigQuery** | `SOUNDEX()` only, needs JS UDFs | ⚠️ Limited | Moderate |
| **Databricks** | `soundex()`, needs Spark UDFs | ⚠️ Limited | Moderate |

---

## Critical Challenge: Scale

### The Problem
- Deterministic: O(n) - hash join on exact values
- Probabilistic: O(n²) - compare all pairs

### Solution: Blocking
Only compare records in the same "block":

```sql
-- Phonetic blocking (SOUNDEX)
WHERE SOUNDEX(a.name) = SOUNDEX(b.name)

-- First-N blocking
WHERE LEFT(a.email, 3) = LEFT(b.email, 3)

-- Token blocking (any shared word)
WHERE EXISTS (SELECT 1 FROM name_tokens_a INTERSECT SELECT 1 FROM name_tokens_b)
```

**Expected reduction:** 95-99% of comparisons eliminated.

---

## Scoring Approaches

### Option 1: Simple Threshold (MVP)
```sql
SELECT a.id, b.id, 
       jaro_winkler_similarity(a.name, b.name) as score
FROM entities a, entities b
WHERE SOUNDEX(a.name) = SOUNDEX(b.name)  -- blocking
  AND jaro_winkler_similarity(a.name, b.name) > 0.85  -- threshold
```

### Option 2: Weighted Scoring (Phase 2)
```sql
SELECT a.id, b.id,
  (jaro_winkler(a.email, b.email) * 8.0 +   -- email weight
   jaro_winkler(a.name, b.name) * 3.0 +     -- name weight
   CASE WHEN a.phone = b.phone THEN 6.0 ELSE 0 END) as score
FROM blocked_pairs
HAVING score > 10.0
```

### Option 3: ML-Based (Phase 3)
- Train classifier on labeled pairs
- BigQuery ML / Snowflake ML / Databricks ML
- Most accurate, requires training data

---

## Implementation Plan

### Phase 1: Fuzzy Name Matching (MVP)
**Effort:** 2-3 weeks | **Impact:** High

1. Add `match_type` column to rules: `EXACT | JARO_WINKLER | SOUNDEX`
2. Add `threshold` column to rules (0.0-1.0)
3. Add blocking strategy configuration
4. Implement fuzzy edge building
5. Tag edges: `match_type = 'PROBABILISTIC'`
6. Include `match_score` on edges

**New Configuration:**
```csv
# idr_rules.csv
rule_id,identifier_type,match_type,threshold,weight,blocking_strategy
name_fuzzy,NAME,JARO_WINKLER,0.85,3.0,SOUNDEX
email_exact,EMAIL,EXACT,1.0,8.0,
phone_exact,PHONE,EXACT,1.0,6.0,
```

### Phase 2: Weighted Scoring
**Effort:** 2 weeks | **Impact:** Medium

1. Add field-level weights
2. Implement Fellegi-Sunter style scoring
3. Calculate composite match scores
4. Configurable match/non-match thresholds

### Phase 3: ML-Based
**Effort:** 4-6 weeks | **Impact:** Highest accuracy

1. Training data pipeline
2. Platform-specific ML implementations
3. Model versioning and deployment
4. A/B testing framework

---

## Architecture Decision

**Key Principle:** Don't replace deterministic - augment it.

```
┌─────────────────────────────────────────────────┐
│              Identity Resolution                │
├─────────────────────────────────────────────────┤
│  Deterministic Layer (existing)                 │
│  - Exact matches on EMAIL, PHONE, etc.          │
│  - High confidence, low false positives         │
├─────────────────────────────────────────────────┤
│  Probabilistic Layer (NEW - optional)           │
│  - Fuzzy matches on NAME, ADDRESS               │
│  - Medium confidence, requires threshold tuning │
│  - Requires blocking for scale                  │
└─────────────────────────────────────────────────┘
```

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Performance at scale | High | Mandatory blocking, incremental processing |
| Platform inconsistency | Medium | Test thresholds per platform |
| False positive increase | High | Conservative thresholds, human review queue |
| Threshold tuning | Medium | Provide recommended defaults, documentation |

---

## Recommendation

**Start with Phase 1:**
1. Implement on DuckDB first (best native support)
2. Add fuzzy NAME matching with SOUNDEX blocking
3. Use Jaro-Winkler with 0.85 threshold
4. Keep as optional feature (disabled by default)
5. Validate with integration tests

**Success Metrics:**
- Match coverage increase (how many more entities linked)
- Precision (% of probabilistic matches that are correct)
- Performance (time increase acceptable?)

---

## Next Steps

1. [ ] Create `idr_blocking_rules` configuration table
2. [ ] Add `match_type` and `threshold` to `idr_rules`
3. [ ] Implement platform-agnostic fuzzy matching macro
4. [ ] Create blocking strategy macro
5. [ ] Build probabilistic edge builder
6. [ ] Update edge schema with `match_type` and `match_score`
7. [ ] Integration tests with fuzzy matching scenarios
