# Dry Run Your Identity Resolution Before Committing

*Preview changes safely in production*

**Tags:** `identity-resolution` `dry-run` `data-quality` `testing` `production`

**Reading time:** 6 minutes

---

> **TL;DR:** Dry run mode runs all processing but doesn't commit changes. Review proposed clusters, catch giant merges from shared identifiers, and add exclusions before going live. Any change to identity resolution logic can have cascading effects—merging clusters that shouldn't be merged, splitting existing relationships, or creating unexpected mega-clusters.

I built dry-run mode into sql-identity-resolution specifically to address this. In this post, I'll explain how it works and how to use it effectively.

## Why Dry Run Matters

Consider this scenario:

1. Marketing adds a new data source
2. That source has a shared email (`info@company.com`) in 50,000 records
3. Without testing, those 50,000 records merge into one cluster
4. Downstream systems now see 50,000 customers as one person

This is a real failure mode. Dry run prevents it.

## How Dry Run Works

When you run with `DRY_RUN = TRUE`:

1. **All processing happens normally** - Identifiers extracted, edges built, clusters computed
2. **No production tables are modified** - Changes stay in work tables
3. **Diff is computed** - Compare proposed state vs current state
4. **Results logged to audit tables** - `dry_run_results`, `dry_run_summary`

```sql
-- Snowflake
CALL idr_run('FULL', 30, TRUE);  -- TRUE = dry run

-- Or Python runners
python idr_run.py --project=X --run-mode=FULL --dry-run
```

## Analyzing Dry Run Output

### Summary Table

```sql
SELECT * FROM idr_out.dry_run_summary 
WHERE run_id = (SELECT MAX(run_id) FROM idr_out.dry_run_summary);
```

Key metrics:

| Metric | Meaning |
|--------|---------|
| `new_entities` | Records appearing for first time |
| `moved_entities` | Records changing clusters |
| `unchanged_entities` | Records staying in same cluster |
| `largest_proposed_cluster` | Size of biggest cluster |
| `groups_would_skip` | Identifier groups exceeding max_group_size |

### Change Type Distribution

```sql
SELECT 
  change_type, 
  COUNT(*) as count
FROM idr_out.dry_run_results
WHERE run_id = 'your_run_id'
GROUP BY change_type;
```

Expected distribution for incremental runs:
- **UNCHANGED**: 90-99%
- **NEW**: 1-10%
- **MOVED**: 0-1%

If MOVED is high, investigate.

### Investigating Moves

```sql
SELECT 
  entity_key,
  current_resolved_id,
  proposed_resolved_id,
  current_cluster_size,
  proposed_cluster_size
FROM idr_out.dry_run_results
WHERE change_type = 'MOVED'
  AND run_id = 'your_run_id'
ORDER BY proposed_cluster_size DESC
LIMIT 100;
```

## Decision Workflow

Based on my experience, here's the workflow I recommend:

```
                    Run Dry Run
                        │
                        ▼
              Check largest_proposed_cluster
                        │
         ┌──────────────┼──────────────┐
         │              │              │
      < 1000         1000-5000       > 5000
         │              │              │
         ▼              ▼              ▼
     Looks OK      Investigate     Stop & Fix
         │              │              │
         ▼              ▼              ▼
   Commit Run    Check if expected  Add to exclusion
                    or problem        list
```

## Common Issues Found in Dry Run

| Finding | Cause | Action |
|---------|-------|--------|
| Giant cluster (10,000+) | Shared identifier | Add to exclusion list |
| Many moves from one cluster | New linking identifier | Verify it's correct |
| Zero new records | Source not active | Check is_active flag |
| Very high move % | Major matching rule change | Review rule config |

## Adding to Exclusion List

If you find a problematic identifier:

```sql
INSERT INTO idr_meta.identifier_exclusion 
(identifier_type, identifier_value_pattern, match_type, reason) 
VALUES
  ('EMAIL', 'noreply@%', 'LIKE', 'System email'),
  ('EMAIL', 'test@test.com', 'EXACT', 'Test data'),
  ('PHONE', '0000000000', 'EXACT', 'Default value');
```

Re-run dry run to confirm the fix.

## My Approach

I recommend this cadence:

1. **Always dry run first** - Even for small changes
2. **Review before committing** - Check summary metrics
3. **Keep audit history** - Don't delete dry_run_results immediately
4. **Automate alerts** - Flag if largest cluster exceeds threshold

```sql
-- Alert query for monitoring
SELECT 
  run_id,
  largest_proposed_cluster,
  groups_would_skip
FROM idr_out.dry_run_summary
WHERE largest_proposed_cluster > 5000
   OR groups_would_skip > 10;
```

## Integration with CDP Evaluation

If you're evaluating CDP solutions, dry-run capability should be on your checklist. I've included this in the evaluation criteria on [CDP Atlas](https://cdpatlas.vercel.app/evaluation) - a tool I built for CDP assessment.

Key questions to ask vendors:
- Can I preview identity resolution changes before commit?
- Are changes auditable and reversible?
- How do I handle mistaken merges?

## Next Steps

In the next post, I'll compare open-source identity resolution tools—Zingg, Dedupe, and sql-identity-resolution—to help you choose the right approach.

---

*This is post 6 of 8 in the warehouse-native identity resolution series.*

**Next:** [Comparing Open Source Identity Resolution Tools](07_comparing_open_source_tools.md)

If you found this helpful:
- ⭐ Star the [GitHub repo](https://github.com/anilkulkarni87/sql-identity-resolution)
- 📖 Check out [CDP Atlas](https://cdpatlas.vercel.app/) for CDP evaluation tools
- 💬 Questions? [Open an issue](https://github.com/anilkulkarni87/sql-identity-resolution/issues)
