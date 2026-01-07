# Building Customer 360 in Snowflake with SQL

*A practical walkthrough using sql-identity-resolution*

---

In the first two posts, I introduced warehouse-native identity resolution and compared deterministic vs probabilistic matching. Now let's get practical—building a Customer 360 view in Snowflake from scratch.

The documentation for sql-identity-resolution can be found [here](https://anilkulkarni87.github.io/sql-identity-resolution/). Below are the generalized steps.

## Prerequisites

- Snowflake account with ACCOUNTADMIN or schema creation privileges
- Source tables with customer identifiers (email, phone, etc.)
- ~30 minutes for initial setup

## Step 1: Install the Schema

Clone the repository and run the DDL script:

```bash
git clone https://github.com/anilkulkarni87/sql-identity-resolution.git
cd sql-identity-resolution
```

In Snowflake:

```sql
-- Run the DDL script
-- This creates: idr_meta, idr_work, idr_out schemas
-- And all required tables
```

The script creates three schemas:

| Schema | Purpose |
|--------|---------|
| `idr_meta` | Configuration tables |
| `idr_work` | Temporary processing tables |
| `idr_out` | Output tables (clusters, profiles) |

## Step 2: Register Source Tables

Add your source tables to the metadata:

```sql
INSERT INTO idr_meta.source_table 
(table_id, table_fqn, entity_type, entity_key_expr, watermark_column, watermark_lookback_minutes, is_active) 
VALUES
  ('crm',     'mydb.sales.customers',     'PERSON', 'customer_id', 'updated_at', 0, TRUE),
  ('web',     'mydb.web.signups',         'PERSON', 'user_id',     'created_at', 0, TRUE),
  ('loyalty', 'mydb.loyalty.members',     'PERSON', 'member_id',   'modified_ts', 0, TRUE);
```

## Step 3: Configure Identifier Mappings

Map column names to identifier types:

```sql
INSERT INTO idr_meta.identifier_mapping 
(table_id, identifier_type, identifier_value_expr, is_hashed) 
VALUES
  -- CRM
  ('crm', 'EMAIL', 'email',           FALSE),
  ('crm', 'PHONE', 'mobile_phone',    FALSE),
  
  -- Web signups
  ('web', 'EMAIL', 'user_email',      FALSE),
  
  -- Loyalty
  ('loyalty', 'EMAIL', 'email_address', FALSE),
  ('loyalty', 'PHONE', 'phone',         FALSE),
  ('loyalty', 'LOYALTY_ID', 'member_id', FALSE);
```

Notice different column names map to the same `identifier_type`. This is the key flexibility—your sources don't need consistent naming.

## Step 4: Add Matching Rules

Default rules for EMAIL and PHONE should already exist. Add custom rules as needed:

```sql
INSERT INTO idr_meta.rule 
(rule_id, rule_name, is_active, priority, identifier_type, canonicalize, allow_hashed, require_non_null, max_group_size) 
VALUES
  ('R_LOYALTY', 'Loyalty ID Match', TRUE, 3, 'LOYALTY_ID', 'NONE', FALSE, TRUE, 10000);
```

## Step 5: Run Dry Run First

Always test with dry run before committing:

```sql
CALL idr_run('FULL', 30, TRUE);  -- TRUE = dry run mode
```

Review the results:

```sql
-- Summary of changes
SELECT * FROM idr_out.dry_run_summary ORDER BY run_id DESC LIMIT 1;

-- Detailed changes
SELECT change_type, COUNT(*) 
FROM idr_out.dry_run_results 
WHERE run_id = (SELECT MAX(run_id) FROM idr_out.dry_run_summary)
GROUP BY change_type;
```

## Step 6: Execute the Run

If dry run looks good:

```sql
CALL idr_run('FULL', 30, FALSE);  -- FALSE = commit changes
```

## Step 7: Query Results

Now you can query unified identities:

```sql
-- How many unique customers do we have?
SELECT COUNT(DISTINCT resolved_id) 
FROM idr_out.identity_resolved_membership_current;

-- Cluster size distribution
SELECT cluster_size, COUNT(*) as num_clusters
FROM idr_out.identity_clusters_current
GROUP BY cluster_size
ORDER BY cluster_size;

-- Golden profiles
SELECT * FROM idr_out.golden_profile_current LIMIT 100;

-- Find all records for a specific customer
SELECT * 
FROM idr_out.identity_resolved_membership_current 
WHERE resolved_id = 'crm:12345';
```

## Performance Results

In my testing:

| Records | Snowflake Warehouse | Time | Cost |
|---------|---------------------|------|------|
| 100K | X-Small | ~15s | ~$0.02 |
| 1M | X-Small | ~45s | ~$0.05 |
| 10M | X-Small | ~168s | ~$0.50 |

## Common Issues and Solutions

| Issue | Solution |
|-------|----------|
| No matches found | Verify identifier mappings point to correct columns |
| Giant clusters (1000+) | Use `max_group_size` in rules or add to exclusion list |
| Missing source tables | Ensure `is_active = TRUE` in source_table |
| Slow performance | Check for extremely common identifiers (shared emails) |

## My Approach for Incremental Processing

After initial full run, switch to incremental mode:

```sql
CALL idr_run('INCR', 30, FALSE);  -- Only process new/changed records
```

The system uses watermark columns to track what's been processed. This significantly reduces processing time for ongoing runs.

## Next Steps

In the next post, I'll cover the hidden costs of traditional CDPs and the cost comparison with this approach.

---

*This is post 3 of 8 in the warehouse-native identity resolution series. Code and documentation available at [GitHub](https://github.com/anilkulkarni87/sql-identity-resolution).*
