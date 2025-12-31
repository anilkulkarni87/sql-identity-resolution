# Configuration Guide

Learn how to configure SQL Identity Resolution for your use case.

---

## Configuration Overview

IDR uses three main configuration tables:

| Table | Purpose |
|-------|---------|
| `idr_meta.source_table` | Which tables to process |
| `idr_meta.rule` | Matching rules and limits |
| `idr_meta.identifier_mapping` | Column-to-identifier mappings |

---

## Registering Source Tables

### Required Fields

```sql
INSERT INTO idr_meta.source_table (
    table_id,                    -- Unique identifier (your choice)
    table_fqn,                   -- Fully qualified table name
    entity_type,                 -- PERSON, ACCOUNT, HOUSEHOLD
    entity_key_expr,             -- SQL expression for entity key
    watermark_column,            -- Column for incremental processing
    watermark_lookback_minutes,  -- Buffer for late-arriving data
    is_active                    -- TRUE to include in processing
) VALUES (...);
```

### Examples

=== "Simple"

    ```sql
    -- Single customer table
    INSERT INTO idr_meta.source_table VALUES
      ('customers', 'crm.customers', 'PERSON', 'customer_id', 'updated_at', 0, TRUE);
    ```

=== "Multiple Sources"

    ```sql
    -- Multiple sources
    INSERT INTO idr_meta.source_table VALUES
      ('customers', 'crm.customers', 'PERSON', 'customer_id', 'updated_at', 0, TRUE),
      ('orders', 'ecom.orders', 'PERSON', 'user_id', 'order_date', 60, TRUE),
      ('support', 'helpdesk.tickets', 'PERSON', 'contact_id', 'created_at', 0, TRUE);
    ```

=== "Composite Key"

    ```sql
    -- Use SQL expression for composite key
    INSERT INTO idr_meta.source_table VALUES
      ('transactions', 
       'finance.transactions', 
       'PERSON', 
       'account_type || '':'' || account_id',  -- composite key
       'transaction_date', 
       0, 
       TRUE);
    ```

---

## Defining Matching Rules

### Rule Properties

| Column | Description |
|--------|-------------|
| `rule_id` | Unique identifier |
| `identifier_type` | EMAIL, PHONE, LOYALTY, etc. |
| `priority` | Lower = higher priority |
| `is_active` | Include in processing |
| `max_group_size` | Limit entities per identifier |

### Common Rules

```sql
INSERT INTO idr_meta.rule VALUES
  -- Email matching (highest priority)
  ('email_exact', 'EMAIL', 1, TRUE, 10000),
  
  -- Phone matching
  ('phone_exact', 'PHONE', 2, TRUE, 5000),
  
  -- Loyalty ID (should be unique)
  ('loyalty_id', 'LOYALTY', 3, TRUE, 1),
  
  -- SSN (highly sensitive, low group size)
  ('ssn_exact', 'SSN', 4, TRUE, 5);
```

### max_group_size Guidelines

| Identifier Type | Recommended max_group_size | Reason |
|-----------------|---------------------------|--------|
| SSN, Loyalty ID | 1-5 | Should be unique |
| Phone | 1000-5000 | Shared family phones |
| Email | 5000-10000 | Shared/generic emails |
| Name | 50000+ | Very common names |

---

## Mapping Identifiers

### Basic Mapping

```sql
INSERT INTO idr_meta.identifier_mapping (
    table_id,              -- FK to source_table
    identifier_type,       -- Must match a rule
    column_expr,           -- Column or SQL expression
    requires_normalization -- Apply standard normalization
) VALUES (...);
```

### Examples

=== "Direct Column"

    ```sql
    INSERT INTO idr_meta.identifier_mapping VALUES
      ('customers', 'EMAIL', 'email', TRUE),
      ('customers', 'PHONE', 'phone', TRUE);
    ```

=== "Multiple Columns"

    ```sql
    -- Map multiple email columns
    INSERT INTO idr_meta.identifier_mapping VALUES
      ('customers', 'EMAIL', 'primary_email', TRUE),
      ('customers', 'EMAIL', 'secondary_email', TRUE);
    ```

=== "SQL Expression"

    ```sql
    -- Concatenate fields
    INSERT INTO idr_meta.identifier_mapping VALUES
      ('customers', 'NAME', 'first_name || '' '' || last_name', TRUE);
    ```

---

## Identifier Exclusions

Exclude known bad identifiers from matching:

### Exact Match Exclusions

```sql
INSERT INTO idr_meta.identifier_exclusion VALUES
  ('EMAIL', 'test@test.com', FALSE, 'Generic test email'),
  ('EMAIL', 'null@null.com', FALSE, 'Null placeholder'),
  ('PHONE', '0000000000', FALSE, 'Invalid phone'),
  ('PHONE', '1111111111', FALSE, 'Invalid phone');
```

### Pattern Exclusions

```sql
INSERT INTO idr_meta.identifier_exclusion VALUES
  ('EMAIL', '%@example.com', TRUE, 'Example domain'),
  ('EMAIL', 'noreply@%', TRUE, 'No-reply addresses'),
  ('EMAIL', '%test%@%', TRUE, 'Test addresses');
```

---

## Configuration Settings

### Available Settings

```sql
INSERT INTO idr_meta.config (config_key, config_value, description) VALUES
  ('dry_run_retention_days', '7', 'Days to retain dry run results'),
  ('large_cluster_threshold', '5000', 'Warn on clusters larger than this');
```

### Reading Configuration

In your runner, use the `get_config` helper:

=== "Python"
    ```python
    threshold = int(get_config('large_cluster_threshold', '5000'))
    ```

=== "Snowflake"
    ```javascript
    var threshold = parseInt(getConfig('large_cluster_threshold', '5000'));
    ```

---

## Multi-Entity Type Setup

For different entity types (PERSON, ACCOUNT, HOUSEHOLD):

```sql
-- Person sources
INSERT INTO idr_meta.source_table VALUES
  ('contacts', 'crm.contacts', 'PERSON', 'contact_id', 'updated_at', 0, TRUE);

-- Account sources (separate entity type)
INSERT INTO idr_meta.source_table VALUES
  ('companies', 'crm.companies', 'ACCOUNT', 'company_id', 'updated_at', 0, TRUE);

-- Rules for accounts
INSERT INTO idr_meta.rule VALUES
  ('company_domain', 'DOMAIN', 1, TRUE, 100),
  ('company_duns', 'DUNS', 2, TRUE, 1);
```

!!! note
    Entities of different types are processed separately and will not be matched together.

---

## Validation Queries

### Check Configuration

```sql
-- Verify source tables
SELECT table_id, table_fqn, is_active
FROM idr_meta.source_table;

-- Verify rules
SELECT rule_id, identifier_type, max_group_size, is_active
FROM idr_meta.rule
ORDER BY priority;

-- Verify mappings
SELECT s.table_id, m.identifier_type, m.column_expr
FROM idr_meta.source_table s
JOIN idr_meta.identifier_mapping m ON s.table_id = m.table_id
WHERE s.is_active = TRUE;

-- Check for unmapped identifiers
SELECT r.identifier_type
FROM idr_meta.rule r
LEFT JOIN idr_meta.identifier_mapping m ON r.identifier_type = m.identifier_type
WHERE m.identifier_type IS NULL AND r.is_active = TRUE;
```

---

## Updating Configuration

### Adding a New Source

```sql
-- 1. Register source
INSERT INTO idr_meta.source_table VALUES
  ('new_source', 'schema.new_table', 'PERSON', 'id', 'updated_at', 0, TRUE);

-- 2. Map identifiers
INSERT INTO idr_meta.identifier_mapping VALUES
  ('new_source', 'EMAIL', 'email_address', TRUE);

-- 3. Run dry run to validate
-- python idr_run.py --dry-run
```

### Disabling a Source

```sql
UPDATE idr_meta.source_table 
SET is_active = FALSE 
WHERE table_id = 'old_source';
```

### Changing max_group_size

```sql
UPDATE idr_meta.rule 
SET max_group_size = 5000 
WHERE rule_id = 'email_exact';
```

---

## Next Steps

- [Dry Run Mode](dry-run-mode.md) - Test your configuration
- [Production Hardening](production-hardening.md) - Optimize for production
- [Troubleshooting](troubleshooting.md) - Common issues
