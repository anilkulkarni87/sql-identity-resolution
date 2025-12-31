# Metadata Configuration Guide

How to configure sql-identity-resolution for your own source tables with custom naming conventions.

---

## Overview

The IDR system is **metadata-driven**—you describe your source tables via configuration tables, and the system dynamically generates SQL to extract identifiers and build identity graphs. No code changes required.

```
Your Source Tables → Metadata Configuration → IDR Run → Unified Identities
```

---

## Configuration Tables

| Table | Purpose |
|-------|---------|
| `idr_meta.source_table` | Register your source tables |
| `idr_meta.source` | Define trust rankings for survivorship |
| `idr_meta.rule` | Configure identifier types and matching rules |
| `idr_meta.identifier_mapping` | Map source columns to identifier types |
| `idr_meta.entity_attribute_mapping` | Map columns for golden profile |
| `idr_meta.survivorship_rule` | Define which value wins for each attribute |

---

## Step-by-Step: Configuring Your Source Tables

### Step 1: Register Source Tables

Add each source table to `idr_meta.source_table`:

```sql
INSERT INTO idr_meta.source_table (
    table_id,           -- Your unique identifier for this source
    table_fqn,          -- Fully qualified table name in YOUR database
    entity_type,        -- PERSON, ACCOUNT, HOUSEHOLD, etc.
    entity_key_expr,    -- SQL expression for unique entity key
    watermark_column,   -- Timestamp column for incremental processing
    watermark_lookback_minutes,
    is_active
) VALUES
    ('crm_customers',                    -- table_id (your choice)
     'mydb.sales.customers',             -- YOUR table name
     'PERSON',
     'customer_id',                      -- YOUR primary key column
     'last_modified_date',               -- YOUR timestamp column
     0,
     TRUE);
```

#### Column Reference

| Column | Description | Example |
|--------|-------------|---------|
| `table_id` | Unique identifier you assign | `crm_customers`, `web_signups` |
| `table_fqn` | Full table path in your database | `catalog.schema.table` |
| `entity_type` | Type of entity | `PERSON`, `ACCOUNT`, `HOUSEHOLD` |
| `entity_key_expr` | SQL expression for unique key | `customer_id`, `id::VARCHAR` |
| `watermark_column` | Timestamp for delta detection | `updated_at`, `load_ts` |
| `watermark_lookback_minutes` | Safety buffer for late-arriving data | `0`, `60` |
| `is_active` | Enable/disable this source | `TRUE`, `FALSE` |

#### Complex Entity Keys

If your table has a composite primary key:

```sql
-- Composite key example
INSERT INTO idr_meta.source_table VALUES
    ('order_lines', 
     'warehouse.orders.order_lines',
     'PERSON',
     'order_id || ''-'' || line_number',  -- Concatenate columns
     'created_at',
     0,
     TRUE);
```

---

### Step 2: Add Trust Rankings

Add entries to `idr_meta.source` to define which sources are more trusted:

```sql
INSERT INTO idr_meta.source (table_id, source_name, trust_rank, is_active) VALUES
    ('crm_customers', 'CRM Master Data', 1, TRUE),    -- Most trusted
    ('web_signups', 'Web Registrations', 2, TRUE),
    ('mobile_app', 'Mobile App Users', 3, TRUE),
    ('third_party', 'External Data', 10, TRUE);       -- Least trusted
```

> **Note:** Lower `trust_rank` = more trusted. Used in golden profile survivorship.

---

### Step 3: Map Identifiers

Map your source columns to identifier types in `idr_meta.identifier_mapping`:

```sql
INSERT INTO idr_meta.identifier_mapping 
    (table_id, identifier_type, identifier_value_expr, is_hashed) 
VALUES
    -- CRM has email and phone
    ('crm_customers', 'EMAIL', 'email_address', FALSE),
    ('crm_customers', 'PHONE', 'mobile_phone', FALSE),
    ('crm_customers', 'LOYALTY_ID', 'loyalty_card_number', FALSE),
    
    -- Web signups might have different column names
    ('web_signups', 'EMAIL', 'user_email', FALSE),
    ('web_signups', 'PHONE', 'contact_phone', FALSE),
    
    -- Third-party data has hashed emails
    ('third_party', 'EMAIL', 'email_sha256', TRUE);
```

#### Column Reference

| Column | Description | Example |
|--------|-------------|---------|
| `table_id` | Must match `source_table.table_id` | `crm_customers` |
| `identifier_type` | Must match a `rule.identifier_type` | `EMAIL`, `PHONE`, `SSN` |
| `identifier_value_expr` | SQL expression for the value | `email_address`, `UPPER(email)` |
| `is_hashed` | Is the value already hashed? | `FALSE`, `TRUE` |

#### Using SQL Expressions

You can use SQL expressions, not just column names:

```sql
-- Clean phone number on extraction
('crm_customers', 'PHONE', 'REGEXP_REPLACE(phone, ''[^0-9]'', '''')', FALSE),

-- Combine first.last as identifier
('crm_customers', 'NAME_KEY', 'LOWER(first_name || ''.'' || last_name)', FALSE),

-- Handle NULL-like values
('web_signups', 'EMAIL', 'NULLIF(email, ''N/A'')', FALSE)
```

---

### Step 4: Configure Rules (if needed)

Default rules exist for EMAIL and PHONE. Add custom rules for new identifier types:

```sql
INSERT INTO idr_meta.rule (
    rule_id, rule_name, is_active, priority, 
    identifier_type, canonicalize, allow_hashed, require_non_null
) VALUES
    ('R_LOYALTY_EXACT', 'Loyalty ID Match', TRUE, 3, 
     'LOYALTY_ID', 'NONE', FALSE, TRUE),
     
    ('R_SSN_EXACT', 'SSN Exact Match', TRUE, 4, 
     'SSN', 'NONE', FALSE, TRUE);
```

#### Canonicalization Options

| Option | Effect | When to Use |
|--------|--------|-------------|
| `LOWERCASE` | `John@Gmail.COM` → `john@gmail.com` | Email, usernames |
| `UPPERCASE` | `john` → `JOHN` | Case-insensitive IDs |
| `NONE` | No transformation | Exact match IDs |
| `TRIM` | Remove whitespace | All identifiers |
| `PHONE_DIGITS` | `+1 (555) 123-4567` → `15551234567` | Phone numbers |

---

### Step 5: Map Attributes for Golden Profile

Map columns to be included in the golden profile:

```sql
INSERT INTO idr_meta.entity_attribute_mapping 
    (table_id, attribute_name, attribute_expr) 
VALUES
    -- CRM: use their column names
    ('crm_customers', 'email_raw', 'email_address'),
    ('crm_customers', 'phone_raw', 'mobile_phone'),
    ('crm_customers', 'first_name', 'first_name'),
    ('crm_customers', 'last_name', 'surname'),                -- Different column name
    ('crm_customers', 'record_updated_at', 'last_modified_date'),
    
    -- Web: different column names
    ('web_signups', 'email_raw', 'user_email'),
    ('web_signups', 'first_name', 'given_name'),
    ('web_signups', 'last_name', 'family_name'),
    ('web_signups', 'record_updated_at', 'signup_date');
```

> **Important:** `attribute_name` must be consistent across sources (e.g., always `email_raw`), but `attribute_expr` can vary.

---

### Step 6: Set Survivorship Rules

Define how to pick the "best" value for each attribute:

```sql
INSERT INTO idr_meta.survivorship_rule (attribute_name, strategy) VALUES
    ('email_primary', 'MOST_RECENT'),       -- Latest email wins
    ('phone_primary', 'SOURCE_PRIORITY'),   -- CRM phone wins over web
    ('first_name', 'MOST_RECENT'),
    ('last_name', 'SOURCE_PRIORITY');
```

| Strategy | Logic |
|----------|-------|
| `MOST_RECENT` | Value with latest `record_updated_at` |
| `SOURCE_PRIORITY` | Value from highest trust_rank source |
| `SOURCE_PRIORITY + MOST_RECENT` | Trust rank first, then recency |

---

## Complete Example

Suppose you have these tables with non-standard names:

| Your Table | Primary Key | Email Column | Phone Column | Timestamp |
|------------|-------------|--------------|--------------|-----------|
| `erp.client_master` | `client_no` | `email_addr` | `ph_number` | `mod_date` |
| `web.registrations` | `reg_id` | `e_mail` | `cell` | `reg_ts` |
| `pos.receipts` | `txn_id` | `cust_email` | NULL | `txn_timestamp` |

**Configuration:**

```sql
-- 1. Register sources
INSERT INTO idr_meta.source_table VALUES
    ('erp_clients', 'erp.client_master', 'PERSON', 'client_no', 'mod_date', 0, TRUE),
    ('web_regs', 'web.registrations', 'PERSON', 'reg_id', 'reg_ts', 0, TRUE),
    ('pos_txns', 'pos.receipts', 'PERSON', 'txn_id', 'txn_timestamp', 0, TRUE);

-- 2. Trust rankings
INSERT INTO idr_meta.source VALUES
    ('erp_clients', 'ERP Master', 1, TRUE),
    ('web_regs', 'Web Signups', 2, TRUE),
    ('pos_txns', 'POS Transactions', 3, TRUE);

-- 3. Map identifiers (using YOUR column names)
INSERT INTO idr_meta.identifier_mapping VALUES
    ('erp_clients', 'EMAIL', 'email_addr', FALSE),
    ('erp_clients', 'PHONE', 'ph_number', FALSE),
    ('web_regs', 'EMAIL', 'e_mail', FALSE),
    ('web_regs', 'PHONE', 'cell', FALSE),
    ('pos_txns', 'EMAIL', 'cust_email', FALSE);
    -- Note: pos_txns has no phone

-- 4. Map attributes for golden profile
INSERT INTO idr_meta.entity_attribute_mapping VALUES
    ('erp_clients', 'email_raw', 'email_addr'),
    ('erp_clients', 'phone_raw', 'ph_number'),
    ('erp_clients', 'record_updated_at', 'mod_date'),
    ('web_regs', 'email_raw', 'e_mail'),
    ('web_regs', 'phone_raw', 'cell'),
    ('web_regs', 'record_updated_at', 'reg_ts'),
    ('pos_txns', 'email_raw', 'cust_email'),
    ('pos_txns', 'record_updated_at', 'txn_timestamp');

-- 5. Run IDR
-- Databricks: IDR_Run.py with RUN_MODE=FULL
-- Snowflake: CALL idr_run('FULL', 30);
-- DuckDB: python idr_run.py --db=mydb.duckdb --run-mode=FULL
```

---

## Validation

After configuring metadata, verify before running:

```sql
-- Check sources registered
SELECT table_id, table_fqn, is_active FROM idr_meta.source_table;

-- Check identifier mappings
SELECT st.table_id, im.identifier_type, im.identifier_value_expr
FROM idr_meta.source_table st
LEFT JOIN idr_meta.identifier_mapping im ON im.table_id = st.table_id
ORDER BY st.table_id, im.identifier_type;

-- Verify rules exist for your identifier types
SELECT DISTINCT im.identifier_type, r.rule_id
FROM idr_meta.identifier_mapping im
LEFT JOIN idr_meta.rule r ON r.identifier_type = im.identifier_type AND r.is_active = TRUE
WHERE r.rule_id IS NULL;
-- Result should be empty (all identifier types have rules)
```

---

## Common Mistakes

| Mistake | Symptom | Fix |
|---------|---------|-----|
| `table_fqn` doesn't exist | "Table not found" error | Verify exact table path |
| `identifier_type` not in rules | No edges created | Add rule or use existing type |
| Different `attribute_name` across sources | Inconsistent golden profile | Use same name, vary `attribute_expr` |
| Missing `record_updated_at` mapping | Survivorship fails | Map a timestamp column |

---

## See Also

- [Architecture](architecture.md) — How metadata drives the pipeline
- [Runbook](runbook.md) — Adding new sources at runtime
- [metadata_samples/](../metadata_samples/) — Example CSV configurations
