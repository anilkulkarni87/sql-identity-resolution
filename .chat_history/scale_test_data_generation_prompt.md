# Prompt: Generate Realistic Test Data for Identity Resolution at Scale

## Context

You are creating a Python script to generate realistic test data for testing identity resolution at scale (50M+ records) for a **global retail company**. The data will be used with [sql-identity-resolution](https://github.com/anilkulkarni87/sql-identity-resolution).

## Requirements

### Scale
- **Target**: 50 million total records across all sources
- **Unique customers**: ~10 million (overlap creates the identity resolution challenge)
- **Output format**: Parquet files (one per source table)
- **Platform**: DuckDB initially, but portable to Snowflake/BigQuery

### Sources to Generate (7 total)

| Source | Records | Description | Identity Overlap |
|--------|---------|-------------|------------------|
| 1. **CRM Master** | 8M | Core customer database | Primary source |
| 2. **E-commerce Transactions** | 15M | Online purchases | 70% overlap with CRM |
| 3. **Loyalty Program** | 6M | Rewards members | 80% overlap with CRM |
| 4. **Mobile App Users** | 10M | App registrations | 50% overlap |
| 5. **In-Store POS** | 8M | Physical store transactions | 30% overlap (many anonymous) |
| 6. **Survey Responses** | 2M | Post-purchase surveys | 60% overlap |
| 7. **Product Reviews** | 1M | Website reviews | 40% overlap |

### Identifier Types

Each source should have SOME of these identifiers (not all):

| Identifier | Description | Noise Level |
|------------|-------------|-------------|
| `email` | Primary email | Low - 5% typos/variations |
| `phone` | Mobile number | Medium - format variations |
| `loyalty_id` | Rewards card number | None - exact match |
| `device_id` | Mobile app device | N/A - device linking |
| `address_hash` | Hashed shipping address | Low |
| `payment_token` | Tokenized payment method | None |

### Realistic Data Characteristics

#### Email Variations
```python
# Same person, different representations:
"john.doe@gmail.com"
"johndoe@gmail.com"     # dot variation
"JOHN.DOE@gmail.com"    # case variation
"john.doe+shop@gmail.com"  # plus addressing
```

#### Phone Format Variations
```python
# Same number, different formats:
"+1-555-123-4567"
"5551234567"
"(555) 123-4567"
"+15551234567"
```

#### Multi-Region Support
- **Regions**: North America (40%), Europe (30%), APAC (20%), LATAM (10%)
- **Phone formats**: Country-specific
- **Email domains**: Mix of global (gmail) and regional providers
- **Names**: Culturally appropriate per region (use Faker locales)

### Schema Definitions

#### 0. Products Dimension (`products`)
```sql
CREATE TABLE products (
    product_id VARCHAR PRIMARY KEY,
    product_name VARCHAR,
    category VARCHAR,        -- 'Electronics', 'Apparel', 'Home', 'Beauty', 'Sports'
    subcategory VARCHAR,
    brand VARCHAR,
    price DECIMAL(10,2),
    margin_pct DECIMAL(5,2),
    is_private_label BOOLEAN,
    launch_date DATE
);
-- Generate ~50,000 products across categories
```

#### 1. CRM Master (`crm_customers`)
```sql
CREATE TABLE crm_customers (
    customer_id VARCHAR PRIMARY KEY,
    email VARCHAR,
    phone VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    loyalty_id VARCHAR,
    address_line1 VARCHAR,
    city VARCHAR,
    country_code VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    opt_in_email BOOLEAN,
    opt_in_sms BOOLEAN,
    customer_segment VARCHAR  -- 'VIP', 'Regular', 'New'
);
```

#### 2. E-commerce Transactions (`ecom_transactions`)
```sql
CREATE TABLE ecom_transactions (
    transaction_id VARCHAR PRIMARY KEY,
    user_id VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    product_id VARCHAR,           -- FK to products
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    order_total DECIMAL(10,2),
    currency VARCHAR,
    shipping_address_hash VARCHAR,
    payment_token VARCHAR,
    device_type VARCHAR,  -- 'mobile', 'desktop', 'tablet'
    order_date TIMESTAMP,
    promo_code VARCHAR
);
-- Note: Multiple rows per order (one per line item)
```

#### 3. Loyalty Program (`loyalty_members`)
```sql
CREATE TABLE loyalty_members (
    member_id VARCHAR PRIMARY KEY,
    loyalty_id VARCHAR UNIQUE,
    email VARCHAR,
    phone VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    tier VARCHAR,  -- 'Bronze', 'Silver', 'Gold', 'Platinum'
    points_balance INTEGER,
    enrollment_date TIMESTAMP,
    last_activity_date TIMESTAMP
);
```

#### 4. Mobile App Users (`mobile_app_users`)
```sql
CREATE TABLE mobile_app_users (
    app_user_id VARCHAR PRIMARY KEY,
    device_id VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    push_token VARCHAR,
    os_type VARCHAR,  -- 'iOS', 'Android'
    app_version VARCHAR,
    first_open_date TIMESTAMP,
    last_active_date TIMESTAMP,
    total_sessions INTEGER
);
```

#### 5. In-Store POS (`pos_transactions`)
```sql
CREATE TABLE pos_transactions (
    pos_transaction_id VARCHAR PRIMARY KEY,
    store_id VARCHAR,
    loyalty_id VARCHAR,  -- Often NULL for anonymous
    product_id VARCHAR,           -- FK to products
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    payment_token VARCHAR,
    transaction_total DECIMAL(10,2),
    transaction_date TIMESTAMP,
    payment_method VARCHAR  -- 'credit', 'debit', 'cash', 'mobile_wallet'
);
-- Note: Multiple rows per transaction (one per line item)
```

#### 6. Survey Responses (`survey_responses`)
```sql
CREATE TABLE survey_responses (
    response_id VARCHAR PRIMARY KEY,
    survey_type VARCHAR,  -- 'post_purchase', 'nps', 'product_feedback'
    email VARCHAR,
    order_id VARCHAR,  -- Links to ecom_transactions
    product_id VARCHAR,           -- For product-specific surveys
    nps_score INTEGER,  -- 0-10
    satisfaction_rating INTEGER,  -- 1-5
    feedback_text VARCHAR,
    submitted_at TIMESTAMP,
    completion_time_seconds INTEGER
);
```

#### 7. Product Reviews (`product_reviews`)
```sql
CREATE TABLE product_reviews (
    review_id VARCHAR PRIMARY KEY,
    product_id VARCHAR,           -- FK to products
    reviewer_user_id VARCHAR,  -- May link to ecom user_id
    reviewer_email VARCHAR,
    reviewer_display_name VARCHAR,
    rating INTEGER,  -- 1-5
    review_title VARCHAR,
    review_text VARCHAR,
    verified_purchase BOOLEAN,
    helpful_votes INTEGER,
    submitted_at TIMESTAMP
);
```

### Identity Graph Behavior

Generate data such that:

1. **High-value customers** (10%): Appear in 5+ sources
2. **Regular customers** (60%): Appear in 2-3 sources
3. **One-time/anonymous** (30%): Appear in only 1 source

### Edge Cases to Include

1. **Shared emails**: 
   - Family accounts (`familysmith@gmail.com` used by 3 people)
   - Company emails (`orders@company.com`)
   
2. **Shared phones**:
   - Household phones
   - Business phone numbers

3. **Problematic identifiers** (should be caught by exclusions):
   - `noreply@company.com`
   - `test@test.com`
   - `0000000000` phone
   - `000-00-0000` placeholder

4. **Giant clusters** (test max_group_size):
   - One email used by 5000+ records (corporate account)

5. **Disjoint identities**:
   - Same person, no shared identifiers (tests negative case)

### Output Structure

```
output/
├── parquet/
│   ├── crm_customers.parquet
│   ├── ecom_transactions.parquet
│   ├── loyalty_members.parquet
│   ├── mobile_app_users.parquet
│   ├── pos_transactions.parquet
│   ├── survey_responses.parquet
│   └── product_reviews.parquet
├── metadata/
│   ├── source_table_config.sql      # idr_meta.source_table inserts
│   ├── identifier_mapping_config.sql # idr_meta.identifier_mapping inserts
│   └── test_exclusions.sql          # Known bad identifiers
└── stats/
    └── generation_report.json       # Record counts, overlap stats
```

### Performance Requirements

- Generate in batches (1M records per batch)
- Use parallel processing for different sources
- Memory efficient (don't load all 50M in memory)
- Reproducible with seed parameter
- Progress logging

### Libraries to Use

- `Faker` - Realistic names, emails, addresses
- `polars` or `pandas` - Data manipulation
- `pyarrow` - Parquet output
- `tqdm` - Progress bars
- `multiprocessing` - Parallel generation

### Validation Checks

After generation, verify:
1. Total records match targets
2. Overlap percentages are approximately correct
3. All identifier formats are consistent
4. Parquet files are readable
5. No memory leaks during generation

---

## Deliverable

Create a Python script `generate_scale_test_data.py` that:

1. Accepts parameters:
   - `--scale` (default: 50M)
   - `--output-dir` (default: ./output)
   - `--seed` (for reproducibility)
   - `--workers` (parallel processes)

2. Generates all 7 source tables with realistic overlapping identities

3. Outputs ready-to-load Parquet files and IDR configuration SQL

4. Produces a report summarizing the generated data characteristics

---

## Example Usage

```bash
# Generate 50M scale test data
python generate_scale_test_data.py --scale=50M --seed=42 --workers=8

# Generate smaller test (5M for validation)
python generate_scale_test_data.py --scale=5M --seed=42

# Load into DuckDB
python -c "
import duckdb
conn = duckdb.connect('scale_test.duckdb')
conn.execute('CREATE TABLE crm_customers AS SELECT * FROM read_parquet(\"output/parquet/crm_customers.parquet\")')
# ... repeat for other tables
"
```
