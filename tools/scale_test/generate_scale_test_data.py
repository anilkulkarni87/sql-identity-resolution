#!/usr/bin/env python3
"""
Scale Test Data Generator for SQL Identity Resolution
======================================================

Generates 50M+ realistic retail customer records across 7 sources
with overlapping identities for testing identity resolution at scale.

Usage:
    # On Databricks
    spark-submit generate_scale_test_data.py

    # With custom config
    spark-submit generate_scale_test_data.py --output /mnt/data/idr_test --scale 50M

Sources Generated:
    1. CRM Master (8M) - Primary customer database
    2. E-commerce Transactions (15M) - Online purchases
    3. Loyalty Program (6M) - Rewards members
    4. Mobile App Users (10M) - App registrations
    5. In-Store POS (8M) - Physical store transactions
    6. Survey Responses (2M) - Post-purchase surveys
    7. Product Reviews (1M) - Website reviews
    + Products Dimension (50K) - Product catalog

Author: Anil Kulkarni
License: Apache 2.0
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import argparse
from datetime import datetime

# =============================================================================
# CONFIGURATION
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(description='Generate scale test data for IDR')
    parser.add_argument('--output', default='/mnt/idr_test_data/parquet/',
                        help='Output path for parquet files')
    parser.add_argument('--scale', default='50M', choices=['5M', '10M', '50M', '100M'],
                        help='Data scale to generate')
    parser.add_argument('--seed', type=int, default=42,
                        help='Random seed for reproducibility')
    parser.add_argument('--format', default='parquet', choices=['parquet', 'delta'],
                        help='Output format')
    return parser.parse_args()

# Scale configurations
SCALE_CONFIG = {
    '5M': {
        'unique_customers': 1_000_000,
        'products': 10_000,
        'crm': 800_000,
        'ecom': 1_500_000,
        'loyalty': 600_000,
        'mobile': 1_000_000,
        'pos': 800_000,
        'surveys': 200_000,
        'reviews': 100_000
    },
    '10M': {
        'unique_customers': 2_000_000,
        'products': 20_000,
        'crm': 1_600_000,
        'ecom': 3_000_000,
        'loyalty': 1_200_000,
        'mobile': 2_000_000,
        'pos': 1_600_000,
        'surveys': 400_000,
        'reviews': 200_000
    },
    '50M': {
        'unique_customers': 10_000_000,
        'products': 50_000,
        'crm': 8_000_000,
        'ecom': 15_000_000,
        'loyalty': 6_000_000,
        'mobile': 10_000_000,
        'pos': 8_000_000,
        'surveys': 2_000_000,
        'reviews': 1_000_000
    },
    '100M': {
        'unique_customers': 20_000_000,
        'products': 100_000,
        'crm': 16_000_000,
        'ecom': 30_000_000,
        'loyalty': 12_000_000,
        'mobile': 20_000_000,
        'pos': 16_000_000,
        'surveys': 4_000_000,
        'reviews': 2_000_000
    }
}

# Categories for products
CATEGORIES = ['Electronics', 'Apparel', 'Home', 'Beauty', 'Sports', 'Grocery', 'Toys']
SUBCATEGORIES = {
    'Electronics': ['Phones', 'Laptops', 'TVs', 'Audio', 'Cameras'],
    'Apparel': ['Mens', 'Womens', 'Kids', 'Shoes', 'Accessories'],
    'Home': ['Furniture', 'Kitchen', 'Bedding', 'Decor', 'Garden'],
    'Beauty': ['Skincare', 'Makeup', 'Haircare', 'Fragrance'],
    'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Water Sports'],
    'Grocery': ['Fresh', 'Pantry', 'Beverages', 'Snacks'],
    'Toys': ['Games', 'Dolls', 'Building', 'Educational']
}
BRANDS = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE', 'BrandF', 'StoreLabel']
COUNTRIES = ['US', 'UK', 'DE', 'FR', 'IN', 'JP', 'AU', 'CA', 'MX', 'BR']
LOYALTY_TIERS = ['Bronze', 'Silver', 'Gold', 'Platinum']
CUSTOMER_SEGMENTS = ['VIP', 'Regular', 'New', 'At-Risk', 'Churned']
DEVICE_TYPES = ['mobile', 'desktop', 'tablet']
PAYMENT_METHODS = ['credit', 'debit', 'cash', 'mobile_wallet', 'gift_card']
OS_TYPES = ['iOS', 'Android']
SURVEY_TYPES = ['post_purchase', 'nps', 'product_feedback', 'support_followup']

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def create_array_literal(items):
    """Create a Spark array literal from Python list."""
    return F.array(*[F.lit(x) for x in items])

def random_element(items, seed_offset=0):
    """Select random element from array based on rand()."""
    arr = create_array_literal(items)
    idx = (F.rand(SEED + seed_offset) * len(items)).cast("int")
    return F.element_at(arr, idx + 1)  # element_at is 1-indexed

def apply_email_noise(df, email_col="email", noise_rate=0.05):
    """
    Apply realistic noise to email addresses.
    - 2% uppercase variation
    - 1% plus addressing (user+tag@domain.com)
    - 1% dot variation in gmail (john.doe vs johndoe)
    - 1% typos
    """
    return df.withColumn(email_col,
        F.when(F.rand() < 0.02, F.upper(F.col(email_col)))
         .when(F.rand() < 0.01, F.regexp_replace(F.col(email_col), "@", "+shop@"))
         .when((F.rand() < 0.01) & F.col(email_col).contains("gmail"),
               F.regexp_replace(F.col(email_col), r"\.(?=.*@gmail)", ""))
         .otherwise(F.col(email_col))
    )

def apply_phone_noise(df, phone_col="phone", noise_rate=0.10):
    """
    Apply realistic noise to phone numbers.
    - 5% add country code prefix
    - 3% add dashes
    - 2% add parentheses
    """
    return df.withColumn(phone_col,
        F.when(F.rand() < 0.05, 
               F.concat(F.lit("+1"), F.col(phone_col)))
         .when(F.rand() < 0.03,
               F.concat(F.substring(phone_col, 1, 3), F.lit("-"),
                       F.substring(phone_col, 4, 3), F.lit("-"),
                       F.substring(phone_col, 7, 4)))
         .when(F.rand() < 0.02,
               F.concat(F.lit("("), F.substring(phone_col, 1, 3), F.lit(") "),
                       F.substring(phone_col, 4, 3), F.lit("-"),
                       F.substring(phone_col, 7, 4)))
         .otherwise(F.col(phone_col))
    )

def generate_timestamp(days_back=365, seed_offset=0):
    """Generate random timestamp within the last N days."""
    return F.expr(f"current_timestamp() - make_interval(0, 0, 0, cast(rand({SEED + seed_offset}) * {days_back} as int), 0, 0, 0)")

# =============================================================================
# MAIN GENERATION LOGIC
# =============================================================================

def main():
    global SEED
    args = parse_args()
    SEED = args.seed
    config = SCALE_CONFIG[args.scale]
    
    print(f"""
    ╔══════════════════════════════════════════════════════════╗
    ║  SQL Identity Resolution - Scale Test Data Generator     ║
    ║                                                          ║
    ║  Scale: {args.scale:>6}                                        ║
    ║  Unique Customers: {config['unique_customers']:>12,}                   ║
    ║  Output: {args.output:<45} ║
    ║  Format: {args.format:<45} ║
    ║  Seed: {args.seed:<47} ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName(f"IDR_Scale_Generator_{args.scale}") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # =========================================================================
    # 0. GENERATE PRODUCTS DIMENSION
    # =========================================================================
    print("Generating Products dimension...")
    
    products = spark.range(0, config['products']).select(
        F.concat(F.lit("PROD-"), F.format_string("%06d", F.col("id"))).alias("product_id"),
        F.concat(F.lit("Product "), F.col("id")).alias("product_name"),
        random_element(CATEGORIES, 1).alias("category"),
        random_element(BRANDS, 2).alias("brand"),
        (F.rand(SEED + 3) * 490 + 10).cast("decimal(10,2)").alias("price"),
        (F.rand(SEED + 4) * 40 + 10).cast("decimal(5,2)").alias("margin_pct"),
        (F.rand(SEED + 5) < 0.15).alias("is_private_label"),
        F.date_sub(F.current_date(), (F.rand(SEED + 6) * 1000).cast("int")).alias("launch_date")
    ).cache()
    
    products_count = products.count()
    print(f"  → Products: {products_count:,}")
    
    # =========================================================================
    # 1. GENERATE MASTER IDENTITY POOL
    # =========================================================================
    print("Generating Master Identity Pool (anchor identities)...")
    
    master_pool = spark.range(0, config['unique_customers']).select(
        F.col("id").alias("m_id"),
        # Core identifiers
        F.concat(F.lit("user"), F.col("id"), F.lit("@"),
                random_element(['gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com'], 10)
        ).alias("true_email"),
        # 10-digit phone number
        F.concat(
            F.lit("5"),  # Start with 5 for realism
            F.format_string("%09d", (F.rand(SEED + 11) * 1000000000).cast("long"))
        ).alias("true_phone"),
        # Loyalty ID
        F.concat(F.lit("L-"), F.format_string("%010d", F.col("id"))).alias("loyalty_id"),
        # Demographics
        F.concat(F.lit("First"), F.col("id") % 10000).alias("first_name"),
        F.concat(F.lit("Last"), F.col("id") % 10000).alias("last_name"),
        # Address components
        F.format_string("%05d", (F.rand(SEED + 12) * 99999).cast("int")).alias("zip"),
        random_element(COUNTRIES, 13).alias("country_code"),
        # Device ID for mobile
        F.concat(F.lit("DEV-"), F.sha2(F.col("id").cast("string"), 256).substr(1, 16)).alias("device_id"),
        # Payment token (consistent per customer)
        F.concat(F.lit("TKN-"), F.sha2(F.col("id").cast("string"), 256).substr(17, 16)).alias("payment_token")
    ).cache()
    
    master_count = master_pool.count()
    print(f"  → Master Pool: {master_count:,}")
    
    # =========================================================================
    # 2. GENERATE CRM MASTER (80% of customers)
    # =========================================================================
    print("Generating CRM Master...")
    
    crm_sample_rate = config['crm'] / config['unique_customers']
    crm = master_pool.sample(False, crm_sample_rate, SEED + 100).select(
        F.concat(F.lit("CRM-"), F.col("m_id")).alias("customer_id"),
        F.col("true_email").alias("email"),
        F.col("true_phone").alias("phone"),
        "first_name", "last_name", "loyalty_id",
        F.concat(F.col("zip"), F.lit(" Main Street")).alias("address_line1"),
        F.lit("Anytown").alias("city"),
        "country_code",
        generate_timestamp(730, 20).alias("created_at"),  # Last 2 years
        generate_timestamp(90, 21).alias("updated_at"),   # Last 90 days
        (F.rand(SEED + 22) > 0.3).alias("opt_in_email"),
        (F.rand(SEED + 23) > 0.5).alias("opt_in_sms"),
        random_element(CUSTOMER_SEGMENTS, 24).alias("customer_segment")
    )
    crm = apply_email_noise(crm)
    crm = apply_phone_noise(crm).limit(config['crm'])
    
    crm_count = crm.count()
    print(f"  → CRM: {crm_count:,}")
    
    # =========================================================================
    # 3. GENERATE E-COMMERCE TRANSACTIONS (line-item level)
    # =========================================================================
    print("Generating E-commerce Transactions...")
    
    ecom_sample_rate = config['ecom'] / config['unique_customers']
    ecom = master_pool.sample(True, ecom_sample_rate, SEED + 200).select(
        F.expr("uuid()").alias("transaction_id"),
        F.concat(F.lit("ECOM-"), F.col("m_id")).alias("user_id"),
        F.col("true_email").alias("email"),
        F.col("true_phone").alias("phone"),
        # Random product assignment (avoid expensive cross join)
        F.concat(F.lit("PROD-"), 
                F.format_string("%06d", (F.rand(SEED + 201) * config['products']).cast("int"))
        ).alias("product_id"),
        (F.rand(SEED + 202) * 4 + 1).cast("int").alias("quantity"),
        (F.rand(SEED + 203) * 200 + 10).cast("decimal(10,2)").alias("unit_price"),
        random_element(['USD', 'EUR', 'GBP', 'CAD', 'AUD'], 204).alias("currency"),
        F.sha2(F.concat(F.col("zip"), F.col("country_code")), 256).alias("shipping_address_hash"),
        "payment_token",
        random_element(DEVICE_TYPES, 205).alias("device_type"),
        generate_timestamp(365, 206).alias("order_date"),
        F.when(F.rand(SEED + 207) < 0.2, 
               F.concat(F.lit("PROMO"), (F.rand() * 100).cast("int"))
        ).otherwise(F.lit(None)).alias("promo_code")
    ).withColumn("order_total", F.col("quantity") * F.col("unit_price"))
    
    ecom = apply_email_noise(ecom)
    ecom = apply_phone_noise(ecom).limit(config['ecom'])
    
    ecom_count = ecom.count()
    print(f"  → E-commerce: {ecom_count:,}")
    
    # =========================================================================
    # 4. GENERATE LOYALTY PROGRAM (60% overlap with CRM)
    # =========================================================================
    print("Generating Loyalty Program...")
    
    loyalty_sample_rate = config['loyalty'] / config['unique_customers']
    loyalty = master_pool.sample(False, loyalty_sample_rate, SEED + 300).select(
        F.concat(F.lit("LYL-"), F.expr("uuid()").substr(1, 8)).alias("member_id"),
        "loyalty_id",
        F.col("true_email").alias("email"),
        F.col("true_phone").alias("phone"),
        "first_name", "last_name",
        random_element(LOYALTY_TIERS, 301).alias("tier"),
        (F.rand(SEED + 302) * 50000).cast("int").alias("points_balance"),
        generate_timestamp(1095, 303).alias("enrollment_date"),  # Last 3 years
        generate_timestamp(30, 304).alias("last_activity_date")
    )
    loyalty = apply_email_noise(loyalty).limit(config['loyalty'])
    
    loyalty_count = loyalty.count()
    print(f"  → Loyalty: {loyalty_count:,}")
    
    # =========================================================================
    # 5. GENERATE MOBILE APP USERS (50% overlap)
    # =========================================================================
    print("Generating Mobile App Users...")
    
    mobile_sample_rate = config['mobile'] / config['unique_customers']
    mobile = master_pool.sample(False, mobile_sample_rate, SEED + 400).select(
        F.concat(F.lit("APP-"), F.expr("uuid()").substr(1, 12)).alias("app_user_id"),
        "device_id",
        F.col("true_email").alias("email"),
        F.col("true_phone").alias("phone"),
        F.concat(F.lit("PUSH-"), F.sha2(F.col("device_id"), 256).substr(1, 32)).alias("push_token"),
        random_element(OS_TYPES, 401).alias("os_type"),
        F.concat(F.lit("3."), (F.rand(SEED + 402) * 20).cast("int")).alias("app_version"),
        generate_timestamp(365, 403).alias("first_open_date"),
        generate_timestamp(14, 404).alias("last_active_date"),
        (F.rand(SEED + 405) * 200 + 1).cast("int").alias("total_sessions")
    )
    mobile = apply_email_noise(mobile)
    mobile = apply_phone_noise(mobile).limit(config['mobile'])
    
    mobile_count = mobile.count()
    print(f"  → Mobile App: {mobile_count:,}")
    
    # =========================================================================
    # 6. GENERATE IN-STORE POS (30% identified, 70% anonymous)
    # =========================================================================
    print("Generating In-Store POS Transactions...")
    
    pos_sample_rate = config['pos'] / config['unique_customers']
    pos = master_pool.sample(True, pos_sample_rate, SEED + 500).select(
        F.concat(F.lit("POS-"), F.expr("uuid()")).alias("pos_transaction_id"),
        F.concat(F.lit("STORE-"), F.format_string("%04d", (F.rand(SEED + 501) * 500).cast("int"))).alias("store_id"),
        # Only 30% have loyalty ID (identified customers)
        F.when(F.rand(SEED + 502) < 0.3, F.col("loyalty_id")).otherwise(F.lit(None)).alias("loyalty_id"),
        F.concat(F.lit("PROD-"),
                F.format_string("%06d", (F.rand(SEED + 503) * config['products']).cast("int"))
        ).alias("product_id"),
        (F.rand(SEED + 504) * 5 + 1).cast("int").alias("quantity"),
        (F.rand(SEED + 505) * 150 + 5).cast("decimal(10,2)").alias("unit_price"),
        "payment_token",
        generate_timestamp(365, 506).alias("transaction_date"),
        random_element(PAYMENT_METHODS, 507).alias("payment_method")
    ).withColumn("transaction_total", F.col("quantity") * F.col("unit_price")).limit(config['pos'])
    
    pos_count = pos.count()
    print(f"  → POS: {pos_count:,}")
    
    # =========================================================================
    # 7. GENERATE SURVEY RESPONSES (from e-commerce customers)
    # =========================================================================
    print("Generating Survey Responses...")
    
    survey_sample_rate = config['surveys'] / config['ecom']
    surveys = ecom.sample(False, min(survey_sample_rate, 1.0), SEED + 600).select(
        F.concat(F.lit("SURV-"), F.expr("uuid()")).alias("response_id"),
        random_element(SURVEY_TYPES, 601).alias("survey_type"),
        "email",
        F.col("transaction_id").alias("order_id"),
        "product_id",
        (F.rand(SEED + 602) * 11).cast("int").alias("nps_score"),  # 0-10
        (F.rand(SEED + 603) * 5 + 1).cast("int").alias("satisfaction_rating"),  # 1-5
        F.when(F.rand(SEED + 604) < 0.3,
               F.lit("Great product, would recommend!")
        ).when(F.rand(SEED + 604) < 0.6,
               F.lit("Meets expectations")
        ).otherwise(F.lit(None)).alias("feedback_text"),
        generate_timestamp(90, 605).alias("submitted_at"),
        (F.rand(SEED + 606) * 300 + 30).cast("int").alias("completion_time_seconds")
    ).limit(config['surveys'])
    
    surveys_count = surveys.count()
    print(f"  → Surveys: {surveys_count:,}")
    
    # =========================================================================
    # 8. GENERATE PRODUCT REVIEWS
    # =========================================================================
    print("Generating Product Reviews...")
    
    review_sample_rate = config['reviews'] / config['ecom']
    reviews = ecom.sample(False, min(review_sample_rate, 1.0), SEED + 700).select(
        F.concat(F.lit("REV-"), F.expr("uuid()")).alias("review_id"),
        "product_id",
        F.col("user_id").alias("reviewer_user_id"),
        "email" if F.rand(SEED + 701) < 0.5 else F.lit(None),  # 50% have email
        F.col("email").alias("reviewer_email"),
        F.concat(F.lit("Reviewer"), (F.rand(SEED + 702) * 10000).cast("int")).alias("reviewer_display_name"),
        (F.rand(SEED + 703) * 5 + 1).cast("int").alias("rating"),  # 1-5
        F.concat(F.lit("Review Title "), (F.rand() * 1000).cast("int")).alias("review_title"),
        F.when(F.rand(SEED + 704) < 0.7,
               F.lit("This product is exactly what I needed. Quality is great.")
        ).otherwise(
               F.lit("Not what I expected. Would not purchase again.")
        ).alias("review_text"),
        (F.rand(SEED + 705) < 0.8).alias("verified_purchase"),
        (F.rand(SEED + 706) * 100).cast("int").alias("helpful_votes"),
        generate_timestamp(180, 707).alias("submitted_at")
    ).limit(config['reviews'])
    
    reviews_count = reviews.count()
    print(f"  → Reviews: {reviews_count:,}")
    
    # =========================================================================
    # 9. INJECT EDGE CASES
    # =========================================================================
    print("Injecting edge cases...")
    
    # Giant cluster - one email used by many records (should trigger max_group_size)
    giant_cluster_size = 6000
    giant_cluster = spark.range(0, giant_cluster_size).select(
        F.concat(F.lit("ECOM-CORP-"), F.col("id")).alias("user_id"),
        F.lit("orders@corporate-hub.com").alias("email"),
        F.lit(None).alias("phone"),
        F.concat(F.lit("PROD-"), F.format_string("%06d", (F.rand() * 100).cast("int"))).alias("product_id"),
        F.lit(1).alias("quantity"),
        F.lit(99.99).cast("decimal(10,2)").alias("unit_price"),
        F.lit("USD").alias("currency"),
        F.lit(None).alias("shipping_address_hash"),
        F.lit(None).alias("payment_token"),
        F.lit("desktop").alias("device_type"),
        generate_timestamp(30, 800).alias("order_date"),
        F.lit(None).alias("promo_code"),
        F.lit(99.99).cast("decimal(10,2)").alias("order_total"),
        F.expr("uuid()").alias("transaction_id")
    )
    
    # Problematic identifiers (should be excluded)
    bad_identifiers = spark.createDataFrame([
        ("ECOM-BAD-1", "noreply@company.com", "0000000000"),
        ("ECOM-BAD-2", "test@test.com", "1111111111"),
        ("ECOM-BAD-3", "do-not-reply@store.com", "9999999999"),
    ], ["user_id", "email", "phone"])
    
    print(f"  → Giant cluster: {giant_cluster_size:,} records")
    print(f"  → Bad identifiers: 3 test records")
    
    # =========================================================================
    # 10. WRITE OUTPUT
    # =========================================================================
    print("\nWriting output files...")
    
    sources = {
        "products": products,
        "crm_customers": crm,
        "ecom_transactions": ecom,
        "loyalty_members": loyalty,
        "mobile_app_users": mobile,
        "pos_transactions": pos,
        "survey_responses": surveys,
        "product_reviews": reviews
    }
    
    output_path = args.output.rstrip('/')
    
    for name, df in sources.items():
        path = f"{output_path}/{name}"
        print(f"  Writing {name}...")
        
        if args.format == 'delta':
            df.write.format("delta").mode("overwrite").save(path)
        else:
            df.write.mode("overwrite").parquet(f"{path}.parquet")
    
    # Write edge cases separately (for testing)
    giant_cluster.write.mode("overwrite").parquet(f"{output_path}/edge_cases/giant_cluster.parquet")
    
    # =========================================================================
    # 11. GENERATE METADATA CONFIGURATION
    # =========================================================================
    print("\nGenerating IDR metadata configuration...")
    
    metadata_sql = f"""
-- IDR Metadata Configuration for Scale Test Data
-- Generated: {datetime.now().isoformat()}
-- Scale: {args.scale}

-- Source Tables
INSERT INTO idr_meta.source_table (table_id, table_fqn, entity_type, entity_key_expr, watermark_column, watermark_lookback_minutes, is_active) VALUES
  ('crm', 'crm_customers', 'PERSON', 'customer_id', 'updated_at', 0, TRUE),
  ('ecom', 'ecom_transactions', 'PERSON', 'user_id', 'order_date', 0, TRUE),
  ('loyalty', 'loyalty_members', 'PERSON', 'member_id', 'last_activity_date', 0, TRUE),
  ('mobile', 'mobile_app_users', 'PERSON', 'app_user_id', 'last_active_date', 0, TRUE),
  ('pos', 'pos_transactions', 'PERSON', 'pos_transaction_id', 'transaction_date', 0, TRUE),
  ('surveys', 'survey_responses', 'PERSON', 'response_id', 'submitted_at', 0, TRUE),
  ('reviews', 'product_reviews', 'PERSON', 'review_id', 'submitted_at', 0, TRUE);

-- Identifier Mappings
INSERT INTO idr_meta.identifier_mapping (table_id, identifier_type, identifier_value_expr, is_hashed) VALUES
  -- CRM
  ('crm', 'EMAIL', 'LOWER(email)', FALSE),
  ('crm', 'PHONE', 'REGEXP_REPLACE(phone, ''[^0-9]'', '''')', FALSE),
  ('crm', 'LOYALTY_ID', 'loyalty_id', FALSE),
  -- E-commerce
  ('ecom', 'EMAIL', 'LOWER(email)', FALSE),
  ('ecom', 'PHONE', 'REGEXP_REPLACE(phone, ''[^0-9]'', '''')', FALSE),
  ('ecom', 'PAYMENT_TOKEN', 'payment_token', FALSE),
  -- Loyalty
  ('loyalty', 'EMAIL', 'LOWER(email)', FALSE),
  ('loyalty', 'PHONE', 'REGEXP_REPLACE(phone, ''[^0-9]'', '''')', FALSE),
  ('loyalty', 'LOYALTY_ID', 'loyalty_id', FALSE),
  -- Mobile
  ('mobile', 'EMAIL', 'LOWER(email)', FALSE),
  ('mobile', 'PHONE', 'REGEXP_REPLACE(phone, ''[^0-9]'', '''')', FALSE),
  ('mobile', 'DEVICE_ID', 'device_id', FALSE),
  -- POS
  ('pos', 'LOYALTY_ID', 'loyalty_id', FALSE),
  ('pos', 'PAYMENT_TOKEN', 'payment_token', FALSE),
  -- Surveys
  ('surveys', 'EMAIL', 'LOWER(email)', FALSE),
  -- Reviews
  ('reviews', 'EMAIL', 'LOWER(reviewer_email)', FALSE);

-- Known Bad Identifiers (for exclusion)
INSERT INTO idr_meta.identifier_exclusion (identifier_type, identifier_value_pattern, match_type, reason) VALUES
  ('EMAIL', 'noreply@%', 'LIKE', 'System email'),
  ('EMAIL', 'test@test.com', 'EXACT', 'Test data'),
  ('EMAIL', '%do-not-reply%', 'LIKE', 'System email'),
  ('EMAIL', 'orders@corporate-hub.com', 'EXACT', 'Shared corporate email'),
  ('PHONE', '0000000000', 'EXACT', 'Invalid phone'),
  ('PHONE', '1111111111', 'EXACT', 'Test phone'),
  ('PHONE', '9999999999', 'EXACT', 'Test phone');
"""
    
    # Write metadata SQL
    metadata_path = f"{output_path}/metadata"
    spark.sparkContext.parallelize([metadata_sql]).coalesce(1).saveAsTextFile(f"{metadata_path}/idr_config.sql")
    
    # =========================================================================
    # 12. SUMMARY REPORT
    # =========================================================================
    total_records = sum([crm_count, ecom_count, loyalty_count, mobile_count, 
                        pos_count, surveys_count, reviews_count])
    
    print(f"""
    ╔══════════════════════════════════════════════════════════╗
    ║                   GENERATION COMPLETE                    ║
    ╠══════════════════════════════════════════════════════════╣
    ║  Products:         {products_count:>12,}                        ║
    ║  CRM Customers:    {crm_count:>12,}                        ║
    ║  E-commerce:       {ecom_count:>12,}                        ║
    ║  Loyalty Members:  {loyalty_count:>12,}                        ║
    ║  Mobile App Users: {mobile_count:>12,}                        ║
    ║  POS Transactions: {pos_count:>12,}                        ║
    ║  Survey Responses: {surveys_count:>12,}                        ║
    ║  Product Reviews:  {reviews_count:>12,}                        ║
    ╠══════════════════════════════════════════════════════════╣
    ║  TOTAL RECORDS:    {total_records:>12,}                        ║
    ║  Unique Customers: {config['unique_customers']:>12,}                        ║
    ╠══════════════════════════════════════════════════════════╣
    ║  Output Path: {output_path:<40} ║
    ╚══════════════════════════════════════════════════════════╝
    
    Next Steps:
    1. Load parquet files into your warehouse
    2. Run: {metadata_path}/idr_config.sql
    3. Execute IDR: CALL idr_run('FULL', 30, TRUE);  -- dry run first!
    """)
    
    spark.stop()

if __name__ == "__main__":
    main()
