#!/usr/bin/env python3
"""
Platform Data Loaders

Utilities for loading Parquet test data into each platform.

Usage:
    python tools/scale_test/loaders.py --platform=duckdb --data=data/retail_customers_20m.parquet
"""

import argparse
import os
import time
from abc import ABC, abstractmethod


class DataLoader(ABC):
    """Abstract base for platform data loaders."""
    
    @abstractmethod
    def load(self, parquet_path: str, table_name: str = "retail_customers") -> float:
        """Load Parquet data. Returns duration in seconds."""
        pass


# ============================================
# DUCKDB LOADER
# ============================================

class DuckDBLoader(DataLoader):
    """Load Parquet into DuckDB."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def load(self, parquet_path: str, table_name: str = "retail_customers") -> float:
        import duckdb
        
        start = time.time()
        conn = duckdb.connect(self.db_path)
        
        conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS 
            SELECT * FROM read_parquet('{parquet_path}')
        """)
        
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        duration = time.time() - start
        
        print(f"  Loaded {count:,} rows into {table_name}")
        print(f"  Duration: {duration:.1f}s")
        
        conn.close()
        return duration


# ============================================
# SNOWFLAKE LOADER
# ============================================

class SnowflakeLoader(DataLoader):
    """Load Parquet into Snowflake via stage."""
    
    def __init__(self, account: str, user: str, password: str, 
                 database: str, warehouse: str, schema: str = "IDR_TEST"):
        self.account = account
        self.user = user
        self.password = password
        self.database = database
        self.warehouse = warehouse
        self.schema = schema
    
    def load(self, parquet_path: str, table_name: str = "retail_customers") -> float:
        import snowflake.connector
        
        start = time.time()
        
        conn = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            warehouse=self.warehouse
        )
        cursor = conn.cursor()
        
        # Create schema if not exists
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        cursor.execute(f"USE SCHEMA {self.schema}")
        
        # Create stage
        stage_name = f"idr_test_stage_{int(time.time())}"
        cursor.execute(f"CREATE OR REPLACE TEMPORARY STAGE {stage_name}")
        
        # Upload file to stage
        print(f"  Uploading {parquet_path} to stage...")
        cursor.execute(f"PUT file://{os.path.abspath(parquet_path)} @{stage_name}")
        
        # Create table and load
        cursor.execute(f"""
            CREATE OR REPLACE TABLE {table_name} (
                entity_id VARCHAR,
                first_name VARCHAR,
                last_name VARCHAR,
                email VARCHAR,
                phone VARCHAR,
                loyalty_id VARCHAR,
                address_street VARCHAR,
                address_city VARCHAR,
                address_state VARCHAR,
                address_zip VARCHAR,
                loyalty_tier VARCHAR,
                created_at TIMESTAMP,
                source_system VARCHAR
            )
        """)
        
        print(f"  Loading into {table_name}...")
        parquet_filename = os.path.basename(parquet_path)
        cursor.execute(f"""
            COPY INTO {table_name}
            FROM @{stage_name}/{parquet_filename}
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """)
        
        # Verify
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        
        duration = time.time() - start
        print(f"  Loaded {count:,} rows into {table_name}")
        print(f"  Duration: {duration:.1f}s")
        
        cursor.close()
        conn.close()
        return duration


# ============================================
# BIGQUERY LOADER
# ============================================

class BigQueryLoader(DataLoader):
    """Load Parquet into BigQuery via GCS."""
    
    def __init__(self, project: str, dataset: str = "idr_test", gcs_bucket: str = None):
        self.project = project
        self.dataset = dataset
        self.gcs_bucket = gcs_bucket or f"{project}-idr-benchmark"
    
    def load(self, parquet_path: str, table_name: str = "retail_customers") -> float:
        from google.cloud import bigquery, storage
        
        start = time.time()
        
        # Upload to GCS
        storage_client = storage.Client(project=self.project)
        
        # Create bucket if not exists
        try:
            bucket = storage_client.get_bucket(self.gcs_bucket)
        except:
            bucket = storage_client.create_bucket(self.gcs_bucket, location="US")
        
        # Upload file
        blob_name = f"benchmark/{os.path.basename(parquet_path)}"
        blob = bucket.blob(blob_name)
        
        print(f"  Uploading to gs://{self.gcs_bucket}/{blob_name}...")
        blob.upload_from_filename(parquet_path)
        
        # Load into BigQuery
        bq_client = bigquery.Client(project=self.project)
        
        # Create dataset if not exists
        dataset_ref = bq_client.dataset(self.dataset)
        try:
            bq_client.get_dataset(dataset_ref)
        except:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            bq_client.create_dataset(dataset)
        
        # Load data
        table_ref = dataset_ref.table(table_name)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        
        uri = f"gs://{self.gcs_bucket}/{blob_name}"
        print(f"  Loading from {uri}...")
        
        load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
        load_job.result()  # Wait for completion
        
        # Verify
        table = bq_client.get_table(table_ref)
        count = table.num_rows
        
        duration = time.time() - start
        print(f"  Loaded {count:,} rows into {self.project}.{self.dataset}.{table_name}")
        print(f"  Duration: {duration:.1f}s")
        
        return duration


# ============================================
# DATABRICKS LOADER
# ============================================

class DatabricksLoader(DataLoader):
    """Load Parquet into Databricks via Volumes or DBFS."""
    
    def __init__(self, catalog: str = "main", schema: str = "idr_test"):
        self.catalog = catalog
        self.schema = schema
    
    def load(self, parquet_path: str, table_name: str = "retail_customers") -> float:
        """
        Note: This is designed to run FROM a Databricks notebook.
        For external loading, use the Databricks SDK or REST API.
        """
        print("  Databricks loading should be done from a notebook:")
        print(f"""
        # In Databricks notebook:
        df = spark.read.parquet("/Volumes/{self.catalog}/{self.schema}/data/{os.path.basename(parquet_path)}")
        df.write.mode("overwrite").saveAsTable("{self.catalog}.{self.schema}.{table_name}")
        """)
        return 0.0


# ============================================
# CLI
# ============================================

def main():
    parser = argparse.ArgumentParser(description="Load test data into platform")
    parser.add_argument("--platform", required=True, choices=["duckdb", "snowflake", "bigquery", "databricks"])
    parser.add_argument("--data", required=True, help="Path to Parquet file")
    parser.add_argument("--table", default="retail_customers", help="Target table name")
    
    # DuckDB
    parser.add_argument("--db", help="DuckDB: database path")
    
    # Snowflake (or use env vars)
    parser.add_argument("--account", help="Snowflake account")
    parser.add_argument("--user", help="Snowflake user")
    parser.add_argument("--password", help="Snowflake password")
    parser.add_argument("--database", help="Snowflake database")
    parser.add_argument("--warehouse", help="Snowflake warehouse")
    
    # BigQuery
    parser.add_argument("--project", help="GCP project")
    parser.add_argument("--dataset", default="idr_test", help="BigQuery dataset")
    parser.add_argument("--bucket", help="GCS bucket for staging")
    
    args = parser.parse_args()
    
    print(f"\n{'='*60}")
    print(f"Loading data: {args.data}")
    print(f"Platform: {args.platform}")
    print(f"{'='*60}\n")
    
    if args.platform == "duckdb":
        loader = DuckDBLoader(db_path=args.db or "idr_benchmark.duckdb")
    elif args.platform == "snowflake":
        loader = SnowflakeLoader(
            account=args.account or os.environ.get("SNOWFLAKE_ACCOUNT"),
            user=args.user or os.environ.get("SNOWFLAKE_USER"),
            password=args.password or os.environ.get("SNOWFLAKE_PASSWORD"),
            database=args.database or os.environ.get("SNOWFLAKE_DATABASE"),
            warehouse=args.warehouse or os.environ.get("SNOWFLAKE_WAREHOUSE"),
        )
    elif args.platform == "bigquery":
        loader = BigQueryLoader(
            project=args.project or os.environ.get("GCP_PROJECT"),
            dataset=args.dataset,
            gcs_bucket=args.bucket,
        )
    elif args.platform == "databricks":
        loader = DatabricksLoader()
    
    duration = loader.load(args.data, args.table)
    
    print(f"\n{'='*60}")
    print(f"Load complete: {duration:.1f}s")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
