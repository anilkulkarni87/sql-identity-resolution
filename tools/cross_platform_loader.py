#!/usr/bin/env python3
"""
Cross-Platform Data Loader for IDR Scale Testing

Exports source tables from DuckDB and loads them to Snowflake, BigQuery, or Databricks.

Usage:
    # Export from DuckDB to Parquet
    python cross_platform_loader.py export --db=scale_test.duckdb --output=./export
    
    # Load to Snowflake
    python cross_platform_loader.py load-snowflake --input=./export --account=xxx --database=IDR_DEV
    
    # Load to BigQuery  
    python cross_platform_loader.py load-bigquery --input=./export --project=my-project --dataset=crm
    
    # Load to Databricks
    python cross_platform_loader.py load-databricks --input=./export --catalog=main --schema=crm

Prerequisites:
    pip install duckdb pyarrow
    
    For Snowflake: pip install snowflake-connector-python
    For BigQuery:  pip install google-cloud-bigquery
    For Databricks: pip install databricks-sql-connector
"""

import argparse
import os
import sys
from pathlib import Path

# ============================================
# EXPORT FROM DUCKDB
# ============================================
def export_from_duckdb(db_path: str, output_dir: str):
    """Export all source tables from DuckDB to Parquet files."""
    try:
        import duckdb
    except ImportError:
        print("Please install duckdb: pip install duckdb")
        sys.exit(1)
    
    os.makedirs(output_dir, exist_ok=True)
    con = duckdb.connect(db_path, read_only=True)
    
    # Get source tables from metadata
    tables = con.execute("""
        SELECT table_id, table_fqn 
        FROM idr_meta.source_table 
        WHERE is_active = TRUE
    """).fetchall()
    
    if not tables:
        print("❌ No active source tables found in idr_meta.source_table")
        sys.exit(1)
    
    print(f"📦 Exporting {len(tables)} tables from {db_path}...")
    
    exported = []
    for table_id, table_fqn in tables:
        output_path = os.path.join(output_dir, f"{table_id}.parquet")
        
        # Get row count
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_fqn}").fetchone()[0]
        
        # Export to Parquet
        con.execute(f"""
            COPY (SELECT * FROM {table_fqn}) 
            TO '{output_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
        
        file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
        print(f"  ✅ {table_id}: {row_count:,} rows → {output_path} ({file_size:.1f} MB)")
        exported.append({
            'table_id': table_id,
            'table_fqn': table_fqn,
            'file': output_path,
            'rows': row_count
        })
    
    con.close()
    
    print(f"\n✅ Export complete! Files in: {output_dir}")
    print(f"   Total tables: {len(exported)}")
    print(f"   Total rows: {sum(e['rows'] for e in exported):,}")
    print(f"\n📋 Next steps:")
    print(f"   1. Run DDL script on target platform to create idr_meta/idr_work/idr_out tables")
    print(f"   2. Load source tables using load-snowflake/load-bigquery/load-databricks")
    print(f"   3. Configure metadata (source_table, identifier_mapping, rule) on target")
    print(f"   4. Run the platform's idr_run script")
    
    return exported


# ============================================
# LOAD TO SNOWFLAKE
# ============================================
def load_to_snowflake(input_dir: str, account: str, user: str, password: str, 
                       database: str, schema: str = 'CRM', warehouse: str = 'COMPUTE_WH'):
    """Load Parquet files to Snowflake."""
    try:
        import snowflake.connector
        from snowflake.connector.pandas_tools import write_pandas
        import pandas as pd
    except ImportError:
        print("Please install: pip install snowflake-connector-python pandas pyarrow")
        sys.exit(1)
    
    print(f"❄️ Connecting to Snowflake: {account}/{database}...")
    
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        database=database,
        schema=schema,
        warehouse=warehouse
    )
    
    cursor = conn.cursor()
    
    # Create schema if not exists
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    cursor.execute(f"USE SCHEMA {schema}")
    
    # Load each parquet file
    parquet_files = list(Path(input_dir).glob("*.parquet"))
    source_files = [f for f in parquet_files if not f.name.startswith("meta_")]
    
    print(f"📤 Loading {len(source_files)} tables...")
    
    for pq_file in source_files:
        table_name = pq_file.stem.upper()
        df = pd.read_parquet(pq_file)
        
        print(f"  Loading {table_name}: {len(df):,} rows...")
        
        # Drop and recreate table
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Write using pandas connector
        success, nchunks, nrows, _ = write_pandas(
            conn, df, table_name, 
            auto_create_table=True,
            quote_identifiers=False
        )
        
        print(f"  ✅ {table_name}: {nrows:,} rows loaded")
    
    cursor.close()
    conn.close()
    
    print(f"\n✅ Snowflake load complete!")


# ============================================
# LOAD TO BIGQUERY
# ============================================
def load_to_bigquery(input_dir: str, project: str, dataset: str = 'crm'):
    """Load Parquet files to BigQuery."""
    try:
        from google.cloud import bigquery
    except ImportError:
        print("Please install: pip install google-cloud-bigquery")
        sys.exit(1)
    
    print(f"🌐 Connecting to BigQuery: {project}.{dataset}...")
    
    client = bigquery.Client(project=project)
    
    # Create dataset if not exists
    dataset_ref = bigquery.Dataset(f"{project}.{dataset}")
    dataset_ref.location = "US"
    try:
        client.create_dataset(dataset_ref, exists_ok=True)
    except Exception as e:
        print(f"  Dataset exists or error: {e}")
    
    # Load each parquet file
    parquet_files = list(Path(input_dir).glob("*.parquet"))
    source_files = [f for f in parquet_files if not f.name.startswith("meta_")]
    
    print(f"📤 Loading {len(source_files)} tables...")
    
    for pq_file in source_files:
        table_name = pq_file.stem
        table_id = f"{project}.{dataset}.{table_name}"
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        
        print(f"  Loading {table_name}...")
        
        with open(pq_file, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        
        job.result()  # Wait for completion
        
        table = client.get_table(table_id)
        print(f"  ✅ {table_name}: {table.num_rows:,} rows loaded")
    
    print(f"\n✅ BigQuery load complete!")


# ============================================
# LOAD TO DATABRICKS
# ============================================
def load_to_databricks(input_dir: str, server_hostname: str, http_path: str, 
                        access_token: str, catalog: str = 'main', schema: str = 'crm'):
    """Load Parquet files to Databricks Unity Catalog."""
    try:
        from databricks import sql
        import pandas as pd
    except ImportError:
        print("Please install: pip install databricks-sql-connector pandas pyarrow")
        sys.exit(1)
    
    print(f"🔶 Connecting to Databricks: {catalog}.{schema}...")
    
    conn = sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token
    )
    
    cursor = conn.cursor()
    
    # Create schema if not exists
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    cursor.execute(f"USE CATALOG {catalog}")
    cursor.execute(f"USE SCHEMA {schema}")
    
    # Load each parquet file
    parquet_files = list(Path(input_dir).glob("*.parquet"))
    source_files = [f for f in parquet_files if not f.name.startswith("meta_")]
    
    print(f"📤 Loading {len(source_files)} tables...")
    print("  ⚠️ For large files, upload to DBFS/S3 first and use COPY INTO")
    
    for pq_file in source_files:
        table_name = pq_file.stem
        df = pd.read_parquet(pq_file)
        
        print(f"  Loading {table_name}: {len(df):,} rows...")
        
        # For small tables, we can use batch inserts
        # For large tables, you should upload to cloud storage first
        if len(df) > 100000:
            print(f"    ⚠️ Large table - consider using DBFS upload + COPY INTO for better performance")
        
        # Create table from first batch to infer schema
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Convert DataFrame to INSERT statements (chunked for large tables)
        chunk_size = 10000
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            
            if i == 0:
                # First chunk - create table
                cols = ", ".join([f"`{c}` STRING" for c in df.columns])
                cursor.execute(f"CREATE TABLE {table_name} ({cols})")
            
            # Insert data
            for _, row in chunk.iterrows():
                values = ", ".join([f"'{str(v)}'" if pd.notna(v) else "NULL" for v in row])
                cursor.execute(f"INSERT INTO {table_name} VALUES ({values})")
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"  ✅ {table_name}: {count:,} rows loaded")
    
    cursor.close()
    conn.close()
    
    print(f"\n✅ Databricks load complete!")


# ============================================
# MAIN CLI
# ============================================
def main():
    parser = argparse.ArgumentParser(
        description='Cross-Platform Data Loader for IDR Scale Testing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export from DuckDB to Parquet')
    export_parser.add_argument('--db', required=True, help='DuckDB database file')
    export_parser.add_argument('--output', default='./export', help='Output directory for Parquet files')
    
    # Snowflake command
    sf_parser = subparsers.add_parser('load-snowflake', help='Load Parquet to Snowflake')
    sf_parser.add_argument('--input', required=True, help='Directory with Parquet files')
    sf_parser.add_argument('--account', required=True, help='Snowflake account')
    sf_parser.add_argument('--user', default=os.environ.get('SNOWFLAKE_USER'), help='Snowflake user')
    sf_parser.add_argument('--password', default=os.environ.get('SNOWFLAKE_PASSWORD'), help='Snowflake password')
    sf_parser.add_argument('--database', required=True, help='Target database')
    sf_parser.add_argument('--schema', default='CRM', help='Target schema')
    sf_parser.add_argument('--warehouse', default='COMPUTE_WH', help='Warehouse')
    
    # BigQuery command
    bq_parser = subparsers.add_parser('load-bigquery', help='Load Parquet to BigQuery')
    bq_parser.add_argument('--input', required=True, help='Directory with Parquet files')
    bq_parser.add_argument('--project', required=True, help='GCP project ID')
    bq_parser.add_argument('--dataset', default='crm', help='Target dataset')
    
    # Databricks command
    db_parser = subparsers.add_parser('load-databricks', help='Load Parquet to Databricks')
    db_parser.add_argument('--input', required=True, help='Directory with Parquet files')
    db_parser.add_argument('--server-hostname', required=True, help='Databricks server hostname')
    db_parser.add_argument('--http-path', required=True, help='SQL warehouse HTTP path')
    db_parser.add_argument('--access-token', default=os.environ.get('DATABRICKS_TOKEN'), help='Access token')
    db_parser.add_argument('--catalog', default='main', help='Unity Catalog name')
    db_parser.add_argument('--schema', default='crm', help='Target schema')
    
    args = parser.parse_args()
    
    if args.command == 'export':
        export_from_duckdb(args.db, args.output)
    elif args.command == 'load-snowflake':
        load_to_snowflake(
            args.input, args.account, args.user, args.password,
            args.database, args.schema, args.warehouse
        )
    elif args.command == 'load-bigquery':
        load_to_bigquery(args.input, args.project, args.dataset)
    elif args.command == 'load-databricks':
        load_to_databricks(
            args.input, args.server_hostname, args.http_path,
            args.access_token, args.catalog, args.schema
        )
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
