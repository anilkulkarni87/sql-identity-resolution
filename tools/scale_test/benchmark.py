#!/usr/bin/env python3
"""
IDR Scale Test Benchmark Runner

Runs identity resolution benchmarks across platforms and collects metrics.

Usage:
    python tools/scale_test/benchmark.py \
        --platform=duckdb \
        --data=data/retail_customers_20m.parquet \
        --output=benchmarks/
"""

import argparse
import json
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, Any, Optional
import subprocess


@dataclass
class BenchmarkResult:
    """Benchmark result data."""
    platform: str
    row_count: int
    seed: int
    run_mode: str
    
    # Timing
    data_load_seconds: float
    entity_extraction_seconds: float
    edge_building_seconds: float
    label_propagation_seconds: float
    output_generation_seconds: float
    total_duration_seconds: float
    
    # Metrics
    entities_processed: int
    edges_created: int
    clusters_created: int
    largest_cluster: int
    singleton_count: int
    lp_iterations: int
    
    # System
    timestamp: str
    run_id: str
    notes: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class BenchmarkAdapter(ABC):
    """Abstract base for platform benchmark adapters."""
    
    @abstractmethod
    def load_data(self, parquet_path: str) -> float:
        """Load data and return duration in seconds."""
        pass
    
    @abstractmethod
    def configure_metadata(self) -> None:
        """Set up source table, rules, and mappings."""
        pass
    
    @abstractmethod
    def run_idr(self, run_mode: str = "FULL", dry_run: bool = False) -> Dict[str, Any]:
        """Run IDR and return metrics."""
        pass
    
    @abstractmethod
    def get_cluster_stats(self) -> Dict[str, Any]:
        """Get cluster statistics."""
        pass
    
    @abstractmethod
    def cleanup(self) -> None:
        """Clean up test data."""
        pass


class DuckDBBenchmarkAdapter(BenchmarkAdapter):
    """DuckDB benchmark adapter."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
    
    def _connect(self):
        if self.conn is None:
            import duckdb
            self.conn = duckdb.connect(self.db_path)
        return self.conn
    
    def load_data(self, parquet_path: str) -> float:
        """Load Parquet data into DuckDB."""
        start = time.time()
        conn = self._connect()
        
        # Create test table from Parquet
        conn.execute(f"""
            CREATE OR REPLACE TABLE retail_customers AS 
            SELECT * FROM read_parquet('{parquet_path}')
        """)
        
        # Verify
        count = conn.execute("SELECT COUNT(*) FROM retail_customers").fetchone()[0]
        print(f"  Loaded {count:,} rows into retail_customers")
        
        return time.time() - start
    
    def configure_metadata(self) -> None:
        """Set up IDR metadata for test data."""
        conn = self._connect()
        
        # Clear existing metadata
        conn.execute("DELETE FROM idr_meta.source_table WHERE table_id = 'retail_test'")
        conn.execute("DELETE FROM idr_meta.identifier_mapping WHERE table_id = 'retail_test'")
        conn.execute("DELETE FROM idr_meta.run_state WHERE table_id = 'retail_test'")
        
        # Register source
        conn.execute("""
            INSERT INTO idr_meta.source_table VALUES
            ('retail_test', 'retail_customers', 'PERSON', 'entity_id', 'created_at', 0, TRUE)
        """)
        
        # Ensure rules exist
        rules = [
            ("email_rule", "EMAIL", 1, True, 50000),
            ("phone_rule", "PHONE", 2, True, 10000),
            ("loyalty_rule", "LOYALTY", 3, True, 100),
            ("address_rule", "ADDRESS", 4, True, 5000),
        ]
        for rule_id, id_type, priority, active, max_size in rules:
            try:
                conn.execute(f"""
                    INSERT INTO idr_meta.rule VALUES
                    ('{rule_id}', '{id_type}', {priority}, {active}, {max_size})
                """)
            except:
                pass  # Rule already exists
        
        # Add mappings
        mappings = [
            ("retail_test", "EMAIL", "email"),
            ("retail_test", "PHONE", "phone"),
            ("retail_test", "LOYALTY", "loyalty_id"),
            ("retail_test", "ADDRESS", "address_street || '' '' || address_city || '' '' || address_state || '' '' || address_zip"),
        ]
        for table_id, id_type, col_expr in mappings:
            try:
                conn.execute(f"""
                    INSERT INTO idr_meta.identifier_mapping VALUES
                    ('{table_id}', '{id_type}', '{col_expr}', TRUE)
                """)
            except:
                pass
    
    def run_idr(self, run_mode: str = "FULL", dry_run: bool = False) -> Dict[str, Any]:
        """Run IDR pipeline."""
        cmd = [
            "python", "sql/duckdb/idr_run.py",
            f"--db={self.db_path}",
            f"--run-mode={run_mode}",
        ]
        if dry_run:
            cmd.append("--dry-run")
        
        start = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.getcwd())
        duration = time.time() - start
        
        if result.returncode != 0:
            print(f"  Error: {result.stderr}")
        
        # Parse output for metrics
        metrics = {"duration": duration}
        for line in result.stdout.split("\n"):
            if "Entities:" in line:
                metrics["entities_processed"] = int(line.split(":")[1].strip().replace(",", ""))
            elif "Edges:" in line:
                metrics["edges_created"] = int(line.split(":")[1].strip().replace(",", ""))
            elif "Iterations:" in line:
                metrics["lp_iterations"] = int(line.split(":")[1].strip())
        
        return metrics
    
    def get_cluster_stats(self) -> Dict[str, Any]:
        """Get cluster statistics."""
        conn = self._connect()
        
        stats = {}
        
        # Total clusters
        result = conn.execute("SELECT COUNT(*) FROM idr_out.identity_clusters_current").fetchone()
        stats["clusters_created"] = result[0] if result else 0
        
        # Largest cluster
        result = conn.execute("SELECT MAX(cluster_size) FROM idr_out.identity_clusters_current").fetchone()
        stats["largest_cluster"] = result[0] if result and result[0] else 0
        
        # Singleton count
        result = conn.execute("SELECT COUNT(*) FROM idr_out.identity_clusters_current WHERE cluster_size = 1").fetchone()
        stats["singleton_count"] = result[0] if result else 0
        
        return stats
    
    def cleanup(self) -> None:
        """Clean up test data."""
        if self.conn:
            self.conn.execute("DROP TABLE IF EXISTS retail_customers")
            self.conn.execute("DELETE FROM idr_meta.source_table WHERE table_id = 'retail_test'")
            self.conn.execute("DELETE FROM idr_meta.identifier_mapping WHERE table_id = 'retail_test'")


class SnowflakeBenchmarkAdapter(BenchmarkAdapter):
    """Snowflake benchmark adapter (stub - requires configuration)."""
    
    def __init__(self, account: str, user: str, password: str, database: str, warehouse: str):
        self.account = account
        self.user = user
        self.password = password
        self.database = database
        self.warehouse = warehouse
        self.conn = None
    
    def _connect(self):
        if self.conn is None:
            import snowflake.connector
            self.conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                database=self.database,
                warehouse=self.warehouse
            )
        return self.conn
    
    def load_data(self, parquet_path: str) -> float:
        """Load data via stage."""
        # This would require:
        # 1. PUT file to stage
        # 2. COPY INTO table
        raise NotImplementedError("Implement based on your Snowflake setup")
    
    def configure_metadata(self) -> None:
        raise NotImplementedError("Implement based on your Snowflake setup")
    
    def run_idr(self, run_mode: str = "FULL", dry_run: bool = False) -> Dict[str, Any]:
        raise NotImplementedError("Implement based on your Snowflake setup")
    
    def get_cluster_stats(self) -> Dict[str, Any]:
        raise NotImplementedError("Implement based on your Snowflake setup")
    
    def cleanup(self) -> None:
        pass


class BigQueryBenchmarkAdapter(BenchmarkAdapter):
    """BigQuery benchmark adapter (stub - requires configuration)."""
    
    def __init__(self, project: str, dataset: str = "idr_benchmark"):
        self.project = project
        self.dataset = dataset
        self.client = None
    
    def _connect(self):
        if self.client is None:
            from google.cloud import bigquery
            self.client = bigquery.Client(project=self.project)
        return self.client
    
    def load_data(self, parquet_path: str) -> float:
        """Load data via GCS."""
        # This would require:
        # 1. Upload to GCS
        # 2. bq load from GCS
        raise NotImplementedError("Implement based on your GCP setup")
    
    def configure_metadata(self) -> None:
        raise NotImplementedError("Implement based on your GCP setup")
    
    def run_idr(self, run_mode: str = "FULL", dry_run: bool = False) -> Dict[str, Any]:
        raise NotImplementedError("Implement based on your GCP setup")
    
    def get_cluster_stats(self) -> Dict[str, Any]:
        raise NotImplementedError("Implement based on your GCP setup")
    
    def cleanup(self) -> None:
        pass


# ============================================
# BENCHMARK RUNNER
# ============================================

def run_benchmark(
    adapter: BenchmarkAdapter,
    parquet_path: str,
    platform: str,
    row_count: int,
    seed: int,
    run_mode: str = "FULL",
    dry_run: bool = False,
    notes: str = ""
) -> BenchmarkResult:
    """Run a complete benchmark."""
    
    run_id = f"bench_{platform}_{row_count//1000000}m_{int(time.time())}"
    
    print(f"\n{'='*60}")
    print(f"Benchmark: {platform} - {row_count:,} rows")
    print(f"{'='*60}")
    
    # Phase 1: Load data
    print("\n[1/5] Loading data...")
    data_load_seconds = adapter.load_data(parquet_path)
    print(f"  Duration: {data_load_seconds:.1f}s")
    
    # Phase 2: Configure metadata
    print("\n[2/5] Configuring metadata...")
    adapter.configure_metadata()
    
    # Phase 3: Run IDR
    print(f"\n[3/5] Running IDR ({run_mode}, dry_run={dry_run})...")
    start = time.time()
    idr_metrics = adapter.run_idr(run_mode, dry_run)
    total_duration = time.time() - start
    print(f"  Duration: {total_duration:.1f}s")
    
    # Phase 4: Get cluster stats
    print("\n[4/5] Collecting cluster stats...")
    cluster_stats = adapter.get_cluster_stats()
    
    # Phase 5: Build result
    print("\n[5/5] Building result...")
    
    result = BenchmarkResult(
        platform=platform,
        row_count=row_count,
        seed=seed,
        run_mode=run_mode,
        data_load_seconds=data_load_seconds,
        entity_extraction_seconds=0,  # TODO: parse from logs
        edge_building_seconds=0,
        label_propagation_seconds=0,
        output_generation_seconds=0,
        total_duration_seconds=total_duration,
        entities_processed=idr_metrics.get("entities_processed", row_count),
        edges_created=idr_metrics.get("edges_created", 0),
        clusters_created=cluster_stats.get("clusters_created", 0),
        largest_cluster=cluster_stats.get("largest_cluster", 0),
        singleton_count=cluster_stats.get("singleton_count", 0),
        lp_iterations=idr_metrics.get("lp_iterations", 0),
        timestamp=datetime.now().isoformat(),
        run_id=run_id,
        notes=notes
    )
    
    print(f"\n{'='*60}")
    print("Benchmark Complete!")
    print(f"{'='*60}")
    
    return result


def save_result(result: BenchmarkResult, output_dir: str):
    """Save benchmark result to JSON."""
    os.makedirs(output_dir, exist_ok=True)
    
    filename = f"{result.run_id}.json"
    path = os.path.join(output_dir, filename)
    
    with open(path, "w") as f:
        json.dump(result.to_dict(), f, indent=2)
    
    print(f"  Result saved: {path}")
    return path


# ============================================
# CLI
# ============================================

def main():
    parser = argparse.ArgumentParser(description="Run IDR benchmark")
    parser.add_argument("--platform", required=True, choices=["duckdb", "snowflake", "bigquery", "databricks"])
    parser.add_argument("--data", required=True, help="Path to Parquet data file")
    parser.add_argument("--output", default="benchmarks/", help="Output directory for results")
    parser.add_argument("--rows", type=int, required=True, help="Number of rows in dataset")
    parser.add_argument("--seed", type=int, default=42, help="Dataset seed")
    parser.add_argument("--run-mode", default="FULL", choices=["FULL", "INCR"])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--notes", default="", help="Notes for this run")
    
    # Platform-specific
    parser.add_argument("--db", help="DuckDB: database path")
    parser.add_argument("--project", help="BigQuery: GCP project")
    
    args = parser.parse_args()
    
    # Create adapter
    if args.platform == "duckdb":
        db_path = args.db or "idr_benchmark.duckdb"
        adapter = DuckDBBenchmarkAdapter(db_path)
    elif args.platform == "snowflake":
        adapter = SnowflakeBenchmarkAdapter(
            account=os.environ.get("SNOWFLAKE_ACCOUNT"),
            user=os.environ.get("SNOWFLAKE_USER"),
            password=os.environ.get("SNOWFLAKE_PASSWORD"),
            database=os.environ.get("SNOWFLAKE_DATABASE"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE")
        )
    elif args.platform == "bigquery":
        adapter = BigQueryBenchmarkAdapter(project=args.project)
    else:
        print(f"Platform {args.platform} not yet implemented")
        return
    
    # Run benchmark
    result = run_benchmark(
        adapter=adapter,
        parquet_path=args.data,
        platform=args.platform,
        row_count=args.rows,
        seed=args.seed,
        run_mode=args.run_mode,
        dry_run=args.dry_run,
        notes=args.notes
    )
    
    # Save result
    save_result(result, args.output)
    
    # Print summary
    print(f"\n{'='*60}")
    print("Summary")
    print(f"{'='*60}")
    print(f"  Platform:        {result.platform}")
    print(f"  Rows:            {result.row_count:,}")
    print(f"  Total Duration:  {result.total_duration_seconds:.1f}s")
    print(f"  Clusters:        {result.clusters_created:,}")
    print(f"  Largest Cluster: {result.largest_cluster:,}")
    print(f"  Singletons:      {result.singleton_count:,}")
    print(f"  LP Iterations:   {result.lp_iterations}")


if __name__ == "__main__":
    main()
